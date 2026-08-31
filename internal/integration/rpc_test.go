package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/controller"
	"github.com/MuhammadMaazA/Orbit/internal/model"
	"github.com/MuhammadMaazA/Orbit/internal/rpc"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestControllerRPCSubmitAndStatus(t *testing.T) {
	state, err := controller.New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	resources := model.ResourceRequest{CPU: 4, MemoryMB: 4_096}
	if _, err := state.RegisterWorker("worker-a", "session-a", model.Capacity{Total: resources, Available: resources}); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	v1.RegisterOrbitControllerServer(server, rpc.NewServer(state))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := v1.NewOrbitControllerClient(connection)
	assignment, err := client.Submit(context.Background(), &v1.Job{Id: "job-1", Resources: &v1.ResourceRequest{Cpu: 1, MemoryMb: 512}})
	if err != nil || assignment.Id == "" {
		t.Fatalf("Submit() = %+v, %v", assignment, err)
	}
	status, err := client.GetJob(context.Background(), &v1.JobStatusRequest{JobId: "job-1"})
	if err != nil || status.Status != "running" || status.AssignmentId != assignment.Id {
		t.Fatalf("GetJob() = %+v, %v", status, err)
	}
}

func TestWorkerLossDispatchesRetryToAnotherSession(t *testing.T) {
	state, err := controller.New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	v1.RegisterOrbitControllerServer(server, rpc.NewServer(state))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	connect := func() *grpc.ClientConn {
		connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		return connection
	}
	connectionA, connectionB := connect(), connect()
	clientA, clientB := v1.NewOrbitControllerClient(connectionA), v1.NewOrbitControllerClient(connectionB)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	streamA, err := clientA.WorkerSession(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	streamB, err := clientB.WorkerSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	register := func(stream v1.OrbitController_WorkerSessionClient, id string) error {
		session := id + "-session"
		if err := stream.Send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Register{Register: &v1.Worker{Id: id, SessionId: session, Total: &v1.ResourceRequest{Cpu: 2, MemoryMb: 1024}}}}); err != nil {
			return err
		}
		if err := stream.Send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_HeartbeatSessionId{HeartbeatSessionId: session}}); err != nil {
			return err
		}
		ack, err := stream.Recv()
		if err != nil {
			return err
		}
		if ack.GetHeartbeatAckSessionId() != session {
			return fmt.Errorf("registration acknowledgement = %q, want %q", ack.GetHeartbeatAckSessionId(), session)
		}
		return nil
	}
	if err := register(streamB, "worker-b"); err != nil {
		t.Fatal(err)
	}
	if err := register(streamA, "worker-a"); err != nil {
		t.Fatal(err)
	}
	assignment, err := clientA.Submit(context.Background(), &v1.Job{Id: "job-1", Resources: &v1.ResourceRequest{Cpu: 2, MemoryMb: 512}})
	if err != nil || assignment.WorkerId != "worker-a" {
		t.Fatalf("Submit() = %+v, %v", assignment, err)
	}
	if _, err := streamA.Recv(); err != nil {
		t.Fatal(err)
	}
	cancelA()
	ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelB()
	message, err := recvWithContext(ctxB, streamB)
	if err != nil {
		t.Fatal(err)
	}
	if message.GetAssignment().WorkerId != "worker-b" || message.GetAssignment().Attempt != 2 {
		t.Fatalf("retry = %+v", message.GetAssignment())
	}
}

func recvWithContext(ctx context.Context, stream v1.OrbitController_WorkerSessionClient) (*v1.ControllerSessionMessage, error) {
	result := make(chan struct {
		message *v1.ControllerSessionMessage
		err     error
	}, 1)
	go func() {
		message, err := stream.Recv()
		result <- struct {
			message *v1.ControllerSessionMessage
			err     error
		}{message, err}
	}()
	select {
	case value := <-result:
		return value.message, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
