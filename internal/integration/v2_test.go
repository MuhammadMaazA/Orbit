package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/controller"
	"github.com/MuhammadMaazA/Orbit/internal/rpc"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerDrainAndUndrainRPC(t *testing.T) {
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
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := v1.NewOrbitControllerClient(connection)
	stream, err := client.WorkerSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	worker := &v1.Worker{Id: "worker-a", SessionId: "session-a", Total: &v1.ResourceRequest{Cpu: 2, MemoryMb: 1_024}}
	if err := stream.Send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Register{Register: worker}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_HeartbeatSessionId{HeartbeatSessionId: worker.SessionId}}); err != nil {
		t.Fatal(err)
	}
	if ack, err := stream.Recv(); err != nil || ack.GetHeartbeatAckSessionId() != worker.SessionId {
		t.Fatalf("registration ack = %+v, %v", ack, err)
	}
	if response, err := client.DrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: worker.Id}); err != nil || !response.Draining {
		t.Fatalf("DrainWorker() = %+v, %v", response, err)
	}
	assignment, err := client.Submit(context.Background(), &v1.Job{Id: "job-1", Resources: &v1.ResourceRequest{Cpu: 1}})
	if err != nil || assignment.Id != "" {
		t.Fatalf("queued Submit() = %+v, %v", assignment, err)
	}
	if response, err := client.UndrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: worker.Id}); err != nil || response.Draining {
		t.Fatalf("UndrainWorker() = %+v, %v", response, err)
	}
	message, err := stream.Recv()
	if err != nil || message.GetAssignment().Job.Id != "job-1" {
		t.Fatalf("undrain assignment = %+v, %v", message.GetAssignment(), err)
	}
}

func TestAdmissionLimitReturnsResourceExhausted(t *testing.T) {
	state, err := controller.NewWithConfig(scheduler.FirstFit{}, controller.Config{MaxAttempts: 2, MaxQueuedJobs: 1, AgingInterval: time.Hour})
	if err != nil {
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
	job := func(id string) *v1.Job { return &v1.Job{Id: id, Resources: &v1.ResourceRequest{Cpu: 2}} }
	if _, err := client.Submit(context.Background(), job("job-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Submit(context.Background(), job("job-2")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second Submit() error = %v, code = %s", err, status.Code(err))
	}
}
