package integration

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"orbit/internal/controller"
	"orbit/internal/model"
	"orbit/internal/rpc"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
	"orbit/internal/scheduler"
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
	connection, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
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
