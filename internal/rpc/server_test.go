package rpc

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/controller"
	"github.com/MuhammadMaazA/Orbit/internal/metrics"
	"github.com/MuhammadMaazA/Orbit/internal/model"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"github.com/MuhammadMaazA/Orbit/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
)

type failingStore struct{}

func (failingStore) Append(storage.Event) error                { return fmt.Errorf("disk full") }
func (failingStore) Snapshot([]byte) error                     { return fmt.Errorf("disk full") }
func (failingStore) Recover() ([]byte, []storage.Event, error) { return nil, nil, nil }

type fakeStream struct {
	grpc.ServerStream
	sent []*v1.ControllerSessionMessage
}

func (f *fakeStream) Send(message *v1.ControllerSessionMessage) error {
	f.sent = append(f.sent, message)
	return nil
}

func (f *fakeStream) Recv() (*v1.WorkerSessionMessage, error) { return nil, io.EOF }

type sequenceStream struct {
	grpc.ServerStream
	messages []*v1.WorkerSessionMessage
	index    int
	sent     []*v1.ControllerSessionMessage
}

func (s *sequenceStream) Send(message *v1.ControllerSessionMessage) error {
	s.sent = append(s.sent, message)
	return nil
}

func (s *sequenceStream) Recv() (*v1.WorkerSessionMessage, error) {
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.index]
	s.index++
	return message, nil
}

func TestRegisterIncrementsWorkersRegisteredNotDrain(t *testing.T) {
	c, err := controller.NewWithConfig(scheduler.FirstFit{}, controller.Config{MaxAttempts: 3, MaxQueuedJobs: 10, AgingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(prometheus.NewRegistry())
	server := NewServer(c, m)
	stream := &sequenceStream{messages: []*v1.WorkerSessionMessage{
		{Payload: &v1.WorkerSessionMessage_Register{Register: &v1.Worker{Id: "worker-a", SessionId: "session-a", Total: &v1.ResourceRequest{Cpu: 4, MemoryMb: 4096}}}},
	}}
	_ = server.WorkerSession(stream)
	if got := testutil.ToFloat64(m.WorkersRegistered); got != 1 {
		t.Fatalf("WorkersRegistered after register = %v, want 1", got)
	}

	// worker-b is registered directly against the controller (bypassing the
	// streamed Register path, so it can't touch the metric on its own) and
	// then drained through the server - draining must not move the counter.
	capacity := model.Capacity{Total: model.ResourceRequest{CPU: 1, MemoryMB: 1}, Available: model.ResourceRequest{CPU: 1, MemoryMB: 1}}
	if _, err := c.RegisterWorker("worker-b", "session-b", capacity); err != nil {
		t.Fatal(err)
	}
	if _, err := server.DrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: "worker-b"}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.WorkersRegistered); got != 1 {
		t.Fatalf("WorkersRegistered after drain = %v, want still 1 (drain must not increment it)", got)
	}
}

func TestSubmitDispatchesDespitePersistenceFailure(t *testing.T) {
	c, err := controller.NewWithConfig(scheduler.FirstFit{}, controller.Config{MaxAttempts: 3, MaxQueuedJobs: 10, AgingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	capacity := model.Capacity{Total: model.ResourceRequest{CPU: 4, MemoryMB: 4096}, Available: model.ResourceRequest{CPU: 4, MemoryMB: 4096}}
	if _, err := c.RegisterWorker("worker-a", "session-a", capacity); err != nil {
		t.Fatal(err)
	}
	// Attach a store that fails on every write, simulating persistence
	// breaking after the worker is already registered and schedulable.
	if err := c.SetStore(failingStore{}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(c)
	stream := &fakeStream{}
	server.sessions["worker-a"] = &workerSession{stream: stream}

	_, err = server.Submit(context.Background(), &v1.Job{Id: "job-1", Resources: &v1.ResourceRequest{Cpu: 1, MemoryMb: 1}})
	if err == nil {
		t.Fatal("expected Submit to surface the persistence error")
	}
	if len(stream.sent) != 1 {
		t.Fatalf("dispatch was dropped when persistence failed: sent %d messages, want 1", len(stream.sent))
	}
}
