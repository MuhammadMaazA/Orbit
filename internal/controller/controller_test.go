package controller

import (
	"errors"
	"testing"
	"time"

	"orbit/internal/model"
	"orbit/internal/scheduler"
)

func testCapacity(cpu int) model.Capacity {
	resources := model.ResourceRequest{CPU: cpu, MemoryMB: 1_024}
	return model.Capacity{Total: resources, Available: resources}
}

func TestControllerPrioritizesQueuedJobsAndSkipsBlockedJobs(t *testing.T) {
	c, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 2, MaxQueuedJobs: 10, AgingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(model.Job{ID: "blocked", CPU: 3, Priority: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(model.Job{ID: "ready", CPU: 1, Priority: 1}); err != nil {
		t.Fatal(err)
	}
	assignments, err := c.RegisterWorker("worker-a", "session-a", testCapacity(1))
	if err != nil || len(assignments) != 1 || assignments[0].Job.ID != "ready" {
		t.Fatalf("RegisterWorker() = %+v, %v", assignments, err)
	}
}

func TestControllerDrainAndUndrain(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterWorker("worker-b", "session-b", testCapacity(2)); err != nil {
		t.Fatal(err)
	}
	if err := c.DrainWorker("worker-a"); err != nil {
		t.Fatal(err)
	}
	if assignments, err := c.Submit(model.Job{ID: "job-1", CPU: 1}); err != nil || len(assignments) != 1 || assignments[0].WorkerID != "worker-b" {
		t.Fatalf("Submit() = %+v, %v", assignments, err)
	}
	if stats := c.Stats(); stats.Draining != 1 {
		t.Fatalf("Stats() = %+v", stats)
	}
	if assignments, err := c.UndrainWorker("worker-a"); err != nil || len(assignments) != 0 {
		t.Fatalf("UndrainWorker() = %+v, %v", assignments, err)
	}
}

func TestControllerRejectsQueuedJobsOverLimit(t *testing.T) {
	c, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 2, MaxQueuedJobs: 1, AgingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(model.Job{ID: "job-1", CPU: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(model.Job{ID: "job-2", CPU: 2}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second Submit() error = %v, want ErrQueueFull", err)
	}
}

func TestControllerReconnectRequeuesOldSession(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2)); err != nil {
		t.Fatal(err)
	}
	assignments, err := c.Submit(model.Job{ID: "job-1", CPU: 2})
	if err != nil || len(assignments) != 1 {
		t.Fatalf("Submit() = %+v, %v", assignments, err)
	}
	reconnected, err := c.RegisterWorker("worker-a", "session-b", testCapacity(2))
	if err != nil || len(reconnected) != 1 || reconnected[0].SessionID != "session-b" || reconnected[0].Attempt != 2 {
		t.Fatalf("reconnect = %+v, %v", reconnected, err)
	}
}

func TestControllerExpiresWorkersAfterHeartbeatTimeout(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	if _, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2)); err != nil {
		t.Fatal(err)
	}
	if err := c.Heartbeat("worker-a", "session-a", base); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExpireWorkers(base.Add(5*time.Second), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Heartbeat("worker-a", "session-a", base.Add(6*time.Second)); err == nil {
		t.Fatal("expired worker accepted heartbeat")
	}
}

func TestControllerFailsJobAfterMaxAttempts(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	assignments, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2))
	if err != nil {
		t.Fatal(err)
	}
	newAssignments, err := c.Submit(model.Job{ID: "job-1", CPU: 2, MemoryMB: 512})
	if err != nil {
		t.Fatal(err)
	}
	assignments = append(assignments, newAssignments...)
	if _, err := c.WorkerLost(assignments[0].WorkerID, assignments[0].SessionID); err != nil {
		t.Fatal(err)
	}
	view, _ := c.GetJob("job-1")
	if view.Status != Failed || view.Attempts != 1 {
		t.Fatalf("job view = %+v", view)
	}
}

func TestControllerQueuesAndReschedules(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if assignments, err := c.Submit(model.Job{ID: "job-1", CPU: 2, MemoryMB: 512}); err != nil || len(assignments) != 0 {
		t.Fatalf("Submit() = %+v, %v; want queued job", assignments, err)
	}
	assignments, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2))
	if err != nil || len(assignments) != 1 {
		t.Fatalf("RegisterWorker() = %+v, %v", assignments, err)
	}
	first := assignments[0]
	if first.ID != "job-1:1" {
		t.Fatalf("assignment ID = %q", first.ID)
	}
	assignments, err = c.WorkerLost("worker-a", "session-a")
	if err != nil || len(assignments) != 0 {
		t.Fatalf("WorkerLost() = %+v, %v; want queued job", assignments, err)
	}
	assignments, err = c.RegisterWorker("worker-b", "session-b", testCapacity(2))
	if err != nil || len(assignments) != 1 {
		t.Fatalf("replacement RegisterWorker() = %+v, %v", assignments, err)
	}
	if assignments[0].ID != "job-1:2" || assignments[0].WorkerID != "worker-b" {
		t.Fatalf("replacement assignment = %+v", assignments[0])
	}
	view, _ := c.GetJob("job-1")
	if view.Status != Running || view.Assignment.ID != "job-1:2" {
		t.Fatalf("job view = %+v", view)
	}
}

func TestControllerRejectsStaleCompletion(t *testing.T) {
	c, err := New(scheduler.FirstFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	assignments, err := c.RegisterWorker("worker-a", "session-a", testCapacity(2))
	if err != nil {
		t.Fatal(err)
	}
	jobAssignments, err := c.Submit(model.Job{ID: "job-1", CPU: 2, MemoryMB: 512})
	if err != nil {
		t.Fatal(err)
	}
	assignments = append(assignments, jobAssignments...)
	first := assignments[0]
	if _, accepted, err := c.Complete(Assignment{ID: first.ID, Job: first.Job, WorkerID: first.WorkerID, SessionID: "old-session", Attempt: first.Attempt}, true); err != nil || accepted {
		t.Fatalf("stale Complete() = accepted=%t, err=%v", accepted, err)
	}
	view, _ := c.GetJob("job-1")
	if view.Status != Running || view.Assignment.ID != first.ID {
		t.Fatalf("stale completion changed job = %+v", view)
	}
}
