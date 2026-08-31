package controller

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/model"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"github.com/MuhammadMaazA/Orbit/internal/storage"
)

func TestControllerRecoversRunningJobAsNewAttempt(t *testing.T) {
	directory := t.TempDir()
	store, err := storage.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 3, MaxQueuedJobs: 10, AgingInterval: time.Hour, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	resources := testCapacity(2)
	if _, err := c.RegisterWorker("worker-a", "session-a", resources); err != nil {
		t.Fatal(err)
	}
	if assignments, err := c.Submit(model.Job{ID: "job-1", CPU: 2}); err != nil || len(assignments) != 1 {
		t.Fatalf("Submit() = %+v, %v", assignments, err)
	}
	recoveredStore, err := storage.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 3, MaxQueuedJobs: 10, AgingInterval: time.Hour, Store: recoveredStore})
	if err != nil {
		t.Fatal(err)
	}
	view, ok := recovered.GetJob("job-1")
	if !ok || view.Status != Queued || view.Attempts != 2 || view.Assignment != nil {
		t.Fatalf("recovered job = %+v, found=%t", view, ok)
	}
	assignments, err := recovered.RegisterWorker("worker-b", "session-b", resources)
	if err != nil || len(assignments) != 1 || assignments[0].Attempt != 3 {
		t.Fatalf("recovered assignment = %+v, %v", assignments, err)
	}
}

func TestSetStorePrefersLatestWALEventOverStaleSnapshot(t *testing.T) {
	directory := t.TempDir()
	store, err := storage.NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	job1 := persistedJob{Job: model.Job{ID: "job-1", CPU: 2}, Status: Queued}
	job2 := persistedJob{Job: model.Job{ID: "job-2", CPU: 2}, Status: Queued}
	older, err := json.Marshal(persistedState{Jobs: []persistedJob{job1}, Queue: []string{"job-1"}})
	if err != nil {
		t.Fatal(err)
	}
	newer, err := json.Marshal(persistedState{Jobs: []persistedJob{job1, job2}, Queue: []string{"job-1", "job-2"}})
	if err != nil {
		t.Fatal(err)
	}
	// Simulates a crash between persistLocked's WAL append and its
	// snapshot.json rewrite: the WAL ends up ahead of the snapshot file.
	if err := store.Snapshot(older); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(storage.Event{Type: "job_submitted", Data: newer}); err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 3, MaxQueuedJobs: 10, AgingInterval: time.Hour, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.GetJob("job-2"); !ok {
		t.Fatal("SetStore restored the stale snapshot instead of the newer WAL entry")
	}
}
