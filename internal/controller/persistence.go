package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/model"
	"github.com/MuhammadMaazA/Orbit/internal/storage"
)

type persistedState struct {
	Jobs     []persistedJob `json:"jobs"`
	Queue    []string       `json:"queue"`
	NextSeq  uint64         `json:"next_seq"`
	Requeued uint64         `json:"requeued"`
}

type persistedJob struct {
	Job        model.Job   `json:"job"`
	Status     JobStatus   `json:"status"`
	Attempts   int         `json:"attempts"`
	Assignment *Assignment `json:"assignment,omitempty"`
	EnqueuedAt int64       `json:"enqueued_at,omitempty"`
	Sequence   uint64      `json:"sequence,omitempty"`
}

func (c *Controller) SetStore(store storage.Store) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = store
	if store == nil {
		return nil
	}
	snapshot, events, err := store.Recover()
	if err != nil {
		return err
	}
	if len(snapshot) == 0 && len(events) > 0 {
		snapshot = events[len(events)-1].Data
	}
	if len(snapshot) == 0 {
		return nil
	}
	if err := c.restoreLocked(snapshot); err != nil {
		return fmt.Errorf("controller: restore state: %w", err)
	}
	return nil
}

func (c *Controller) persistLocked(eventType string) error {
	if c.store == nil {
		return nil
	}
	snapshot, err := c.snapshotLocked()
	if err != nil {
		return err
	}
	if err := c.store.Append(storage.Event{Type: eventType, Timestamp: c.now(), Data: snapshot}); err != nil {
		return err
	}
	return c.store.Snapshot(snapshot)
}

func (c *Controller) snapshotLocked() ([]byte, error) {
	state := persistedState{Queue: append([]string(nil), c.queue...), NextSeq: c.nextSeq, Requeued: c.requeued}
	jobIDs := make([]string, 0, len(c.jobs))
	for id := range c.jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	for _, id := range jobIDs {
		job := c.jobs[id]
		state.Jobs = append(state.Jobs, persistedJob{Job: job.job, Status: job.status, Attempts: job.attempts, Assignment: copyAssignment(job.assignment), EnqueuedAt: job.enqueuedAt.UnixNano(), Sequence: job.sequence})
	}
	return json.Marshal(state)
}

func (c *Controller) restoreLocked(data []byte) error {
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	c.workers = make(map[string]*workerState)
	c.jobs = make(map[string]*jobState, len(state.Jobs))
	c.queue = nil
	c.nextSeq = state.NextSeq
	c.requeued = state.Requeued
	for _, saved := range state.Jobs {
		job := &jobState{job: saved.Job, status: saved.Status, attempts: saved.Attempts, assignment: copyAssignment(saved.Assignment), sequence: saved.Sequence}
		if saved.EnqueuedAt != 0 {
			job.enqueuedAt = time.Unix(0, saved.EnqueuedAt)
		}
		if job.status == Running {
			job.assignment = nil
			job.attempts++
			if job.attempts < c.maxAttempts {
				job.status = Queued
				c.enqueueLocked(job)
			} else {
				job.status = Failed
			}
		} else if job.status == Queued {
			c.jobs[job.job.ID] = job
			c.queue = append(c.queue, job.job.ID)
			continue
		}
		c.jobs[job.job.ID] = job
	}
	return nil
}
