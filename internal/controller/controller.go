package controller

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"orbit/internal/model"
	"orbit/internal/scheduler"
)

type JobStatus string

const (
	Queued    JobStatus = "queued"
	Running   JobStatus = "running"
	Completed JobStatus = "completed"
	Failed    JobStatus = "failed"
)

type Assignment struct {
	ID        string
	Job       model.Job
	WorkerID  string
	SessionID string
	Attempt   int
}

type JobView struct {
	Job        model.Job
	Status     JobStatus
	Assignment *Assignment
	Attempts   int
}

type Stats struct {
	Workers   int
	Queued    int
	Running   int
	Completed int
	Failed    int
}

type workerState struct {
	id       string
	capacity model.Capacity
	session  string
	seen     time.Time
}

type jobState struct {
	job        model.Job
	status     JobStatus
	attempts   int
	assignment *Assignment
}

type Controller struct {
	mu          sync.Mutex
	policy      scheduler.Policy
	maxAttempts int
	workers     map[string]*workerState
	jobs        map[string]*jobState
	queue       []string
}

func New(policy scheduler.Policy, maxAttempts int) (*Controller, error) {
	if policy == nil {
		return nil, fmt.Errorf("controller: nil scheduling policy")
	}
	if maxAttempts < 1 {
		return nil, fmt.Errorf("controller: max attempts must be positive")
	}
	return &Controller{
		policy:      policy,
		maxAttempts: maxAttempts,
		workers:     make(map[string]*workerState),
		jobs:        make(map[string]*jobState),
	}, nil
}

func (c *Controller) RegisterWorker(id, session string, capacity model.Capacity) ([]Assignment, error) {
	if id == "" || session == "" {
		return nil, fmt.Errorf("register worker: id and session are required")
	}
	if err := capacity.Valid(); err != nil {
		return nil, fmt.Errorf("register worker %q: %w", id, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[id] = &workerState{id: id, session: session, capacity: capacity, seen: time.Now()}
	return c.scheduleLocked(), nil
}

func (c *Controller) Heartbeat(workerID, session string, at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := c.workers[workerID]
	if worker == nil || worker.session != session {
		return fmt.Errorf("heartbeat worker %q: stale session", workerID)
	}
	worker.seen = at
	return nil
}

func (c *Controller) ExpireWorkers(now time.Time, timeout time.Duration) ([]Assignment, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("expire workers: timeout must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	workerIDs := make([]string, 0, len(c.workers))
	for id := range c.workers {
		workerIDs = append(workerIDs, id)
	}
	sort.Strings(workerIDs)
	for _, id := range workerIDs {
		worker := c.workers[id]
		if now.Sub(worker.seen) >= timeout {
			c.expireWorkerLocked(id, worker.session)
		}
	}
	return c.scheduleLocked(), nil
}

func (c *Controller) Submit(job model.Job) ([]Assignment, error) {
	if job.ID == "" {
		return nil, fmt.Errorf("submit job: id is required")
	}
	if request := job.Resources(); request.CPU < 0 || request.MemoryMB < 0 || request.GPU < 0 {
		return nil, fmt.Errorf("submit job %q: resource requests must be non-negative", job.ID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.jobs[job.ID]; exists {
		return nil, fmt.Errorf("submit job %q: already exists", job.ID)
	}
	c.jobs[job.ID] = &jobState{job: job, status: Queued}
	c.queue = append(c.queue, job.ID)
	return c.scheduleLocked(), nil
}

func (c *Controller) Complete(assignment Assignment, success bool) ([]Assignment, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.jobs[assignment.Job.ID]
	if job == nil || job.assignment == nil {
		return nil, false, nil
	}
	if job.assignment.ID != assignment.ID || job.assignment.WorkerID != assignment.WorkerID || job.assignment.SessionID != assignment.SessionID || job.assignment.Attempt != assignment.Attempt {
		return nil, false, nil
	}
	worker := c.workers[job.assignment.WorkerID]
	if worker == nil {
		return nil, false, nil
	}
	if err := worker.capacity.Release(job.assignment.Job.Resources()); err != nil {
		return nil, false, fmt.Errorf("complete job %q: %w", assignment.Job.ID, err)
	}
	job.assignment = nil
	if success {
		job.status = Completed
	} else {
		job.status = Failed
	}
	return c.scheduleLocked(), true, nil
}

func (c *Controller) WorkerLost(workerID, session string) ([]Assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := c.workers[workerID]
	if worker == nil || worker.session != session {
		return nil, nil
	}
	c.expireWorkerLocked(workerID, session)
	return c.scheduleLocked(), nil
}

func (c *Controller) expireWorkerLocked(workerID, session string) {
	delete(c.workers, workerID)
	jobIDs := make([]string, 0, len(c.jobs))
	for id := range c.jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	for _, id := range jobIDs {
		job := c.jobs[id]
		if job.assignment == nil || job.assignment.WorkerID != workerID || job.assignment.SessionID != session {
			continue
		}
		job.assignment = nil
		if job.attempts < c.maxAttempts {
			job.status = Queued
			c.queue = append(c.queue, job.job.ID)
		} else {
			job.status = Failed
		}
	}
}

func (c *Controller) GetJob(id string) (JobView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	job, ok := c.jobs[id]
	if !ok {
		return JobView{}, false
	}
	return JobView{Job: job.job, Status: job.status, Assignment: copyAssignment(job.assignment), Attempts: job.attempts}, true
}

func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := Stats{Workers: len(c.workers), Queued: len(c.queue)}
	for _, job := range c.jobs {
		switch job.status {
		case Running:
			stats.Running++
		case Completed:
			stats.Completed++
		case Failed:
			stats.Failed++
		}
	}
	return stats
}

func (c *Controller) scheduleLocked() []Assignment {
	var result []Assignment
	for len(c.queue) > 0 {
		jobID := c.queue[0]
		job := c.jobs[jobID]
		workers := c.workerListLocked()
		index, ok := c.policy.Select(workers, job.job)
		if !ok {
			break
		}
		worker := c.workers[workers[index].ID]
		if err := worker.capacity.Allocate(job.job.Resources()); err != nil {
			break
		}
		c.queue = c.queue[1:]
		job.attempts++
		assignment := Assignment{ID: fmt.Sprintf("%s:%d", job.job.ID, job.attempts), Job: job.job, WorkerID: worker.id, SessionID: worker.session, Attempt: job.attempts}
		job.assignment = &assignment
		job.status = Running
		result = append(result, assignment)
	}
	return result
}

func (c *Controller) workerListLocked() []model.Worker {
	workers := make([]model.Worker, 0, len(c.workers))
	for _, worker := range c.workers {
		workers = append(workers, model.Worker{ID: worker.id, Capacity: worker.capacity})
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers
}

func copyAssignment(assignment *Assignment) *Assignment {
	if assignment == nil {
		return nil
	}
	copy := *assignment
	return &copy
}
