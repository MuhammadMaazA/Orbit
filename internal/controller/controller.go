package controller

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/energy"
	"github.com/MuhammadMaazA/Orbit/internal/model"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"github.com/MuhammadMaazA/Orbit/internal/storage"
)

type JobStatus string

const (
	Queued    JobStatus = "queued"
	Running   JobStatus = "running"
	Completed JobStatus = "completed"
	Failed    JobStatus = "failed"
)

var ErrQueueFull = errors.New("controller: job queue is full")

type Config struct {
	MaxAttempts    int
	MaxQueuedJobs  int
	AgingInterval  time.Duration
	Store          storage.Store
	PowerIdleWatts float64
	PowerCPUWatts  float64
	PowerGPUWatts  float64
}

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
	Workers          int
	Draining         int
	Queued           int
	Running          int
	Completed        int
	Failed           int
	Requeued         uint64
	ActiveWorkers    int
	EnergyJoules     float64
	ActiveWorkerTime float64
}

type workerState struct {
	id       string
	capacity model.Capacity
	session  string
	seen     time.Time
	draining bool
}

type jobState struct {
	job        model.Job
	status     JobStatus
	attempts   int
	assignment *Assignment
	enqueuedAt time.Time
	sequence   uint64
}

type Controller struct {
	mu          sync.Mutex
	policy      scheduler.Policy
	maxAttempts int
	workers     map[string]*workerState
	jobs        map[string]*jobState
	queue       []string
	maxQueued   int
	aging       time.Duration
	nextSeq     uint64
	now         func() time.Time
	requeued    uint64
	store       storage.Store
	energy      *energy.Model
}

func New(policy scheduler.Policy, maxAttempts int) (*Controller, error) {
	return NewWithConfig(policy, Config{MaxAttempts: maxAttempts, MaxQueuedJobs: 1_000, AgingInterval: 30 * time.Second})
}

func NewWithConfig(policy scheduler.Policy, config Config) (*Controller, error) {
	if policy == nil {
		return nil, fmt.Errorf("controller: nil scheduling policy")
	}
	if config.MaxAttempts < 1 {
		return nil, fmt.Errorf("controller: max attempts must be positive")
	}
	if config.MaxQueuedJobs < 0 {
		return nil, fmt.Errorf("controller: max queued jobs cannot be negative")
	}
	if config.AgingInterval <= 0 {
		return nil, fmt.Errorf("controller: aging interval must be positive")
	}
	if config.PowerIdleWatts == 0 && config.PowerCPUWatts == 0 && config.PowerGPUWatts == 0 {
		config.PowerIdleWatts, config.PowerCPUWatts, config.PowerGPUWatts = 100, 10, 50
	}
	if config.PowerIdleWatts < 0 || config.PowerCPUWatts < 0 || config.PowerGPUWatts < 0 {
		return nil, fmt.Errorf("controller: power values cannot be negative")
	}
	controller := &Controller{
		policy:      policy,
		maxAttempts: config.MaxAttempts,
		workers:     make(map[string]*workerState),
		jobs:        make(map[string]*jobState),
		maxQueued:   config.MaxQueuedJobs,
		aging:       config.AgingInterval,
		now:         time.Now,
		energy:      energy.New(energy.Config{IdleWatts: config.PowerIdleWatts, CPUWatts: config.PowerCPUWatts, GPUWatts: config.PowerGPUWatts}),
	}
	if config.Store != nil {
		if err := controller.SetStore(config.Store); err != nil {
			return nil, err
		}
	}
	return controller, nil
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
	if previous := c.workers[id]; previous != nil {
		if previous.session == session {
			return nil, fmt.Errorf("register worker %q: session already registered", id)
		}
		c.expireWorkerLocked(id, previous.session)
	}
	at := c.now()
	c.workers[id] = &workerState{id: id, session: session, capacity: capacity, seen: at}
	c.energy.Register(id, capacity, c.energyTime(at))
	assignments := c.scheduleLocked()
	return assignments, c.persistLocked("worker_registered")
}

func (c *Controller) DrainWorker(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := c.workers[id]
	if worker == nil {
		return fmt.Errorf("drain worker %q: not found", id)
	}
	worker.draining = true
	return c.persistLocked("worker_drained")
}

func (c *Controller) UndrainWorker(id string) ([]Assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := c.workers[id]
	if worker == nil {
		return nil, fmt.Errorf("undrain worker %q: not found", id)
	}
	worker.draining = false
	assignments := c.scheduleLocked()
	return assignments, c.persistLocked("worker_undrained")
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
	assignments := c.scheduleLocked()
	return assignments, c.persistLocked("workers_expired")
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
	if c.maxQueued > 0 && len(c.queue) >= c.maxQueued && !c.canFitAnyWorkerLocked(job) {
		return nil, ErrQueueFull
	}
	c.jobs[job.ID] = &jobState{job: job, status: Queued}
	c.enqueueLocked(c.jobs[job.ID])
	assignments := c.scheduleLocked()
	return assignments, c.persistLocked("job_submitted")
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
	c.energy.Observe(worker.id, worker.capacity, c.energyTime(c.now()))
	job.assignment = nil
	if success {
		job.status = Completed
	} else {
		job.status = Failed
	}
	assignments := c.scheduleLocked()
	return assignments, true, c.persistLocked("job_completed")
}

func (c *Controller) WorkerLost(workerID, session string) ([]Assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := c.workers[workerID]
	if worker == nil || worker.session != session {
		return nil, nil
	}
	c.expireWorkerLocked(workerID, session)
	assignments := c.scheduleLocked()
	return assignments, c.persistLocked("worker_lost")
}

func (c *Controller) expireWorkerLocked(workerID, session string) {
	if worker := c.workers[workerID]; worker != nil && worker.session == session {
		c.energy.Remove(workerID, c.energyTime(c.now()))
	}
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
			c.requeued++
			c.enqueueLocked(job)
		} else {
			job.status = Failed
		}
	}
}

func (c *Controller) enqueueLocked(job *jobState) {
	c.nextSeq++
	job.enqueuedAt = c.now()
	job.sequence = c.nextSeq
	c.queue = append(c.queue, job.job.ID)
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

func (c *Controller) ListJobs() []JobView {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.jobs))
	for id := range c.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	views := make([]JobView, 0, len(ids))
	for _, id := range ids {
		job := c.jobs[id]
		views = append(views, JobView{Job: job.job, Status: job.status, Assignment: copyAssignment(job.assignment), Attempts: job.attempts})
	}
	return views
}

func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	energyStats := c.energy.Snapshot(c.energyTime(c.now()))
	stats := Stats{Workers: len(c.workers), Queued: len(c.queue), Requeued: c.requeued, ActiveWorkers: energyStats.ActiveWorkers, EnergyJoules: energyStats.Joules, ActiveWorkerTime: energyStats.ActiveWorkerTime}
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
	for _, worker := range c.workers {
		if worker.draining {
			stats.Draining++
		}
	}
	return stats
}

func (c *Controller) scheduleLocked() []Assignment {
	var result []Assignment
	for len(c.queue) > 0 {
		c.sortQueueLocked()
		workers := c.workerListLocked()
		queueIndex, workerIndex := c.nextSchedulableLocked(workers)
		if queueIndex < 0 {
			break
		}
		jobID := c.queue[queueIndex]
		job := c.jobs[jobID]
		worker := c.workers[workers[workerIndex].ID]
		if err := worker.capacity.Allocate(job.job.Resources()); err != nil {
			break
		}
		c.energy.Observe(worker.id, worker.capacity, c.energyTime(c.now()))
		c.queue = append(c.queue[:queueIndex], c.queue[queueIndex+1:]...)
		job.attempts++
		assignment := Assignment{ID: fmt.Sprintf("%s:%d", job.job.ID, job.attempts), Job: job.job, WorkerID: worker.id, SessionID: worker.session, Attempt: job.attempts}
		job.assignment = &assignment
		job.status = Running
		result = append(result, assignment)
	}
	return result
}

func (c *Controller) energyTime(at time.Time) float64 {
	return float64(at.UnixNano()) / 1e9
}

func (c *Controller) nextSchedulableLocked(workers []model.Worker) (int, int) {
	for queueIndex, jobID := range c.queue {
		job := c.jobs[jobID]
		workerIndex, ok := c.policy.Select(workers, job.job)
		if ok {
			return queueIndex, workerIndex
		}
	}
	return -1, -1
}

func (c *Controller) sortQueueLocked() {
	now := c.now()
	sort.SliceStable(c.queue, func(i, j int) bool {
		a, b := c.jobs[c.queue[i]], c.jobs[c.queue[j]]
		aPriority := effectivePriority(a, now, c.aging)
		bPriority := effectivePriority(b, now, c.aging)
		if aPriority != bPriority {
			return aPriority > bPriority
		}
		if a.sequence != b.sequence {
			return a.sequence < b.sequence
		}
		return a.job.ID < b.job.ID
	})
}

func effectivePriority(job *jobState, now time.Time, aging time.Duration) int {
	if job.enqueuedAt.IsZero() || now.Before(job.enqueuedAt) {
		return job.job.Priority
	}
	return job.job.Priority + int(now.Sub(job.enqueuedAt)/aging)
}

func (c *Controller) canFitAnyWorkerLocked(job model.Job) bool {
	workers := c.workerListLocked()
	_, ok := c.policy.Select(workers, job)
	return ok
}

func (c *Controller) workerListLocked() []model.Worker {
	workers := make([]model.Worker, 0, len(c.workers))
	for _, worker := range c.workers {
		if worker.draining {
			continue
		}
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
