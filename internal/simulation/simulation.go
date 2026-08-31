package simulation

import (
	"container/heap"
	"fmt"
	"sort"

	"orbit/internal/energy"
	"orbit/internal/model"
	"orbit/internal/scheduler"
)

type Job struct {
	Spec     model.Job
	Arrival  int
	Duration int
}

type Worker struct {
	ID       string
	Capacity model.Capacity
}

type Result struct {
	Policy            string
	Jobs              int
	Completed         int
	Makespan          int
	AverageWait       float64
	AverageTurnaround float64
	EnergyJoules      float64
	ActiveWorkerTime  float64
}

type event struct {
	at      int
	order   int
	jobID   string
	worker  int
	arrival bool
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].order < q[j].order
}
func (q eventQueue) Swap(i, j int)   { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(value any) { *q = append(*q, value.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	value := old[len(old)-1]
	*q = old[:len(old)-1]
	return value
}

func Run(policy scheduler.Policy, workers []Worker, jobs []Job) (Result, error) {
	if policy == nil {
		return Result{}, fmt.Errorf("simulate: nil policy")
	}
	if len(workers) == 0 {
		return Result{}, fmt.Errorf("simulate: no workers")
	}
	for _, job := range jobs {
		if job.Arrival < 0 || job.Duration <= 0 {
			return Result{}, fmt.Errorf("simulate job %q: invalid timing", job.Spec.ID)
		}
	}
	jobs = append([]Job(nil), jobs...)
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].Arrival < jobs[j].Arrival })
	workerState := make([]model.Worker, len(workers))
	energyModel := energy.New(energy.Config{IdleWatts: 100, CPUWatts: 10, GPUWatts: 50})
	seenWorkers := make(map[string]struct{}, len(workers))
	for i, worker := range workers {
		if worker.ID == "" {
			return Result{}, fmt.Errorf("simulate: worker ID is required")
		}
		if _, exists := seenWorkers[worker.ID]; exists {
			return Result{}, fmt.Errorf("simulate: duplicate worker %q", worker.ID)
		}
		if err := worker.Capacity.Valid(); err != nil {
			return Result{}, fmt.Errorf("simulate worker %q: %w", worker.ID, err)
		}
		seenWorkers[worker.ID] = struct{}{}
		workerState[i] = model.Worker{ID: worker.ID, Capacity: worker.Capacity}
		energyModel.Register(worker.ID, worker.Capacity, 0)
	}
	jobByID := make(map[string]Job, len(jobs))
	queue := make([]string, 0, len(jobs))
	events := &eventQueue{}
	for i, job := range jobs {
		if job.Spec.ID == "" {
			return Result{}, fmt.Errorf("simulate: job ID is required")
		}
		if _, exists := jobByID[job.Spec.ID]; exists {
			return Result{}, fmt.Errorf("simulate: duplicate job %q", job.Spec.ID)
		}
		jobByID[job.Spec.ID] = job
		heap.Push(events, event{at: job.Arrival, order: i, jobID: job.Spec.ID, arrival: true})
	}
	starts := make(map[string]int, len(jobs))
	completed := 0
	var wait, turnaround int
	lastTime := 0
	for events.Len() > 0 || len(queue) > 0 {
		if events.Len() > 0 {
			next := (*events)[0].at
			if len(queue) == 0 || next <= lastTime {
				lastTime = next
			}
		}
		for events.Len() > 0 && (*events)[0].at == lastTime {
			e := heap.Pop(events).(event)
			if e.arrival {
				queue = append(queue, e.jobID)
				continue
			}
			workerState[e.worker].Capacity.Release(jobByID[e.jobID].Spec.Resources())
			energyModel.Observe(workerState[e.worker].ID, workerState[e.worker].Capacity, float64(lastTime))
			completed++
			turnaround += lastTime - jobByID[e.jobID].Arrival
		}
		for len(queue) > 0 {
			sort.SliceStable(queue, func(i, j int) bool {
				left, right := jobByID[queue[i]], jobByID[queue[j]]
				leftPriority := left.Spec.Priority + (lastTime-left.Arrival)/30
				rightPriority := right.Spec.Priority + (lastTime-right.Arrival)/30
				if leftPriority != rightPriority {
					return leftPriority > rightPriority
				}
				return left.Arrival < right.Arrival
			})
			candidateIndex, workerIndex := -1, -1
			var candidate Job
			for queueIndex, jobID := range queue {
				candidate = jobByID[jobID]
				workersForPolicy := make([]model.Worker, len(workerState))
				copy(workersForPolicy, workerState)
				selectedWorker, ok := policy.Select(workersForPolicy, candidate.Spec)
				if !ok {
					continue
				}
				candidateIndex, workerIndex = queueIndex, selectedWorker
				break
			}
			if candidateIndex < 0 {
				break
			}
			if err := workerState[workerIndex].Capacity.Allocate(candidate.Spec.Resources()); err != nil {
				return Result{}, err
			}
			energyModel.Observe(workerState[workerIndex].ID, workerState[workerIndex].Capacity, float64(lastTime))
			queue = append(queue[:candidateIndex], queue[candidateIndex+1:]...)
			starts[candidate.Spec.ID] = lastTime
			wait += lastTime - candidate.Arrival
			heap.Push(events, event{at: lastTime + candidate.Duration, order: len(starts), jobID: candidate.Spec.ID, worker: workerIndex})
		}
		if events.Len() > 0 && (len(queue) == 0 || (*events)[0].at > lastTime) {
			lastTime = (*events)[0].at
		}
		if events.Len() == 0 && len(queue) > 0 {
			break
		}
	}
	energyStats := energyModel.Snapshot(float64(lastTime))
	return Result{Policy: policy.Name(), Jobs: len(jobs), Completed: completed, Makespan: lastTime, AverageWait: average(wait, completed), AverageTurnaround: average(turnaround, completed), EnergyJoules: energyStats.Joules, ActiveWorkerTime: energyStats.ActiveWorkerTime}, nil
}

func average(total, count int) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}
