package replay

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"orbit/internal/energy"
	"orbit/internal/model"
	"orbit/internal/scheduler"
)

type Config struct {
	Policy        scheduler.Policy
	WorkerScale   int
	GPUScale      int
	Power         energy.Config
	InjectFailure string
	Explain       bool
}

type Decision struct {
	TimeMS   int64
	JobID    string
	Policy   string
	Eligible []string
	Selected string
}

type Result struct {
	Policy                                      string
	Jobs, Completed, Retries, Failures          int
	MakespanMS                                  int64
	MeanWaitMS, P50WaitMS, P95WaitMS, P99WaitMS float64
	EnergyJoules, ActiveWorkerTime              float64
	Decisions                                   []Decision
}

type running struct {
	job     Event
	worker  string
	attempt int
	start   int64
	end     int64
}

func Run(trace Trace, config Config) (Result, error) {
	if err := trace.Validate(); err != nil {
		return Result{}, err
	}
	if config.Policy == nil {
		return Result{}, fmt.Errorf("replay: policy is required")
	}
	if config.WorkerScale <= 0 {
		config.WorkerScale = 1
	}
	if config.GPUScale <= 0 {
		config.GPUScale = 1
	}
	power := config.Power
	if power.IdleWatts == 0 && power.CPUWatts == 0 && power.GPUWatts == 0 {
		power = energy.Config{IdleWatts: 100, CPUWatts: 10, GPUWatts: 50}
	}
	workers := make(map[string]model.Worker)
	active := make(map[string]running)
	queue := make([]Event, 0)
	completed := make(map[string]bool)
	waits := make([]int64, 0)
	energyModel := energy.New(power)
	result := Result{Policy: config.Policy.Name(), Jobs: len(jobEvents(trace))}
	events := append([]Event(nil), trace.Events...)
	if config.InjectFailure != "" {
		if id, at, ok := parseFailure(config.InjectFailure); ok {
			duplicate := false
			for _, event := range events {
				if event.TimeMS == at && event.Type == WorkerFailed && event.WorkerID == id {
					duplicate = true
					break
				}
			}
			if !duplicate {
				events = append(events, Event{TimeMS: at, Type: WorkerFailed, WorkerID: id})
			}
			sort.SliceStable(events, func(i, j int) bool { return events[i].TimeMS < events[j].TimeMS })
		}
	}
	last := int64(0)
	for _, event := range events {
		if event.TimeMS < last {
			return Result{}, fmt.Errorf("replay: non-monotonic event")
		}
		last = event.TimeMS
		finish(&active, workers, completed, event.TimeMS, &result, &waits, energyModel)
		switch event.Type {
		case WorkerAdded, WorkerRecovered:
			capacity := scaledCapacity(event, config.WorkerScale, config.GPUScale)
			workers[event.WorkerID] = model.Worker{ID: event.WorkerID, Capacity: capacity}
			energyModel.Register(event.WorkerID, capacity, float64(event.TimeMS)/1000)
		case WorkerRemoved, WorkerFailed:
			for id, job := range active {
				if job.worker == event.WorkerID {
					queue = append(queue, job.job)
					delete(active, id)
					result.Retries++
					energyModel.Remove(event.WorkerID, float64(event.TimeMS)/1000)
				}
			}
			delete(workers, event.WorkerID)
			result.Failures++
		case JobSubmitted:
			queue = append(queue, event)
		}
		schedule(event.TimeMS, config, workers, &queue, active, &result, waits, energyModel)
	}
	for len(active) > 0 || len(queue) > 0 {
		next := int64(-1)
		for _, job := range active {
			if next < 0 || job.end < next {
				next = job.end
			}
		}
		if next < 0 {
			break
		}
		last = next
		finish(&active, workers, completed, last, &result, &waits, energyModel)
		schedule(last, config, workers, &queue, active, &result, waits, energyModel)
	}
	stats := energyModel.Snapshot(float64(last) / 1000)
	result.EnergyJoules, result.ActiveWorkerTime = stats.Joules, stats.ActiveWorkerTime
	result.MakespanMS = last
	setPercentiles(&result, waits)
	return result, nil
}

func schedule(at int64, config Config, workers map[string]model.Worker, queue *[]Event, active map[string]running, result *Result, waits []int64, energyModel *energy.Model) {
	for len(*queue) > 0 {
		sort.SliceStable(*queue, func(i, j int) bool {
			if (*queue)[i].Priority != (*queue)[j].Priority {
				return (*queue)[i].Priority > (*queue)[j].Priority
			}
			return (*queue)[i].JobID < (*queue)[j].JobID
		})
		ids := make([]string, 0, len(workers))
		for id := range workers {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		list := make([]model.Worker, 0, len(ids))
		for _, id := range ids {
			list = append(list, workers[id])
		}
		job := (*queue)[0]
		index, ok := config.Policy.Select(list, model.Job{ID: job.JobID, CPU: job.CPU, MemoryMB: job.MemoryMB, GPU: job.GPU, Priority: job.Priority})
		if !ok {
			break
		}
		worker := list[index]
		before := worker.Capacity
		eligible := make([]string, 0, len(list))
		for _, candidate := range list {
			if candidate.Capacity.CanFit(model.Job{CPU: job.CPU, MemoryMB: job.MemoryMB, GPU: job.GPU}.Resources()) {
				eligible = append(eligible, candidate.ID)
			}
		}
		if err := worker.Capacity.Allocate(model.Job{CPU: job.CPU, MemoryMB: job.MemoryMB, GPU: job.GPU}.Resources()); err != nil {
			break
		}
		workers[worker.ID] = worker
		active[job.JobID] = running{job: job, worker: worker.ID, attempt: 1, start: at, end: at + job.DurationMS}
		*queue = (*queue)[1:]
		result.Decisions = append(result.Decisions, Decision{TimeMS: at, JobID: job.JobID, Policy: config.Policy.Name(), Eligible: eligible, Selected: worker.ID})
		if config.Explain {
			_ = before
		}
		energyModel.Observe(worker.ID, worker.Capacity, float64(at)/1000)
	}
}

func finish(active *map[string]running, workers map[string]model.Worker, completed map[string]bool, at int64, result *Result, waits *[]int64, energyModel *energy.Model) {
	for id, job := range *active {
		if job.end > at {
			continue
		}
		worker := workers[job.worker]
		_ = worker.Capacity.Release(model.Job{CPU: job.job.CPU, MemoryMB: job.job.MemoryMB, GPU: job.job.GPU}.Resources())
		workers[job.worker] = worker
		energyModel.Observe(job.worker, worker.Capacity, float64(at)/1000)
		completed[id] = true
		result.Completed++
		*waits = append(*waits, job.start-job.job.TimeMS)
		delete(*active, id)
	}
}

func scaledCapacity(event Event, workerScale, gpuScale int) model.Capacity {
	total := model.ResourceRequest{CPU: event.CPU * workerScale, MemoryMB: event.MemoryMB * workerScale, GPU: event.GPU * gpuScale}
	return model.Capacity{Total: total, Available: total}
}
func jobEvents(trace Trace) []Event {
	result := []Event{}
	for _, e := range trace.Events {
		if e.Type == JobSubmitted {
			result = append(result, e)
		}
	}
	return result
}
func parseFailure(value string) (string, int64, bool) {
	parts := strings.Split(value, "@")
	if len(parts) != 2 || parts[0] == "" {
		return "", 0, false
	}
	raw := strings.TrimSuffix(parts[1], "s")
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds < 0 {
		return "", 0, false
	}
	return parts[0], int64(seconds * 1000), true
}
func setPercentiles(result *Result, values []int64) {
	if len(values) == 0 {
		return
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	result.MeanWaitMS = average(values)
	result.P50WaitMS = float64(values[percentileIndex(len(values), 0.50)])
	result.P95WaitMS = float64(values[percentileIndex(len(values), 0.95)])
	result.P99WaitMS = float64(values[percentileIndex(len(values), 0.99)])
}

func percentileIndex(count int, percentile float64) int {
	index := int(math.Ceil(percentile*float64(count))) - 1
	if index < 0 {
		return 0
	}
	if index >= count {
		return count - 1
	}
	return index
}
func average(values []int64) float64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return float64(total) / float64(len(values))
}
