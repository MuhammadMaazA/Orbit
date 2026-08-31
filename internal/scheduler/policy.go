package scheduler

import (
	"fmt"

	"github.com/MuhammadMaazA/Orbit/internal/model"
)

type Policy interface {
	Name() string
	Select([]model.Worker, model.Job) (int, bool)
}

type FirstFit struct{}

func (FirstFit) Name() string { return "first-fit" }

func (FirstFit) Select(workers []model.Worker, job model.Job) (int, bool) {
	for i, worker := range workers {
		if worker.Capacity.CanFit(job.Resources()) {
			return i, true
		}
	}
	return -1, false
}

type BestFit struct{}

func (BestFit) Name() string { return "best-fit" }

func (BestFit) Select(workers []model.Worker, job model.Job) (int, bool) {
	best := -1
	for i, worker := range workers {
		if !worker.Capacity.CanFit(job.Resources()) {
			continue
		}
		if best == -1 || lessRemaining(worker.Capacity, workers[best].Capacity, job.Resources()) {
			best = i
		}
	}
	return best, best >= 0
}

type BinPack struct{}

func (BinPack) Name() string { return "bin-pack" }

func (BinPack) Select(workers []model.Worker, job model.Job) (int, bool) {
	best := -1
	for i, worker := range workers {
		if !worker.Capacity.CanFit(job.Resources()) {
			continue
		}
		if best == -1 || moreUtilized(worker.Capacity, workers[best].Capacity) {
			best = i
		}
	}
	return best, best >= 0
}

type EnergyAware struct{}

func (EnergyAware) Name() string { return "energy" }

func (EnergyAware) Select(workers []model.Worker, job model.Job) (int, bool) {
	best := -1
	for i, worker := range workers {
		if !worker.Capacity.CanFit(job.Resources()) || !isActive(worker.Capacity) {
			continue
		}
		if best == -1 || moreUtilized(worker.Capacity, workers[best].Capacity) {
			best = i
		}
	}
	if best >= 0 {
		return best, true
	}
	return (BinPack{}).Select(workers, job)
}

func isActive(capacity model.Capacity) bool {
	return capacity.Available.CPU < capacity.Total.CPU || capacity.Available.MemoryMB < capacity.Total.MemoryMB || capacity.Available.GPU < capacity.Total.GPU
}

// lessRemaining reports whether placing the job on a would leave a tighter
// fit than placing it on b: a smaller fractional headroom in whichever
// resource (CPU, memory, or GPU) is most constrained afterwards. Comparing
// fractions of each worker's own capacity - rather than summing raw
// CPU+MB+GPU units - keeps memory (typically thousands of MB) from
// dominating the score just because its numbers are bigger.
func lessRemaining(a, b model.Capacity, request model.ResourceRequest) bool {
	return dominantUtilizationAfter(a, request) > dominantUtilizationAfter(b, request)
}

func moreUtilized(a, b model.Capacity) bool {
	return dominantUtilization(a) > dominantUtilization(b)
}

func dominantUtilizationAfter(capacity model.Capacity, request model.ResourceRequest) float64 {
	after := capacity
	after.Available = model.ResourceRequest{
		CPU:      capacity.Available.CPU - request.CPU,
		MemoryMB: capacity.Available.MemoryMB - request.MemoryMB,
		GPU:      capacity.Available.GPU - request.GPU,
	}
	return dominantUtilization(after)
}

func dominantUtilization(capacity model.Capacity) float64 {
	dominant := 0.0
	if v := utilization(capacity.Total.CPU, capacity.Available.CPU); v > dominant {
		dominant = v
	}
	if v := utilization(capacity.Total.MemoryMB, capacity.Available.MemoryMB); v > dominant {
		dominant = v
	}
	if v := utilization(capacity.Total.GPU, capacity.Available.GPU); v > dominant {
		dominant = v
	}
	return dominant
}

func utilization(total, available int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(total-available) / float64(total)
}

func Schedule(workers []model.Worker, job model.Job, policy Policy) (string, error) {
	if policy == nil {
		return "", fmt.Errorf("schedule job %q: nil policy", job.ID)
	}
	i, ok := policy.Select(workers, job)
	if !ok {
		return "", fmt.Errorf("no worker available for job %s", job.ID)
	}
	if err := workers[i].Capacity.Allocate(job.Resources()); err != nil {
		return "", fmt.Errorf("allocate job %q on worker %q: %w", job.ID, workers[i].ID, err)
	}
	return workers[i].ID, nil
}
