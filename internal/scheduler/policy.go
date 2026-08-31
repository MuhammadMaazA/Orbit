package scheduler

import (
	"fmt"

	"orbit/internal/model"
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

func lessRemaining(a, b model.Capacity, request model.ResourceRequest) bool {
	acpu, amem, agpu := a.Available.CPU-request.CPU, a.Available.MemoryMB-request.MemoryMB, a.Available.GPU-request.GPU
	bcpu, bmem, bgpu := b.Available.CPU-request.CPU, b.Available.MemoryMB-request.MemoryMB, b.Available.GPU-request.GPU
	if acpu != bcpu {
		return acpu < bcpu
	}
	if amem != bmem {
		return amem < bmem
	}
	return agpu < bgpu
}

func moreUtilized(a, b model.Capacity) bool {
	aUsed, aTotal := (a.Total.CPU-a.Available.CPU)+(a.Total.MemoryMB-a.Available.MemoryMB)+(a.Total.GPU-a.Available.GPU), a.Total.CPU+a.Total.MemoryMB+a.Total.GPU
	bUsed, bTotal := (b.Total.CPU-b.Available.CPU)+(b.Total.MemoryMB-b.Available.MemoryMB)+(b.Total.GPU-b.Available.GPU), b.Total.CPU+b.Total.MemoryMB+b.Total.GPU
	if aTotal == 0 || bTotal == 0 {
		return aTotal > bTotal
	}
	return aUsed*bTotal > bUsed*aTotal
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
