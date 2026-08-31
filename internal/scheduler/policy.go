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

// BestFit chooses the eligible worker with the smallest remaining CPU after
// placement, then memory and GPU, making ties stable by slice order.
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

// BinPack places work on the worker with the highest current utilization,
// packing jobs tightly and preserving larger workers for later jobs.
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

func lessRemaining(a, b, request model.Resources) bool {
	acpu, amem, agpu := a.AvailableCPU-request.CPU, a.AvailableMB-request.MemoryMB, a.AvailableGPU-request.GPU
	bcpu, bmem, bgpu := b.AvailableCPU-request.CPU, b.AvailableMB-request.MemoryMB, b.AvailableGPU-request.GPU
	if acpu != bcpu {
		return acpu < bcpu
	}
	if amem != bmem {
		return amem < bmem
	}
	return agpu < bgpu
}

func moreUtilized(a, b model.Resources) bool {
	// Cross multiplication avoids floating-point tie-breaking. A zero-sized
	// worker is never preferred over a non-zero worker.
	aUsed, aTotal := (a.CPU-a.AvailableCPU)+(a.MemoryMB-a.AvailableMB)+(a.GPU-a.AvailableGPU), a.CPU+a.MemoryMB+a.GPU
	bUsed, bTotal := (b.CPU-b.AvailableCPU)+(b.MemoryMB-b.AvailableMB)+(b.GPU-b.AvailableGPU), b.CPU+b.MemoryMB+b.GPU
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
