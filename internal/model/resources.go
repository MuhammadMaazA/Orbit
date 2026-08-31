package model

import "fmt"

// Resources describes schedulable capacity. Available values are mutable
// accounting fields; total values are the worker's fixed capacity.
type Resources struct {
	CPU          int
	MemoryMB     int
	GPU          int
	AvailableCPU int
	AvailableMB  int
	AvailableGPU int
}

func (r Resources) Valid() error {
	if r.CPU < 0 || r.MemoryMB < 0 || r.GPU < 0 {
		return fmt.Errorf("total resources must be non-negative")
	}
	if r.AvailableCPU < 0 || r.AvailableMB < 0 || r.AvailableGPU < 0 {
		return fmt.Errorf("available resources must be non-negative")
	}
	if r.AvailableCPU > r.CPU || r.AvailableMB > r.MemoryMB || r.AvailableGPU > r.GPU {
		return fmt.Errorf("available resources cannot exceed total capacity")
	}
	return nil
}

func (r Resources) CanFit(request Resources) bool {
	return r.AvailableCPU >= request.CPU && r.AvailableMB >= request.MemoryMB && r.AvailableGPU >= request.GPU
}

func (r *Resources) Allocate(request Resources) error {
	if !r.CanFit(request) {
		return fmt.Errorf("insufficient resources")
	}
	r.AvailableCPU -= request.CPU
	r.AvailableMB -= request.MemoryMB
	r.AvailableGPU -= request.GPU
	return nil
}

func (r *Resources) Release(request Resources) error {
	r.AvailableCPU += request.CPU
	r.AvailableMB += request.MemoryMB
	r.AvailableGPU += request.GPU
	if err := r.Valid(); err != nil {
		return fmt.Errorf("release resources: %w", err)
	}
	return nil
}
