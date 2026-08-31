package model

import "fmt"

// Resources describes schedulable capacity. Available values are mutable
// accounting fields; total values are the worker's fixed capacity.
type ResourceRequest struct {
	CPU      int
	MemoryMB int
	GPU      int
}

type Capacity struct {
	Total     ResourceRequest
	Available ResourceRequest
}

func (c Capacity) Valid() error {
	if c.Total.CPU < 0 || c.Total.MemoryMB < 0 || c.Total.GPU < 0 {
		return fmt.Errorf("total resources must be non-negative")
	}
	if c.Available.CPU < 0 || c.Available.MemoryMB < 0 || c.Available.GPU < 0 {
		return fmt.Errorf("available resources must be non-negative")
	}
	if c.Available.CPU > c.Total.CPU || c.Available.MemoryMB > c.Total.MemoryMB || c.Available.GPU > c.Total.GPU {
		return fmt.Errorf("available resources cannot exceed total capacity")
	}
	return nil
}

func (c Capacity) CanFit(request ResourceRequest) bool {
	return c.Available.CPU >= request.CPU && c.Available.MemoryMB >= request.MemoryMB && c.Available.GPU >= request.GPU
}

func (c *Capacity) Allocate(request ResourceRequest) error {
	if !c.CanFit(request) {
		return fmt.Errorf("insufficient resources")
	}
	c.Available.CPU -= request.CPU
	c.Available.MemoryMB -= request.MemoryMB
	c.Available.GPU -= request.GPU
	return nil
}

func (c *Capacity) Release(request ResourceRequest) error {
	after := ResourceRequest{
		CPU:      c.Available.CPU + request.CPU,
		MemoryMB: c.Available.MemoryMB + request.MemoryMB,
		GPU:      c.Available.GPU + request.GPU,
	}
	if after.CPU > c.Total.CPU || after.MemoryMB > c.Total.MemoryMB || after.GPU > c.Total.GPU {
		return fmt.Errorf("release resources: exceeds total capacity")
	}
	c.Available = after
	return nil
}
