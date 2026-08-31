package model

import "testing"

func FuzzResourceAllocationPreservesInvariants(f *testing.F) {
	f.Add(8, 1_024, 1, 2, 512, 0)
	f.Add(1, 256, 0, 1, 128, 0)
	f.Fuzz(func(t *testing.T, totalCPU, totalMemory, totalGPU, requestCPU, requestMemory, requestGPU int) {
		if totalCPU < 0 || totalMemory < 0 || totalGPU < 0 || requestCPU < 0 || requestMemory < 0 || requestGPU < 0 {
			t.Skip()
		}
		capacity := Capacity{Total: ResourceRequest{CPU: totalCPU, MemoryMB: totalMemory, GPU: totalGPU}, Available: ResourceRequest{CPU: totalCPU, MemoryMB: totalMemory, GPU: totalGPU}}
		request := ResourceRequest{CPU: requestCPU, MemoryMB: requestMemory, GPU: requestGPU}
		if !capacity.CanFit(request) {
			return
		}
		if err := capacity.Allocate(request); err != nil {
			t.Fatal(err)
		}
		if err := capacity.Valid(); err != nil {
			t.Fatal(err)
		}
		if err := capacity.Release(request); err != nil {
			t.Fatal(err)
		}
		if capacity.Available != capacity.Total {
			t.Fatalf("capacity after release = %+v, total = %+v", capacity.Available, capacity.Total)
		}
	})
}
