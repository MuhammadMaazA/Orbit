package model

import "testing"

func TestResourcesAllocateAndRelease(t *testing.T) {
	r := Resources{CPU: 8, MemoryMB: 16_384, GPU: 2, AvailableCPU: 8, AvailableMB: 16_384, AvailableGPU: 2}
	request := Resources{CPU: 4, MemoryMB: 4_096, GPU: 1}
	if err := r.Allocate(request); err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if want := (Resources{CPU: 8, MemoryMB: 16_384, GPU: 2, AvailableCPU: 4, AvailableMB: 12_288, AvailableGPU: 1}); r != want {
		t.Fatalf("after Allocate() = %+v, want %+v", r, want)
	}
	if err := r.Release(request); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if r.AvailableCPU != 8 || r.AvailableMB != 16_384 || r.AvailableGPU != 2 {
		t.Fatalf("after Release() available = %+v", r)
	}
}

func TestResourcesRejectedAllocationDoesNotMutate(t *testing.T) {
	r := Resources{CPU: 2, MemoryMB: 1_024, AvailableCPU: 2, AvailableMB: 1_024}
	want := r
	if err := r.Allocate(Resources{CPU: 3}); err == nil {
		t.Fatal("Allocate() succeeded with insufficient CPU")
	}
	if r != want {
		t.Fatalf("rejected Allocate() mutated resources: got %+v, want %+v", r, want)
	}
}
