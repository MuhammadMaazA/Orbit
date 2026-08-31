package model

import "testing"

func TestResourcesAllocateAndRelease(t *testing.T) {
	r := Capacity{Total: ResourceRequest{CPU: 8, MemoryMB: 16_384, GPU: 2}, Available: ResourceRequest{CPU: 8, MemoryMB: 16_384, GPU: 2}}
	request := ResourceRequest{CPU: 4, MemoryMB: 4_096, GPU: 1}
	if err := r.Allocate(request); err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if want := (Capacity{Total: ResourceRequest{CPU: 8, MemoryMB: 16_384, GPU: 2}, Available: ResourceRequest{CPU: 4, MemoryMB: 12_288, GPU: 1}}); r != want {
		t.Fatalf("after Allocate() = %+v, want %+v", r, want)
	}
	if err := r.Release(request); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if r.Available.CPU != 8 || r.Available.MemoryMB != 16_384 || r.Available.GPU != 2 {
		t.Fatalf("after Release() available = %+v", r)
	}
}

func TestResourcesRejectedAllocationDoesNotMutate(t *testing.T) {
	r := Capacity{Total: ResourceRequest{CPU: 2, MemoryMB: 1_024}, Available: ResourceRequest{CPU: 2, MemoryMB: 1_024}}
	want := r
	if err := r.Allocate(ResourceRequest{CPU: 3}); err == nil {
		t.Fatal("Allocate() succeeded with insufficient CPU")
	}
	if r != want {
		t.Fatalf("rejected Allocate() mutated resources: got %+v, want %+v", r, want)
	}
}

func TestResourcesRejectedReleaseDoesNotMutate(t *testing.T) {
	r := Capacity{Total: ResourceRequest{CPU: 2, MemoryMB: 1_024}, Available: ResourceRequest{CPU: 2, MemoryMB: 1_024}}
	want := r
	if err := r.Release(ResourceRequest{CPU: 1}); err == nil {
		t.Fatal("Release() succeeded when it would exceed total CPU capacity")
	}
	if r != want {
		t.Fatalf("rejected Release() mutated resources: got %+v, want %+v", r, want)
	}
}
