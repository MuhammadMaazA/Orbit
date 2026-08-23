package main

import "testing"

// A job fits on an eligible node and is not placed on an ineligible one.
func TestScheduleSuccessfulPlacement(t *testing.T) {
	nodes := []Node{
		{ID: "a", TotalCPU: 4, AvailableCPU: 4, TotalMemoryMB: 8192, AvailableMemoryMB: 8192},
		{ID: "b", TotalCPU: 16, AvailableCPU: 16, TotalMemoryMB: 32768, AvailableMemoryMB: 32768},
	}
	job := Job{ID: "job-2", CPU: 12, MemoryMB: 16384}

	got, err := Schedule(nodes, job)
	if err != nil {
		t.Fatalf("Schedule() returned error for a job that should fit: %v", err)
	}
	if got != "b" {
		t.Errorf("Schedule() assigned job to %q, want %q", got, "b")
	}

	if nodes[0].AvailableCPU != 4 || nodes[0].AvailableMemoryMB != 8192 {
		t.Errorf("node a was mutated even though it was not selected: cpu=%d memory=%d",
			nodes[0].AvailableCPU, nodes[0].AvailableMemoryMB)
	}
	if nodes[1].AvailableCPU != 4 || nodes[1].AvailableMemoryMB != 16384 {
		t.Errorf("node b available = %d CPU / %d MB, want 4 CPU / 16384 MB",
			nodes[1].AvailableCPU, nodes[1].AvailableMemoryMB)
	}
}

// A node must reject a job it cannot satisfy.
func TestNodeAllocateInsufficientResources(t *testing.T) {
	n := Node{ID: "c", TotalCPU: 4, AvailableCPU: 4, TotalMemoryMB: 8192, AvailableMemoryMB: 8192}
	job := Job{ID: "job-big", CPU: 8, MemoryMB: 8192}

	if err := n.Allocate(job); err == nil {
		t.Fatal("Allocate() succeeded for a job that should not fit")
	}
}

// Scheduling must fail cleanly when no node has enough resources.
func TestScheduleNoSuitableNode(t *testing.T) {
	nodes := []Node{
		{ID: "a", TotalCPU: 4, AvailableCPU: 4, TotalMemoryMB: 8192, AvailableMemoryMB: 8192},
	}
	job := Job{ID: "job-huge", CPU: 100, MemoryMB: 100000}

	if _, err := Schedule(nodes, job); err == nil {
		t.Fatal("Schedule() succeeded for a job no node can satisfy")
	}
}

// Two jobs on the same node must consume resources cumulatively, so a
// later job can be rejected because of an earlier allocation.
func TestScheduleSequentialAllocation(t *testing.T) {
	nodes := []Node{
		{ID: "a", TotalCPU: 8, AvailableCPU: 8, TotalMemoryMB: 16384, AvailableMemoryMB: 16384},
	}

	first := Job{ID: "job-1", CPU: 6, MemoryMB: 12288}
	if _, err := Schedule(nodes, first); err != nil {
		t.Fatalf("Schedule() failed on first job: %v", err)
	}
	if nodes[0].AvailableCPU != 2 || nodes[0].AvailableMemoryMB != 4096 {
		t.Fatalf("after first job, available = %d CPU / %d MB, want 2 CPU / 4096 MB",
			nodes[0].AvailableCPU, nodes[0].AvailableMemoryMB)
	}

	second := Job{ID: "job-2", CPU: 4, MemoryMB: 2048}
	if _, err := Schedule(nodes, second); err == nil {
		t.Fatal("Schedule() succeeded for a job that should no longer fit after the first allocation")
	}
}

// A job requiring exactly the available resources must be accepted and
// leave the node at zero available CPU and memory.
func TestNodeAllocateExactBoundary(t *testing.T) {
	n := Node{ID: "a", TotalCPU: 8, AvailableCPU: 4, TotalMemoryMB: 16384, AvailableMemoryMB: 4096}
	job := Job{ID: "job-fill", CPU: 4, MemoryMB: 4096}

	if err := n.Allocate(job); err != nil {
		t.Fatalf("Allocate() returned error for a job requiring exactly the available resources: %v", err)
	}
	if n.AvailableCPU != 0 || n.AvailableMemoryMB != 0 {
		t.Errorf("AvailableCPU/AvailableMemoryMB = %d/%d, want 0/0", n.AvailableCPU, n.AvailableMemoryMB)
	}
}

// A failed allocation must not change the node's available resources.
func TestNodeAllocateFailureLeavesStateUnchanged(t *testing.T) {
	n := Node{ID: "c", TotalCPU: 4, AvailableCPU: 4, TotalMemoryMB: 8192, AvailableMemoryMB: 8192}
	job := Job{ID: "job-big", CPU: 8, MemoryMB: 8192}

	_ = n.Allocate(job)

	if n.AvailableCPU != 4 || n.AvailableMemoryMB != 8192 {
		t.Errorf("failed allocation changed node state: got cpu=%d memory=%d, want cpu=4 memory=8192",
			n.AvailableCPU, n.AvailableMemoryMB)
	}
}
