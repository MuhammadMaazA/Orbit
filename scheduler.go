package main

import "fmt"

// Node represents a machine with a fixed resource capacity. TotalCPU and
// TotalMemoryMB never change; AvailableCPU and AvailableMemoryMB track what
// is currently unused and shrink as jobs are allocated to the node.
type Node struct {
	ID                string
	TotalCPU          int
	AvailableCPU      int
	TotalMemoryMB     int
	AvailableMemoryMB int
}

// Job describes the resources a unit of work needs to run.
type Job struct {
	ID       string
	CPU      int
	MemoryMB int
}

// CanFit reports whether n currently has enough available CPU and memory
// to run j. It does not modify n.
func (n Node) CanFit(j Job) bool {
	return n.AvailableCPU >= j.CPU && n.AvailableMemoryMB >= j.MemoryMB
}

// Allocate reserves j's resources on n, decreasing the node's available
// CPU and memory. It uses a pointer receiver because it mutates n; with a
// value receiver, Allocate would only modify a copy and the caller would
// never see the change.
//
// If n cannot satisfy j, Allocate returns an error and leaves n untouched,
// so a rejected allocation can never leave the node in an inconsistent
// state (available resources negative, or out of sync with what's
// actually running).
func (n *Node) Allocate(j Job) error {
	if !n.CanFit(j) {
		return fmt.Errorf("node %s cannot fit job %s (needs %d CPU / %d MB, has %d CPU / %d MB available)",
			n.ID, j.ID, j.CPU, j.MemoryMB, n.AvailableCPU, n.AvailableMemoryMB)
	}
	n.AvailableCPU -= j.CPU
	n.AvailableMemoryMB -= j.MemoryMB
	return nil
}

// FirstFit scans nodes in order and returns the index of the first one
// that can fit job. It returns (-1, false) if none can. FirstFit only
// inspects nodes; it never allocates.
func FirstFit(nodes []Node, job Job) (int, bool) {
	for i, n := range nodes {
		if n.CanFit(job) {
			return i, true
		}
	}
	return -1, false
}

// Schedule finds the first node in nodes that can fit job, allocates the
// job's resources to it, and returns that node's ID.
//
// nodes is a slice, so its backing array is shared with the caller: even
// though nodes is passed by value here, nodes[i].Allocate(job) mutates the
// same underlying array the caller sees. That's what lets repeated calls
// to Schedule against the same slice reflect earlier allocations.
func Schedule(nodes []Node, job Job) (string, error) {
	i, ok := FirstFit(nodes, job)
	if !ok {
		return "", fmt.Errorf("no node available for job %s (needs %d CPU, %d MB memory)", job.ID, job.CPU, job.MemoryMB)
	}
	if err := nodes[i].Allocate(job); err != nil {
		return "", err
	}
	return nodes[i].ID, nil
}
