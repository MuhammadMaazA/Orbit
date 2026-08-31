package main

import (
	"fmt"

	"orbit/internal/model"
	"orbit/internal/scheduler"
)

type Node struct {
	ID                string
	TotalCPU          int
	AvailableCPU      int
	TotalMemoryMB     int
	AvailableMemoryMB int
	TotalGPU          int
	AvailableGPU      int
}

type Job struct {
	ID       string
	CPU      int
	MemoryMB int
	GPU      int
}

func (n Node) CanFit(j Job) bool {
	return n.AvailableCPU >= j.CPU && n.AvailableMemoryMB >= j.MemoryMB && n.AvailableGPU >= j.GPU
}

func (n *Node) Allocate(j Job) error {
	if !n.CanFit(j) {
		return fmt.Errorf("node %s cannot fit job %s (needs %d CPU / %d MB, has %d CPU / %d MB available)",
			n.ID, j.ID, j.CPU, j.MemoryMB, n.AvailableCPU, n.AvailableMemoryMB)
	}
	resources := model.Resources{CPU: n.TotalCPU, MemoryMB: n.TotalMemoryMB, GPU: n.TotalGPU, AvailableCPU: n.AvailableCPU, AvailableMB: n.AvailableMemoryMB, AvailableGPU: n.AvailableGPU}
	if err := resources.Allocate(model.Job{CPU: j.CPU, MemoryMB: j.MemoryMB, GPU: j.GPU}.Resources()); err != nil {
		return fmt.Errorf("node %s cannot fit job %s: %w", n.ID, j.ID, err)
	}
	n.AvailableCPU, n.AvailableMemoryMB, n.AvailableGPU = resources.AvailableCPU, resources.AvailableMB, resources.AvailableGPU
	return nil
}

func FirstFit(nodes []Node, job Job) (int, bool) {
	workers := make([]model.Worker, len(nodes))
	for i, n := range nodes {
		workers[i] = model.Worker{ID: n.ID, Capacity: model.Resources{CPU: n.TotalCPU, MemoryMB: n.TotalMemoryMB, GPU: n.TotalGPU, AvailableCPU: n.AvailableCPU, AvailableMB: n.AvailableMemoryMB, AvailableGPU: n.AvailableGPU}}
	}
	return (scheduler.FirstFit{}).Select(workers, model.Job{CPU: job.CPU, MemoryMB: job.MemoryMB, GPU: job.GPU})
}

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
