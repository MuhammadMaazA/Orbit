package main

import "fmt"

func main() {
	nodes := []Node{
		{ID: "node-a", TotalCPU: 8, AvailableCPU: 8, TotalMemoryMB: 16384, AvailableMemoryMB: 16384},
		{ID: "node-b", TotalCPU: 16, AvailableCPU: 16, TotalMemoryMB: 32768, AvailableMemoryMB: 32768},
		{ID: "node-c", TotalCPU: 4, AvailableCPU: 4, TotalMemoryMB: 8192, AvailableMemoryMB: 8192},
	}

	jobs := []Job{
		{ID: "job-1", CPU: 4, MemoryMB: 8192},
		{ID: "job-2", CPU: 12, MemoryMB: 16384},
		{ID: "job-3", CPU: 8, MemoryMB: 8192},
	}

	for _, job := range jobs {
		fmt.Printf("Scheduling %s (cpu=%d memory=%dMB)\n", job.ID, job.CPU, job.MemoryMB)
		nodeID, err := Schedule(nodes, job)
		if err != nil {
			fmt.Println("-> no suitable node")
			continue
		}
		fmt.Printf("-> assigned to %s\n", nodeID)
	}

	fmt.Println("\nCluster state:")
	for _, n := range nodes {
		fmt.Printf("%s: cpu %d/%d available, memory %d/%d MB available\n",
			n.ID, n.AvailableCPU, n.TotalCPU, n.AvailableMemoryMB, n.TotalMemoryMB)
	}
}
