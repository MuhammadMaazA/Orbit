package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"orbit/internal/model"
	"orbit/internal/scheduler"
	"orbit/internal/simulation"
)

func main() {
	seed := flag.Int64("seed", 42, "workload seed")
	count := flag.Int("jobs", 100, "job count")
	output := flag.String("output", "", "CSV output path")
	flag.Parse()
	resources := model.ResourceRequest{CPU: 16, MemoryMB: 16_384}
	workers := []simulation.Worker{{ID: "worker-a", Capacity: model.Capacity{Total: resources, Available: resources}}, {ID: "worker-b", Capacity: model.Capacity{Total: resources, Available: resources}}}
	jobs := simulation.Generate(*seed, *count)
	policies := []scheduler.Policy{scheduler.FirstFit{}, scheduler.BestFit{}, scheduler.BinPack{}}
	writer := os.Stdout
	if *output != "" {
		if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
			slog.Error("create output directory", "error", err)
			os.Exit(1)
		}
		file, err := os.Create(*output)
		if err != nil {
			slog.Error("create output", "error", err)
			os.Exit(1)
		}
		defer file.Close()
		writer = file
	}
	fmt.Fprintln(writer, "policy,jobs,completed,makespan,average_wait,average_turnaround")
	for _, policy := range policies {
		result, err := simulation.Run(policy, workers, jobs)
		if err != nil {
			slog.Error("simulate", "policy", policy.Name(), "error", err)
			os.Exit(1)
		}
		fmt.Fprintf(writer, "%s,%d,%d,%d,%.2f,%.2f\n", result.Policy, result.Jobs, result.Completed, result.Makespan, result.AverageWait, result.AverageTurnaround)
	}
}
