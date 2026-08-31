package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/MuhammadMaazA/Orbit/internal/energy"
	"github.com/MuhammadMaazA/Orbit/internal/replay"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
)

func main() {
	traces := []string{"bursty", "gpu-heavy", "fragmentation-heavy", "failure-heavy"}
	policies := []scheduler.Policy{scheduler.FirstFit{}, scheduler.BestFit{}, scheduler.BinPack{}, scheduler.EnergyAware{}}
	if err := os.MkdirAll("artifacts/benchmarks", 0o755); err != nil {
		panic(err)
	}
	file, err := os.Create("artifacts/benchmarks/replay_results.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fmt.Fprintln(file, "trace,policy,jobs,completed,makespan_ms,mean_wait_ms,p50_wait_ms,p95_wait_ms,p99_wait_ms,retries,failures,energy_joules,active_worker_time_s")
	for _, name := range traces {
		traceFile, err := os.Open(filepath.Join("traces", name+".json"))
		if err != nil {
			panic(err)
		}
		trace, err := replay.Load(traceFile)
		traceFile.Close()
		if err != nil {
			panic(err)
		}
		for _, policy := range policies {
			result, err := replay.Run(trace, replay.Config{Policy: policy, Power: energy.Config{IdleWatts: 100, CPUWatts: 10, GPUWatts: 50}})
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(file, "%s,%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%d,%d,%.2f,%.2f\n", name, result.Policy, result.Jobs, result.Completed, result.MakespanMS, result.MeanWaitMS, result.P50WaitMS, result.P95WaitMS, result.P99WaitMS, result.Retries, result.Failures, result.EnergyJoules, result.ActiveWorkerTime)
		}
	}
}
