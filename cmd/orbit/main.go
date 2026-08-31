package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/MuhammadMaazA/Orbit/internal/energy"
	"github.com/MuhammadMaazA/Orbit/internal/importer"
	"github.com/MuhammadMaazA/Orbit/internal/replay"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const version = "v0.1.0"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	if os.Args[1] == "version" {
		fmt.Println("orbit", version)
		return
	}
	if os.Args[1] == "inspect" {
		runInspect(os.Args[2:])
		return
	}
	if os.Args[1] == "import" {
		runImport(os.Args[2:])
		return
	}
	if os.Args[1] == "demo" {
		runDemo()
		return
	}
	if os.Args[1] != "submit" && os.Args[1] != "status" && os.Args[1] != "drain" && os.Args[1] != "undrain" && os.Args[1] != "replay" && os.Args[1] != "compare" {
		usage()
	}
	if os.Args[1] == "replay" || os.Args[1] == "compare" {
		runReplayCommand(os.Args[1], os.Args[2:])
		return
	}
	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	address := flags.String("controller", "127.0.0.1:9000", "controller address")
	id := flags.String("id", "", "job ID")
	if os.Args[1] == "drain" || os.Args[1] == "undrain" {
		_ = flags.Parse(os.Args[2:])
		if *id == "" {
			slog.Error("worker ID is required")
			os.Exit(2)
		}
		connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("connect controller", "error", err)
			os.Exit(1)
		}
		defer connection.Close()
		client := v1.NewOrbitControllerClient(connection)
		var response *v1.WorkerStateResponse
		if os.Args[1] == "drain" {
			response, err = client.DrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: *id})
		} else {
			response, err = client.UndrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: *id})
		}
		if err != nil {
			slog.Error("change worker state", "error", err)
			os.Exit(1)
		}
		fmt.Printf("%s draining=%t\n", *id, response.Draining)
		return
	}
	if os.Args[1] == "status" {
		_ = flags.Parse(os.Args[2:])
		connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("connect controller", "error", err)
			os.Exit(1)
		}
		defer connection.Close()
		client := v1.NewOrbitControllerClient(connection)
		if *id == "" {
			jobs, listErr := client.ListJobs(context.Background(), &v1.JobListRequest{})
			if listErr != nil {
				slog.Error("list jobs", "error", listErr)
				os.Exit(1)
			}
			for _, job := range jobs.Jobs {
				fmt.Printf("%s status=%s attempt=%d worker=%s assignment=%s\n", job.Job.Id, job.Status, job.Attempt, job.WorkerId, job.AssignmentId)
			}
			return
		}
		jobStatus, statusErr := client.GetJob(context.Background(), &v1.JobStatusRequest{JobId: *id})
		if statusErr != nil {
			slog.Error("get job", "error", statusErr)
			os.Exit(1)
		}
		fmt.Printf("%s status=%s attempt=%d worker=%s assignment=%s\n", jobStatus.Job.Id, jobStatus.Status, jobStatus.Attempt, jobStatus.WorkerId, jobStatus.AssignmentId)
		return
	}
	cpu := flags.Int("cpu", 1, "CPU request")
	memory := flags.Int("memory-mb", 1, "memory request in MB")
	gpu := flags.Int("gpu", 0, "GPU request")
	priority := flags.Int("priority", 1, "job priority")
	_ = flags.Parse(os.Args[2:])
	if *id == "" {
		slog.Error("job ID is required")
		os.Exit(2)
	}
	connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("connect controller", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	assignment, err := v1.NewOrbitControllerClient(connection).Submit(context.Background(), &v1.Job{Id: *id, Resources: &v1.ResourceRequest{Cpu: int32(*cpu), MemoryMb: int32(*memory), Gpu: int32(*gpu)}, Priority: int32(*priority)})
	if err != nil {
		slog.Error("submit job", "error", err)
		os.Exit(1)
	}
	if assignment.Id == "" {
		fmt.Println("queued")
	} else {
		fmt.Printf("assigned %s to %s\n", assignment.Id, assignment.WorkerId)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: orbit version|inspect|import|demo|submit|status|drain|undrain|replay|compare [flags]")
	os.Exit(2)
}

func runInspect(arguments []string) {
	flags := flag.NewFlagSet("inspect", flag.ExitOnError)
	tracePath := flags.String("trace", "", "versioned trace JSON path")
	flags.Parse(arguments)
	trace, err := loadTrace(*tracePath)
	if err != nil {
		slog.Error("inspect trace", "error", err)
		os.Exit(1)
	}
	counts := make(map[string]int)
	for _, event := range trace.Events {
		counts[event.Type]++
	}
	fmt.Printf("version=%d events=%d worker_added=%d jobs=%d failures=%d\n", trace.Version, len(trace.Events), counts[replay.WorkerAdded], counts[replay.JobSubmitted], counts[replay.WorkerFailed]+counts[replay.WorkerRemoved])
}

func runImport(arguments []string) {
	flags := flag.NewFlagSet("import", flag.ExitOnError)
	outputPath := flags.String("output", "", "output trace JSON path")
	flags.Parse(arguments)
	if *outputPath == "" || flags.NArg() != 2 || flags.Arg(0) != "normalized" {
		slog.Error("usage: orbit import --output trace.json normalized input.csv")
		os.Exit(2)
	}
	input, err := os.Open(flags.Arg(1))
	if err != nil {
		slog.Error("open CSV", "error", err)
		os.Exit(1)
	}
	defer input.Close()
	trace, stats, err := importer.NormalizedCSV(input)
	if err != nil {
		slog.Error("import CSV", "error", err)
		os.Exit(1)
	}
	output, err := os.Create(*outputPath)
	if err != nil {
		slog.Error("create trace", "error", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(trace)
	closeErr := output.Close()
	if err != nil || closeErr != nil {
		if err == nil {
			err = closeErr
		}
		slog.Error("write trace", "error", err)
		os.Exit(1)
	}
	fmt.Printf("imported_rows=%d events=%d output=%s\n", stats.Rows, len(trace.Events), *outputPath)
}

func runDemo() {
	fmt.Println("policy comparison")
	runReplayCommand("compare", []string{"-trace", "traces/heterogeneous.json", "-baseline", "first-fit", "-candidate", "energy"})
	fmt.Println("failure replay")
	runReplayCommand("replay", []string{"-trace", "traces/failure-heavy.json", "-policy", "best-fit", "-explain"})
}

func loadTrace(path string) (replay.Trace, error) {
	if path == "" {
		return replay.Trace{}, fmt.Errorf("trace is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return replay.Trace{}, fmt.Errorf("open trace: %w", err)
	}
	defer file.Close()
	return replay.Load(file)
}

func runReplayCommand(command string, arguments []string) {
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	tracePath := flags.String("trace", "", "versioned trace JSON path")
	policyName := flags.String("policy", "first-fit", "replay policy")
	baselineName := flags.String("baseline", "first-fit", "comparison baseline policy")
	candidateName := flags.String("candidate", "energy", "comparison candidate policy")
	failure := flags.String("inject-failure", "", "worker-id@seconds")
	workerScale := flags.Int("worker-scale", 1, "capacity scale for replay workers")
	gpuScale := flags.Int("gpu-scale", 1, "GPU capacity scale for replay workers")
	powerIdle := flags.Float64("power-idle", 100, "idle worker power in watts")
	powerCPU := flags.Float64("power-cpu", 10, "power per allocated CPU in watts")
	powerGPU := flags.Float64("power-gpu", 50, "power per allocated GPU in watts")
	explain := flags.Bool("explain", false, "include scheduling decisions")
	flags.Parse(arguments)
	if *tracePath == "" {
		slog.Error("trace is required")
		os.Exit(2)
	}
	file, err := os.Open(*tracePath)
	if err != nil {
		slog.Error("open trace", "error", err)
		os.Exit(1)
	}
	defer file.Close()
	trace, err := replay.Load(file)
	if err != nil {
		slog.Error("load trace", "error", err)
		os.Exit(1)
	}
	if command == "compare" {
		printComparison(trace, *baselineName, *candidateName, replay.Config{WorkerScale: *workerScale, GPUScale: *gpuScale, Power: energy.Config{IdleWatts: *powerIdle, CPUWatts: *powerCPU, GPUWatts: *powerGPU}, Explain: *explain})
		return
	}
	policy, err := replayPolicy(*policyName)
	if err != nil {
		slog.Error("policy", "error", err)
		os.Exit(2)
	}
	result, err := replay.Run(trace, replay.Config{Policy: policy, WorkerScale: *workerScale, GPUScale: *gpuScale, Power: energy.Config{IdleWatts: *powerIdle, CPUWatts: *powerCPU, GPUWatts: *powerGPU}, InjectFailure: *failure, Explain: *explain})
	if err != nil {
		slog.Error("replay", "error", err)
		os.Exit(1)
	}
	fmt.Printf("policy=%s jobs=%d completed=%d makespan=%dms p95_wait=%.0fms energy=%.2fJ active_worker_time=%.2fs retries=%d failures=%d\n", result.Policy, result.Jobs, result.Completed, result.MakespanMS, result.P95WaitMS, result.EnergyJoules, result.ActiveWorkerTime, result.Retries, result.Failures)
	for _, decision := range result.Decisions {
		if *explain {
			fmt.Printf("t=%dms job=%s policy=%s selected=%s\n", decision.TimeMS, decision.JobID, decision.Policy, decision.Selected)
		}
	}
}

func printComparison(trace replay.Trace, baselineName, candidateName string, base replay.Config) {
	baseline, err := replayPolicy(baselineName)
	if err != nil {
		slog.Error("baseline policy", "error", err)
		os.Exit(2)
	}
	candidate, err := replayPolicy(candidateName)
	if err != nil {
		slog.Error("candidate policy", "error", err)
		os.Exit(2)
	}
	base.Policy = baseline
	candidateConfig := base
	candidateConfig.Policy = candidate
	left, err := replay.Run(trace, base)
	if err != nil {
		slog.Error("baseline replay", "error", err)
		os.Exit(1)
	}
	right, err := replay.Run(trace, candidateConfig)
	if err != nil {
		slog.Error("candidate replay", "error", err)
		os.Exit(1)
	}
	fmt.Println("metric                  baseline       candidate       delta")
	fmt.Printf("completed               %-14d %-14d %+d\n", left.Completed, right.Completed, right.Completed-left.Completed)
	fmt.Printf("makespan_ms             %-14d %-14d %+d\n", left.MakespanMS, right.MakespanMS, right.MakespanMS-left.MakespanMS)
	fmt.Printf("p95_wait_ms             %-14.2f %-14.2f %+0.2f\n", left.P95WaitMS, right.P95WaitMS, right.P95WaitMS-left.P95WaitMS)
	fmt.Printf("energy_joules           %-14.2f %-14.2f %+0.2f\n", left.EnergyJoules, right.EnergyJoules, right.EnergyJoules-left.EnergyJoules)
	fmt.Printf("active_worker_time_s    %-14.2f %-14.2f %+0.2f\n", left.ActiveWorkerTime, right.ActiveWorkerTime, right.ActiveWorkerTime-left.ActiveWorkerTime)
	for index := 0; index < len(left.Decisions) && index < len(right.Decisions); index++ {
		baselineDecision, candidateDecision := left.Decisions[index], right.Decisions[index]
		if baselineDecision.JobID != candidateDecision.JobID || baselineDecision.Selected != candidateDecision.Selected {
			fmt.Printf("\nFIRST DIVERGENCE - job=%s (t=%.1fs)\n\n", baselineDecision.JobID, float64(baselineDecision.TimeMS)/1000)
			printDecision(baselineName, baselineDecision)
			fmt.Println()
			printDecision(candidateName, candidateDecision)
			return
		}
	}
}

// printDecision explains how a policy arrived at its choice for one job:
// the worker it picked, plus (for capacity-driven policies) how every
// candidate worker was evaluated and why the loser(s) were passed over.
func printDecision(policyName string, decision replay.Decision) {
	fmt.Printf("%s -> %s\n", policyName, decision.Selected)
	if policyName == "first-fit" {
		fmt.Println("  first feasible worker")
		return
	}
	for _, candidate := range decision.Candidates {
		if candidate.Feasible {
			fmt.Printf("  %s: feasible, residual=cpu:%d mem:%d gpu:%d\n", candidate.WorkerID, candidate.Residual.CPU, candidate.Residual.MemoryMB, candidate.Residual.GPU)
		} else {
			fmt.Printf("  %s: rejected, %s\n", candidate.WorkerID, candidate.Reason)
		}
	}
	fmt.Printf("%s selected %s because %s\n", policyName, decision.Selected, explainSelection(policyName, decision))
}

func explainSelection(policyName string, decision replay.Decision) string {
	switch policyName {
	case "best-fit":
		return "it left the least residual capacity among feasible workers (tightest fit)"
	case "bin-pack":
		return "it was the most-utilized feasible worker, consolidating load onto fewer machines"
	case "energy":
		for _, candidate := range decision.Candidates {
			if candidate.WorkerID != decision.Selected {
				continue
			}
			if candidate.Active {
				return "it was the most-utilized already-active feasible worker, avoiding powering on an idle one"
			}
			return "no active worker had capacity for this job, so it fell back to the most-utilized feasible worker overall"
		}
	}
	return "it best matched the policy's selection criteria"
}

func replayPolicy(name string) (scheduler.Policy, error) {
	switch name {
	case "first-fit":
		return scheduler.FirstFit{}, nil
	case "best-fit":
		return scheduler.BestFit{}, nil
	case "bin-pack":
		return scheduler.BinPack{}, nil
	case "energy":
		return scheduler.EnergyAware{}, nil
	default:
		return nil, fmt.Errorf("unsupported policy %q", name)
	}
}
