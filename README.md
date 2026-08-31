# Orbit

Orbit is a distributed cluster scheduler and deterministic workload replay engine written in Go.

It schedules heterogeneous CPU, memory, and GPU jobs over gRPC, detects worker loss, retries interrupted work, and fences stale completions. The replay engine runs the same versioned trace under different policies so scheduling choices can be compared without changing the workload.

## Run it

```text
go build ./...
go test ./...
make demo
```

Start a controller and worker in separate terminals:

```text
go run ./cmd/controller -policy energy
go run ./cmd/worker -controller 127.0.0.1:9000 -id worker-a
go run ./cmd/orbit submit -id job-1 -cpu 2 -memory-mb 1024
go run ./cmd/orbit status -id job-1
```

## Replay

Traces are versioned JSON files in `traces/`. They describe worker capacity, job arrivals, worker failures, and worker recovery. Replay uses a logical clock and deterministic worker ordering.

```text
go run ./cmd/orbit replay -trace traces/heterogeneous.json -policy energy
go run ./cmd/orbit compare -trace traces/fragmentation-heavy.json -baseline first-fit -candidate best-fit
go run ./cmd/orbit replay -trace traces/failure-heavy.json -policy energy -inject-failure worker-a@0.05s
```

The comparison reports completion, makespan, wait percentiles, modelled energy, active-worker time, and the first assignment divergence. The energy calculation is an analytical model, not hardware telemetry.

## Policies

- `first-fit` selects the first feasible worker.
- `best-fit` minimises remaining capacity after placement.
- `bin-pack` prefers the most utilised feasible worker.
- `energy` prefers an already active worker and otherwise uses bin-packing.

Queued jobs have priorities with ageing. Workers can be drained for maintenance, and admission control can cap queued work.

## Failure and recovery

Workers register with session IDs and send heartbeats. Lost workers cause active jobs to be retried. Completion is accepted only for the current assignment, worker session, and attempt. The controller can persist state in a JSONL event log and atomic snapshot; workers must register again after restart.

Orbit provides at-least-once-style execution. It does not provide exactly-once execution, consensus, or controller high availability.

## Benchmarks

```text
make benchmark
```

The generated results in `artifacts/benchmarks/` cover deterministic simulator workloads and replay traces. The fragmentation trace demonstrates a real placement trade-off: best-fit reaches 210 ms makespan and 100 ms p95 wait, while first-fit, bin-pack, and energy reach 110 ms and 0 ms p95 wait on the same three-job trace. These are simulated results, not live-cluster measurements.

## Layout

```text
api/                 protobuf schema
cmd/controller/      controller process
cmd/worker/          worker process
cmd/orbit/           live client and replay CLI
internal/controller/ live scheduling state
internal/replay/     deterministic trace replay
internal/scheduler/ policy implementations
internal/simulation/ discrete workload simulator
internal/storage/    persistence
internal/energy/     modelled power accounting
traces/              versioned replay workloads
artifacts/           generated benchmark results
```

Orbit is intentionally a single-controller project. It has no Kubernetes integration, external database, cloud deployment, frontend, or hardware power telemetry.
