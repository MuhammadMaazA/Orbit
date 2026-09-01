# Orbit

Orbit is a distributed cluster scheduler and deterministic workload replay engine written in Go.

It schedules heterogeneous CPU, memory, and GPU jobs over gRPC, detects worker loss, retries interrupted work, and fences stale completions. The replay engine runs the same versioned trace under different policies so scheduling choices can be compared without changing the workload.

## Install

```text
go install github.com/MuhammadMaazA/Orbit/cmd/orbit@latest
```

Or download a prebuilt binary from the [releases page](https://github.com/MuhammadMaazA/Orbit/releases).

```text
orbit demo
```

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

## Real workload

`traces/google-cluster-2011.json` is a 30-second slice of Google's real
`clusterdata-2011-2` Borg trace ([CC-BY](https://creativecommons.org/licenses/by/4.0/),
[github.com/google/cluster-data](https://github.com/google/cluster-data)).
32 machines, 32 jobs, unmodified from the trace.
`cmd/googletrace` converts Google's raw `task_events`/`machine_events` CSVs
into Orbit's normalized format.

```text
go run ./cmd/googletrace -task-events task_events.csv -machine-events machine_events.csv \
  -output traces/google-cluster-2011.normalized.csv
orbit import --output traces/google-cluster-2011.json normalized traces/google-cluster-2011.normalized.csv
orbit compare -trace traces/google-cluster-2011.json -baseline first-fit -candidate best-fit
```

Metric deltas are `+0` here. 32 machines easily cover 32 light jobs, so
nothing queues. Placement still diverges (`FIRST DIVERGENCE`) and explains
why. Capping the same jobs to one worker (`-max-workers 1`) makes them
queue for real. p95 wait goes from 0ms to 37683ms.

## Policies

- `first-fit` selects the first feasible worker.
- `best-fit` picks the feasible worker that would be left with the least headroom in its most-loaded resource (CPU, memory, or GPU, compared as a fraction of that worker's own capacity, not raw units).
- `bin-pack` prefers the feasible worker whose most-loaded resource is already closest to full, by the same per-resource fraction.
- `energy` prefers an already active worker by that measure, and otherwise falls back to bin-packing.

Queued jobs have priorities with ageing. Workers can be drained for maintenance, and admission control can cap queued work.

## Failure and recovery

Workers register with session IDs and send heartbeats. Lost workers cause active jobs to be retried. Completion is accepted only for the current assignment, worker session, and attempt. The controller can persist state in a JSONL event log and atomic snapshot; workers must register again after restart.

This gives at-least-once execution: a single controller is the source of truth for assignment state, so a lost worker's jobs always get retried elsewhere rather than silently dropped.

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
internal/importer/   trace importers for external workload formats
traces/              versioned replay workloads
artifacts/           generated benchmark results
```

Orbit is a single binary and a single controller by design: clone it, `go build`, and run a scheduler and a deterministic replay engine with nothing else to stand up first.

## License

[MIT](LICENSE)
