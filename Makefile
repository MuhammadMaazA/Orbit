GO ?= go
BUF ?= buf

.PHONY: build test vet race generate simulate benchmark benchmark-policy benchmark-replay demo

build:
	$(GO) build ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

race:
	CGO_ENABLED=1 $(GO) test -race ./...

generate:
	$(BUF) generate

simulate:
	$(GO) run ./cmd/simulator -seed 42 -jobs 100

benchmark-policy:
	$(GO) run ./cmd/simulator -seed 42 -jobs 1000 -output artifacts/benchmarks/policy_results.csv
	$(GO) run ./cmd/simulator -seed 84 -jobs 10000 -output artifacts/benchmarks/policy_results_10000.csv
	$(GO) test -run '^$$' -bench 'BenchmarkPolicies|BenchmarkSimulation' -benchmem ./internal/scheduler ./internal/simulation

benchmark-replay:
	$(GO) run ./cmd/replaybench

benchmark: benchmark-policy benchmark-replay

demo:
	powershell -ExecutionPolicy Bypass -File scripts/demo.ps1
