GO ?= go
BUF ?= buf

.PHONY: build test vet race generate simulate benchmark-policy demo

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
	$(GO) run ./cmd/simulator -seed 42 -jobs 100 -output artifacts/benchmarks/policy_results.csv
	$(GO) test -run '^$$' -bench 'BenchmarkPolicies|BenchmarkSimulation' -benchmem ./internal/scheduler ./internal/simulation

demo:
	powershell -ExecutionPolicy Bypass -File scripts/demo.ps1
