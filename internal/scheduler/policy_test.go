package scheduler

import (
	"testing"

	"github.com/MuhammadMaazA/Orbit/internal/model"
)

func TestFirstFitIsDeterministicAndGPUAware(t *testing.T) {
	workers := []model.Worker{
		{ID: "cpu-only", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 8, MemoryMB: 8_192}, Available: model.ResourceRequest{CPU: 8, MemoryMB: 8_192}}},
		{ID: "gpu-worker", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 8, MemoryMB: 8_192, GPU: 1}, Available: model.ResourceRequest{CPU: 8, MemoryMB: 8_192, GPU: 1}}},
	}
	idx, ok := (FirstFit{}).Select(workers, model.Job{ID: "gpu-job", CPU: 1, GPU: 1})
	if !ok || idx != 1 {
		t.Fatalf("Select() = %d, %t; want 1, true", idx, ok)
	}
}

func TestPoliciesChooseDeterministically(t *testing.T) {
	workers := []model.Worker{
		{ID: "small", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 8, MemoryMB: 8_192}, Available: model.ResourceRequest{CPU: 4, MemoryMB: 4_096}}},
		{ID: "large", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 16, MemoryMB: 16_384}, Available: model.ResourceRequest{CPU: 12, MemoryMB: 12_288}}},
	}
	job := model.Job{CPU: 4, MemoryMB: 4_096}
	for _, tc := range []struct {
		name   string
		policy Policy
		want   int
	}{
		{"best-fit", BestFit{}, 0},
		{"bin-pack", BinPack{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.policy.Select(workers, job)
			if !ok || got != tc.want {
				t.Fatalf("Select() = %d, %t; want %d, true", got, ok, tc.want)
			}
		})
	}
}

func TestBinPackUsesFractionalUtilizationNotRawUnits(t *testing.T) {
	// worker-a: CPU 90% used, memory 20% used, GPU 100% used - GPU-bound.
	// worker-b: CPU 40% used, memory 80% used, GPU 0% used - memory-bound.
	// Summing raw CPU+MB+GPU units picks worker-b, because memory's large
	// numbers swamp the score even though worker-a is maxed out on GPU.
	// Fractional (dominant-resource) utilization must pick worker-a: its
	// bottleneck resource (GPU) is more saturated than worker-b's (memory).
	workers := []model.Worker{
		{ID: "worker-a", Capacity: model.Capacity{
			Total:     model.ResourceRequest{CPU: 10, MemoryMB: 1_000_000, GPU: 1},
			Available: model.ResourceRequest{CPU: 1, MemoryMB: 800_000, GPU: 0},
		}},
		{ID: "worker-b", Capacity: model.Capacity{
			Total:     model.ResourceRequest{CPU: 10, MemoryMB: 1_000_000, GPU: 1},
			Available: model.ResourceRequest{CPU: 6, MemoryMB: 200_000, GPU: 1},
		}},
	}
	idx, ok := (BinPack{}).Select(workers, model.Job{})
	if !ok || idx != 0 {
		t.Fatalf("Select() = %d, %t; want 0 (worker-a), true", idx, ok)
	}
}

func TestBestFitUsesFractionalResidualNotLexicographicCPU(t *testing.T) {
	// worker-a is CPU-heavy and memory-idle; placing the job leaves it 75%
	// used on its bottleneck resource (memory). worker-b is CPU-light and
	// memory-huge; placing the job leaves it only 50% used on its
	// bottleneck (CPU), with memory barely touched. A CPU-first
	// lexicographic comparison would pick worker-b purely because its raw
	// leftover CPU count is smaller, ignoring that worker-a is proportionally
	// the tighter fit. Best-fit must pick worker-a.
	workers := []model.Worker{
		{ID: "worker-a", Capacity: model.Capacity{
			Total:     model.ResourceRequest{CPU: 1_000, MemoryMB: 200},
			Available: model.ResourceRequest{CPU: 500, MemoryMB: 150},
		}},
		{ID: "worker-b", Capacity: model.Capacity{
			Total:     model.ResourceRequest{CPU: 10, MemoryMB: 100_000},
			Available: model.ResourceRequest{CPU: 6, MemoryMB: 99_900},
		}},
	}
	job := model.Job{CPU: 1, MemoryMB: 100}
	idx, ok := (BestFit{}).Select(workers, job)
	if !ok || idx != 0 {
		t.Fatalf("Select() = %d, %t; want 0 (worker-a), true", idx, ok)
	}
}

func TestEnergyAwarePrefersActiveWorker(t *testing.T) {
	workers := []model.Worker{
		{ID: "active", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 8}, Available: model.ResourceRequest{CPU: 4}}},
		{ID: "idle", Capacity: model.Capacity{Total: model.ResourceRequest{CPU: 8}, Available: model.ResourceRequest{CPU: 8}}},
	}
	idx, ok := (EnergyAware{}).Select(workers, model.Job{CPU: 2})
	if !ok || idx != 0 {
		t.Fatalf("Select() = %d, %t; want 0, true", idx, ok)
	}
}
