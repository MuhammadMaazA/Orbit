package scheduler

import (
	"testing"

	"orbit/internal/model"
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
