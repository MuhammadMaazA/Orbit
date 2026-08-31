package scheduler

import (
	"testing"

	"orbit/internal/model"
)

func BenchmarkPolicies(b *testing.B) {
	workers := make([]model.Worker, 32)
	capacity := model.ResourceRequest{CPU: 32, MemoryMB: 65_536, GPU: 4}
	for i := range workers {
		workers[i] = model.Worker{ID: string(rune('a' + i)), Capacity: model.Capacity{Total: capacity, Available: capacity}}
	}
	job := model.Job{ID: "bench", CPU: 4, MemoryMB: 1_024, GPU: 1}
	for _, policy := range []Policy{FirstFit{}, BestFit{}, BinPack{}} {
		b.Run(policy.Name(), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				policy.Select(workers, job)
			}
		})
	}
}
