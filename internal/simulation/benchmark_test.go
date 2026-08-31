package simulation

import (
	"testing"

	"github.com/MuhammadMaazA/Orbit/internal/model"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
)

func BenchmarkSimulation(b *testing.B) {
	resources := model.ResourceRequest{CPU: 32, MemoryMB: 65_536}
	workers := []Worker{{ID: "a", Capacity: model.Capacity{Total: resources, Available: resources}}, {ID: "b", Capacity: model.Capacity{Total: resources, Available: resources}}}
	jobs := Generate(42, 100)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Run(scheduler.FirstFit{}, workers, jobs)
	}
}
