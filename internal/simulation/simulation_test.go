package simulation

import (
	"testing"

	"orbit/internal/model"
	"orbit/internal/scheduler"
)

func TestRunReleasesCapacityAndCompletesQueuedJobs(t *testing.T) {
	capacity := model.ResourceRequest{CPU: 2, MemoryMB: 1_024}
	result, err := Run(scheduler.FirstFit{}, []Worker{{ID: "w1", Capacity: model.Capacity{Total: capacity, Available: capacity}}}, []Job{
		{Spec: model.Job{ID: "j1", CPU: 2, MemoryMB: 512}, Arrival: 0, Duration: 3},
		{Spec: model.Job{ID: "j2", CPU: 2, MemoryMB: 512}, Arrival: 0, Duration: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 2 || result.Makespan != 5 || result.AverageWait != 1.5 || result.AverageTurnaround != 4 {
		t.Fatalf("result = %+v", result)
	}
}
