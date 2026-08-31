package energy

import (
	"testing"

	"orbit/internal/model"
)

func TestModelAccountsIdleAndDynamicPower(t *testing.T) {
	capacity := model.Capacity{Total: model.ResourceRequest{CPU: 4}, Available: model.ResourceRequest{CPU: 2}}
	m := New(Config{IdleWatts: 100, CPUWatts: 10})
	m.Register("worker-a", capacity, 0)
	m.Observe("worker-a", model.Capacity{Total: capacity.Total, Available: model.ResourceRequest{CPU: 4}}, 5)
	snapshot := m.Snapshot(10)
	if snapshot.Joules != 1100 {
		t.Fatalf("Joules = %v, want 1100", snapshot.Joules)
	}
	if snapshot.ActiveWorkerTime != 5 {
		t.Fatalf("ActiveWorkerTime = %v, want 5", snapshot.ActiveWorkerTime)
	}
}

func TestModelNeverReportsNegativeEnergy(t *testing.T) {
	m := New(Config{IdleWatts: 100})
	m.Register("worker-a", model.Capacity{Total: model.ResourceRequest{CPU: 1}, Available: model.ResourceRequest{CPU: 1}}, 10)
	if got := m.Snapshot(5).Joules; got != 0 {
		t.Fatalf("Joules = %v, want 0", got)
	}
}
