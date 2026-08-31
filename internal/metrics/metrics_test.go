package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewRegistersOrbitMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := New(registry)
	metrics.JobsSubmitted.Inc()
	metrics.SetGauges(2, 1, 3, 4)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"orbit_jobs_submitted_total":         false,
		"orbit_jobs_rejected_total":          false,
		"orbit_jobs_requeued_total":          false,
		"orbit_workers_registered_total":     false,
		"orbit_scheduling_attempts_total":    false,
		"orbit_stale_results_rejected_total": false,
		"orbit_workers":                      false,
		"orbit_workers_draining":             false,
		"orbit_jobs_queued":                  false,
		"orbit_jobs_running":                 false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing metric %q", name)
		}
	}
}
