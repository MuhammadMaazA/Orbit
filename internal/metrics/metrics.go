package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	JobsSubmitted prometheus.Counter
	JobsCompleted prometheus.Counter
	JobsFailed    prometheus.Counter
	Workers       prometheus.Gauge
	Queued        prometheus.Gauge
	Running       prometheus.Gauge
}

func (m *Metrics) SetGauges(workers, queued, running int) {
	m.Workers.Set(float64(workers))
	m.Queued.Set(float64(queued))
	m.Running.Set(float64(running))
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		JobsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_submitted_total", Help: "Jobs accepted by the controller."}),
		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_completed_total", Help: "Jobs completed successfully."}),
		JobsFailed:    prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_failed_total", Help: "Jobs that failed or exhausted retries."}),
		Workers:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_workers", Help: "Currently registered workers."}),
		Queued:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_jobs_queued", Help: "Currently queued jobs."}),
		Running:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_jobs_running", Help: "Currently running jobs."}),
	}
	for _, collector := range []prometheus.Collector{m.JobsSubmitted, m.JobsCompleted, m.JobsFailed, m.Workers, m.Queued, m.Running} {
		reg.MustRegister(collector)
	}
	return m
}
