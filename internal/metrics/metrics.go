package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	JobsSubmitted        prometheus.Counter
	JobsCompleted        prometheus.Counter
	JobsFailed           prometheus.Counter
	JobsRejected         prometheus.Counter
	JobsRequeued         prometheus.Counter
	WorkersRegistered    prometheus.Counter
	SchedulingAttempts   prometheus.Counter
	StaleResultsRejected prometheus.Counter
	Workers              prometheus.Gauge
	Draining             prometheus.Gauge
	Queued               prometheus.Gauge
	Running              prometheus.Gauge
}

func (m *Metrics) SetGauges(workers, draining, queued, running int) {
	m.Workers.Set(float64(workers))
	m.Draining.Set(float64(draining))
	m.Queued.Set(float64(queued))
	m.Running.Set(float64(running))
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		JobsSubmitted:        prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_submitted_total", Help: "Jobs accepted by the controller."}),
		JobsCompleted:        prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_completed_total", Help: "Jobs completed successfully."}),
		JobsFailed:           prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_failed_total", Help: "Jobs that failed or exhausted retries."}),
		JobsRejected:         prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_rejected_total", Help: "Jobs rejected by admission control."}),
		JobsRequeued:         prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_jobs_requeued_total", Help: "Jobs returned to the queue after worker loss."}),
		WorkersRegistered:    prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_workers_registered_total", Help: "Worker registrations accepted."}),
		SchedulingAttempts:   prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_scheduling_attempts_total", Help: "Assignments created by the scheduler."}),
		StaleResultsRejected: prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_stale_results_rejected_total", Help: "Completion results rejected because their assignment is stale."}),
		Workers:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_workers", Help: "Currently registered workers."}),
		Draining:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_workers_draining", Help: "Currently draining workers."}),
		Queued:               prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_jobs_queued", Help: "Currently queued jobs."}),
		Running:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "orbit_jobs_running", Help: "Currently running jobs."}),
	}
	for _, collector := range []prometheus.Collector{m.JobsSubmitted, m.JobsCompleted, m.JobsFailed, m.JobsRejected, m.JobsRequeued, m.WorkersRegistered, m.SchedulingAttempts, m.StaleResultsRejected, m.Workers, m.Draining, m.Queued, m.Running} {
		reg.MustRegister(collector)
	}
	return m
}
