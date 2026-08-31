package replay

import (
	"strings"
	"testing"

	"orbit/internal/scheduler"
)

const testTrace = `{"version":1,"events":[{"time_ms":0,"type":"worker_added","worker_id":"cpu-a","cpu":4,"memory_mb":4096},{"time_ms":0,"type":"worker_added","worker_id":"cpu-b","cpu":4,"memory_mb":4096},{"time_ms":1,"type":"job_submitted","job_id":"job-1","cpu":2,"memory_mb":512,"duration_ms":10},{"time_ms":2,"type":"job_submitted","job_id":"job-2","cpu":2,"memory_mb":512,"duration_ms":10}]}`

func TestReplayIsDeterministic(t *testing.T) {
	trace, err := Load(strings.NewReader(testTrace))
	if err != nil {
		t.Fatal(err)
	}
	one, err := Run(trace, Config{Policy: scheduler.EnergyAware{}})
	if err != nil {
		t.Fatal(err)
	}
	two, err := Run(trace, Config{Policy: scheduler.EnergyAware{}})
	if err != nil {
		t.Fatal(err)
	}
	if one.MakespanMS != two.MakespanMS || one.Completed != two.Completed || one.EnergyJoules != two.EnergyJoules {
		t.Fatalf("replay results differ: %+v vs %+v", one, two)
	}
}

func TestTraceRejectsInvalidInput(t *testing.T) {
	_, err := Load(strings.NewReader(`{"version":1,"events":[{"time_ms":1,"type":"unknown"}]}`))
	if err == nil {
		t.Fatal("Load accepted an unknown event")
	}
}

func TestReplayInjectedFailureRetriesWork(t *testing.T) {
	trace, err := Load(strings.NewReader(`{"version":1,"events":[{"time_ms":0,"type":"worker_added","worker_id":"worker-a","cpu":4,"memory_mb":4096},{"time_ms":0,"type":"worker_added","worker_id":"worker-b","cpu":4,"memory_mb":4096},{"time_ms":1,"type":"job_submitted","job_id":"job-1","cpu":2,"memory_mb":512,"duration_ms":100}]}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Run(trace, Config{Policy: scheduler.FirstFit{}, InjectFailure: "worker-a@0.050s"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 1 || result.Retries != 1 || result.Failures != 1 {
		t.Fatalf("result = %+v", result)
	}
}
