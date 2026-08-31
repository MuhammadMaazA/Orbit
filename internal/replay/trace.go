package replay

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/MuhammadMaazA/Orbit/internal/model"
)

const Version = 1

const (
	WorkerAdded     = "worker_added"
	WorkerRemoved   = "worker_removed"
	WorkerFailed    = "worker_failed"
	WorkerRecovered = "worker_recovered"
	JobSubmitted    = "job_submitted"
)

type Trace struct {
	Version int     `json:"version"`
	Events  []Event `json:"events"`
}

type Event struct {
	TimeMS     int64  `json:"time_ms"`
	Type       string `json:"type"`
	WorkerID   string `json:"worker_id,omitempty"`
	JobID      string `json:"job_id,omitempty"`
	CPU        int    `json:"cpu,omitempty"`
	MemoryMB   int    `json:"memory_mb,omitempty"`
	GPU        int    `json:"gpu,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Priority   int    `json:"priority,omitempty"`
}

func Load(r io.Reader) (Trace, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var trace Trace
	if err := decoder.Decode(&trace); err != nil {
		return Trace{}, fmt.Errorf("decode trace: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Trace{}, fmt.Errorf("trace contains multiple JSON values")
		}
		return Trace{}, fmt.Errorf("decode trace: %w", err)
	}
	if err := trace.Validate(); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func (t Trace) Validate() error {
	if t.Version != Version {
		return fmt.Errorf("trace: unsupported version %d", t.Version)
	}
	last := int64(0)
	workers := make(map[string]bool)
	jobs := make(map[string]bool)
	for i, event := range t.Events {
		if event.TimeMS < 0 {
			return fmt.Errorf("trace event %d: negative timestamp", i)
		}
		if event.TimeMS < last {
			return fmt.Errorf("trace event %d: timestamp is not monotonic", i)
		}
		last = event.TimeMS
		switch event.Type {
		case WorkerAdded, WorkerRecovered:
			if event.WorkerID == "" {
				return fmt.Errorf("trace event %d: worker ID is required", i)
			}
			if event.CPU < 0 || event.MemoryMB < 0 || event.GPU < 0 {
				return fmt.Errorf("trace event %d: negative worker resources", i)
			}
			capacity := model.Capacity{Total: model.ResourceRequest{CPU: event.CPU, MemoryMB: event.MemoryMB, GPU: event.GPU}, Available: model.ResourceRequest{CPU: event.CPU, MemoryMB: event.MemoryMB, GPU: event.GPU}}
			if err := capacity.Valid(); err != nil {
				return fmt.Errorf("trace event %d: %w", i, err)
			}
			if workers[event.WorkerID] {
				return fmt.Errorf("trace event %d: worker %q is already active", i, event.WorkerID)
			}
			workers[event.WorkerID] = true
		case WorkerRemoved, WorkerFailed:
			if event.WorkerID == "" || !workers[event.WorkerID] {
				return fmt.Errorf("trace event %d: worker %q is not active", i, event.WorkerID)
			}
			workers[event.WorkerID] = false
		case JobSubmitted:
			if event.JobID == "" || jobs[event.JobID] {
				return fmt.Errorf("trace event %d: duplicate or empty job ID", i)
			}
			if event.CPU < 0 || event.MemoryMB < 0 || event.GPU < 0 || event.DurationMS <= 0 {
				return fmt.Errorf("trace event %d: invalid job", i)
			}
			jobs[event.JobID] = true
		default:
			return fmt.Errorf("trace event %d: unknown type %q", i, event.Type)
		}
	}
	return nil
}
