package importer

import (
	"strings"
	"testing"
)

func TestNormalizedCSV(t *testing.T) {
	trace, stats, err := NormalizedCSV(strings.NewReader("time_ms,type,worker_id,cpu,memory_mb,gpu,job_id,duration_ms,priority\n0,worker_added,w1,4,4096,0,,,\n1,job_submitted,,,2,512,job-1,10,3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 2 || len(trace.Events) != 2 || trace.Events[1].JobID != "job-1" {
		t.Fatalf("stats=%+v trace=%+v", stats, trace)
	}
}

func TestNormalizedCSVRejectsInvalidRows(t *testing.T) {
	_, _, err := NormalizedCSV(strings.NewReader("time_ms,type\n0,worker_added\n-1,job_submitted\n"))
	if err == nil {
		t.Fatal("expected invalid CSV to be rejected")
	}
}
