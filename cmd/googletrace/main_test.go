package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCSV(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTasksJoinsFullLifecycleWithinWindow(t *testing.T) {
	// job 1: submit in window, then schedule and finish - should be kept.
	// job 2: submit outside the window - excluded even though it has a full lifecycle.
	// job 3: submit in window but never scheduled - excluded (incomplete).
	path := writeCSV(t, `100,,1,0,,0,,3,9,0.5,0.25,,
150,,1,0,machine-a,1,,3,9,,,,
400,,1,0,,4,,3,9,,,,
50,,2,0,,0,,3,9,0.5,0.25,,
60,,2,0,machine-b,1,,3,9,,,,
90,,2,0,,4,,3,9,,,,
120,,3,0,,0,,3,9,0.5,0.25,,
`)
	tasks, err := loadTasks(path, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	job1 := tasks[taskKey{jobID: "1", index: "0"}]
	if job1 == nil || !job1.haveSchedule || !job1.haveTerminal || job1.scheduleUS != 150 || job1.terminalUS != 400 || job1.machineID != "machine-a" {
		t.Fatalf("job 1 = %+v", job1)
	}
	if _, ok := tasks[taskKey{jobID: "2", index: "0"}]; ok {
		t.Fatal("job 2 (submitted outside the window) should not be tracked")
	}
	job3 := tasks[taskKey{jobID: "3", index: "0"}]
	if job3 == nil || job3.haveSchedule || job3.haveTerminal {
		t.Fatalf("job 3 (never scheduled) should be incomplete, got %+v", job3)
	}
}

func TestLoadMachinesPicksLatestAddAtOrBeforeWindowStart(t *testing.T) {
	// machine-a: added at t=0, then re-added (e.g. after removal) at t=50 -
	// the later record should win, since it reflects capacity at windowStart.
	// machine-b: only added after windowStart - must be ignored.
	// machine-c: not in the wanted set - must be ignored even though valid.
	path := writeCSV(t, `0,a,0,,0.5,0.25
50,a,0,,0.75,0.5
0,a,1,,,
200,b,0,,1,1
0,c,0,,1,1
`)
	machines, err := loadMachines(path, map[string]bool{"a": true, "b": true}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 1 {
		t.Fatalf("machines = %+v, want only machine a", machines)
	}
	got := machines["a"]
	if got.cpuNorm != 0.75 || got.memNorm != 0.5 {
		t.Fatalf("machine a = %+v, want the t=50 record", got)
	}
}

func TestScaled(t *testing.T) {
	for _, tc := range []struct {
		norm, reference float64
		want            string
	}{
		{0.5, 64, "32"},
		{0.015625, 64, "1"},
		{0.001, 64, "0"},
		{0, 64, "0"},
	} {
		if got := scaled(tc.norm, tc.reference); got != tc.want {
			t.Fatalf("scaled(%v, %v) = %q, want %q", tc.norm, tc.reference, got, tc.want)
		}
	}
}
