// googletrace converts a slice of Google's public clusterdata-2011-2
// task_events/machine_events tables (https://github.com/google/cluster-data,
// CC-BY) into Orbit's normalized importer CSV. See docs/real-workload.md for
// the full reproduction steps and column reference.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
)

const (
	taskEventSubmit   = "0"
	taskEventSchedule = "1"
	machineEventAdd   = "0"
)

var terminalTaskEvents = map[string]bool{"3": true, "4": true, "5": true, "6": true} // FAIL, FINISH, KILL, LOST

type taskKey struct {
	jobID string
	index string
}

type task struct {
	submitUS         int64
	scheduleUS       int64
	terminalUS       int64
	haveSchedule     bool
	haveTerminal     bool
	machineID        string
	priority         int64
	cpuNorm, memNorm float64
}

type machine struct {
	addUS            int64
	cpuNorm, memNorm float64
}

func main() {
	taskEventsPath := flag.String("task-events", "", "path to Google task_events CSV")
	machineEventsPath := flag.String("machine-events", "", "path to Google machine_events CSV")
	output := flag.String("output", "", "path to write Orbit normalized CSV")
	windowStart := flag.Int64("window-start-us", 600_000_000, "trace-relative start of the submit window, in microseconds")
	windowEnd := flag.Int64("window-end-us", 630_000_000, "trace-relative end of the submit window, in microseconds (exclusive)")
	refCPU := flag.Float64("reference-cpu", 64, "assumed CPU core count for a normalized capacity of 1.0")
	refMemMB := flag.Float64("reference-memory-mb", 131072, "assumed memory in MB for a normalized capacity of 1.0")
	maxWorkers := flag.Int("max-workers", 0, "cap the number of synthesized workers (0 = no cap); Orbit picks its own placement regardless of the task's original machine, so this creates real contention instead of one worker per referenced machine")
	flag.Parse()
	if *taskEventsPath == "" || *machineEventsPath == "" || *output == "" {
		slog.Error("usage: googletrace -task-events file -machine-events file -output normalized.csv")
		os.Exit(2)
	}

	tasks, err := loadTasks(*taskEventsPath, *windowStart, *windowEnd)
	if err != nil {
		slog.Error("load task events", "error", err)
		os.Exit(1)
	}
	complete := make(map[taskKey]*task, len(tasks))
	usedMachines := make(map[string]bool)
	for key, t := range tasks {
		if !t.haveSchedule || !t.haveTerminal || t.terminalUS <= t.scheduleUS {
			continue
		}
		complete[key] = t
		usedMachines[t.machineID] = true
	}

	machines, err := loadMachines(*machineEventsPath, usedMachines, *windowStart)
	if err != nil {
		slog.Error("load machine events", "error", err)
		os.Exit(1)
	}

	out, err := os.Create(*output)
	if err != nil {
		slog.Error("create output", "error", err)
		os.Exit(1)
	}
	defer out.Close()
	writer := csv.NewWriter(out)
	defer writer.Flush()
	if err := writer.Write([]string{"time_ms", "type", "worker_id", "cpu", "memory_mb", "gpu", "job_id", "duration_ms", "priority"}); err != nil {
		slog.Error("write header", "error", err)
		os.Exit(1)
	}

	machineIDs := make([]string, 0, len(machines))
	for id := range machines {
		machineIDs = append(machineIDs, id)
	}
	sort.Strings(machineIDs)
	if *maxWorkers > 0 && len(machineIDs) > *maxWorkers {
		machineIDs = machineIDs[:*maxWorkers]
	}
	for _, id := range machineIDs {
		m := machines[id]
		row := []string{"0", "worker_added", "machine-" + id, scaled(m.cpuNorm, *refCPU), scaled(m.memNorm, *refMemMB), "0", "", "", ""}
		if err := writer.Write(row); err != nil {
			slog.Error("write worker row", "error", err)
			os.Exit(1)
		}
	}

	type ordered struct {
		key taskKey
		t   *task
	}
	rows := make([]ordered, 0, len(complete))
	for key, t := range complete {
		rows = append(rows, ordered{key, t})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].t.submitUS != rows[j].t.submitUS {
			return rows[i].t.submitUS < rows[j].t.submitUS
		}
		if rows[i].key.jobID != rows[j].key.jobID {
			return rows[i].key.jobID < rows[j].key.jobID
		}
		return rows[i].key.index < rows[j].key.index
	})
	for _, r := range rows {
		t := r.t
		timeMS := (t.submitUS - *windowStart) / 1000
		durationMS := (t.terminalUS - t.scheduleUS) / 1000
		if durationMS < 1 {
			durationMS = 1
		}
		row := []string{
			strconv.FormatInt(timeMS, 10), "job_submitted", "",
			scaled(t.cpuNorm, *refCPU), scaled(t.memNorm, *refMemMB), "0",
			fmt.Sprintf("task-%s-%s", r.key.jobID, r.key.index),
			strconv.FormatInt(durationMS, 10),
			strconv.FormatInt(t.priority, 10),
		}
		if err := writer.Write(row); err != nil {
			slog.Error("write job row", "error", err)
			os.Exit(1)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		slog.Error("flush output", "error", err)
		os.Exit(1)
	}
	fmt.Printf("workers=%d jobs=%d window=[%d,%d)us reference_cpu=%.0f reference_memory_mb=%.0f output=%s\n", len(machineIDs), len(rows), *windowStart, *windowEnd, *refCPU, *refMemMB, *output)
}

func scaled(norm, reference float64) string {
	value := int64(math.Round(norm * reference))
	if value < 0 {
		value = 0
	}
	return strconv.FormatInt(value, 10)
}

func loadTasks(path string, windowStart, windowEnd int64) (map[taskKey]*task, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	tasks := make(map[taskKey]*task)
	wanted := make(map[taskKey]bool)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) < 9 {
			continue
		}
		eventTime, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			continue
		}
		key := taskKey{jobID: row[2], index: row[3]}
		eventType := row[5]
		switch {
		case eventType == taskEventSubmit && eventTime >= windowStart && eventTime < windowEnd:
			if len(row) < 11 || row[9] == "" || row[10] == "" {
				continue
			}
			cpuNorm, err := strconv.ParseFloat(row[9], 64)
			if err != nil {
				continue
			}
			memNorm, err := strconv.ParseFloat(row[10], 64)
			if err != nil {
				continue
			}
			priority, err := strconv.ParseInt(row[8], 10, 64)
			if err != nil {
				continue
			}
			wanted[key] = true
			tasks[key] = &task{submitUS: eventTime, cpuNorm: cpuNorm, memNorm: memNorm, priority: priority}
		case eventType == taskEventSchedule && wanted[key]:
			t := tasks[key]
			if t != nil && !t.haveSchedule && eventTime >= t.submitUS {
				t.haveSchedule = true
				t.scheduleUS = eventTime
				t.machineID = row[4]
			}
		case terminalTaskEvents[eventType] && wanted[key]:
			t := tasks[key]
			if t != nil && t.haveSchedule && !t.haveTerminal && eventTime >= t.scheduleUS {
				t.haveTerminal = true
				t.terminalUS = eventTime
			}
		}
	}
	return tasks, nil
}

func loadMachines(path string, wanted map[string]bool, windowStart int64) (map[string]machine, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	machines := make(map[string]machine)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) < 6 || row[2] != machineEventAdd {
			continue
		}
		id := row[1]
		if !wanted[id] {
			continue
		}
		eventTime, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil || eventTime > windowStart {
			continue
		}
		if row[4] == "" || row[5] == "" {
			continue
		}
		cpuNorm, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			continue
		}
		memNorm, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			continue
		}
		if existing, ok := machines[id]; !ok || eventTime > existing.addUS {
			machines[id] = machine{addUS: eventTime, cpuNorm: cpuNorm, memNorm: memNorm}
		}
	}
	return machines, nil
}
