package importer

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/MuhammadMaazA/Orbit/internal/replay"
)

type Stats struct {
	Rows int
}

func NormalizedCSV(r io.Reader) (replay.Trace, Stats, error) {
	reader := csv.NewReader(r)
	header, err := reader.Read()
	if err != nil {
		return replay.Trace{}, Stats{}, fmt.Errorf("import CSV header: %w", err)
	}
	columns := make(map[string]int, len(header))
	for index, column := range header {
		if column == "" {
			return replay.Trace{}, Stats{}, fmt.Errorf("import CSV column %d: empty name", index+1)
		}
		if _, exists := columns[column]; exists {
			return replay.Trace{}, Stats{}, fmt.Errorf("import CSV: duplicate column %q", column)
		}
		columns[column] = index
	}
	if _, ok := columns["time_ms"]; !ok {
		return replay.Trace{}, Stats{}, fmt.Errorf("import CSV: required column %q is missing", "time_ms")
	}
	if _, ok := columns["type"]; !ok {
		return replay.Trace{}, Stats{}, fmt.Errorf("import CSV: required column %q is missing", "type")
	}

	trace := replay.Trace{Version: replay.Version}
	stats := Stats{}
	for {
		row, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return replay.Trace{}, stats, fmt.Errorf("import CSV row %d: %w", stats.Rows+2, readErr)
		}
		stats.Rows++
		if len(row) != len(header) {
			return replay.Trace{}, stats, fmt.Errorf("import CSV row %d: got %d fields, want %d", stats.Rows+1, len(row), len(header))
		}
		event, err := parseEvent(row, columns, stats.Rows+1)
		if err != nil {
			return replay.Trace{}, stats, err
		}
		trace.Events = append(trace.Events, event)
	}
	if err := trace.Validate(); err != nil {
		return replay.Trace{}, stats, fmt.Errorf("import CSV validation: %w", err)
	}
	return trace, stats, nil
}

func parseEvent(row []string, columns map[string]int, line int) (replay.Event, error) {
	value := func(name string) string {
		if index, ok := columns[name]; ok {
			return row[index]
		}
		return ""
	}
	text := func(name string) (string, error) {
		result := value(name)
		if result == "" {
			return "", fmt.Errorf("import CSV row %d: %q is required", line, name)
		}
		return result, nil
	}
	integer := func(name string, required bool) (int64, error) {
		raw := value(name)
		if raw == "" && !required {
			return 0, nil
		}
		if raw == "" {
			return 0, fmt.Errorf("import CSV row %d: %q is required", line, name)
		}
		result, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("import CSV row %d: invalid %s %q", line, name, raw)
		}
		return result, nil
	}

	eventType, err := text("type")
	if err != nil {
		return replay.Event{}, err
	}
	timeMS, err := integer("time_ms", true)
	if err != nil {
		return replay.Event{}, err
	}
	event := replay.Event{Type: eventType, TimeMS: timeMS}
	event.WorkerID = value("worker_id")
	event.JobID = value("job_id")
	for name, destination := range map[string]*int{"cpu": &event.CPU, "memory_mb": &event.MemoryMB, "gpu": &event.GPU, "priority": &event.Priority} {
		parsed, parseErr := integer(name, false)
		if parseErr != nil {
			return replay.Event{}, parseErr
		}
		*destination = int(parsed)
	}
	event.DurationMS, err = integer("duration_ms", false)
	if err != nil {
		return replay.Event{}, err
	}
	return event, nil
}
