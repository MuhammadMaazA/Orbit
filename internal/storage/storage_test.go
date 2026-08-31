package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreAppendsAndRecoversEvents(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Unix(10, 0).UTC()
	if err := store.Append(Event{Type: "job_submitted", Timestamp: timestamp, Data: json.RawMessage(`{"id":"job-1"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(Event{Type: "job_assigned", Timestamp: timestamp, Data: json.RawMessage(`{"id":"job-1"}`)}); err != nil {
		t.Fatal(err)
	}
	_, events, err := store.Recover()
	if err != nil || len(events) != 2 || events[0].Type != "job_submitted" || events[1].Type != "job_assigned" {
		t.Fatalf("Recover() = %+v, %v", events, err)
	}
}

func TestFileStoreSnapshotReplacesAtomically(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Snapshot([]byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Snapshot([]byte(`{"version":2}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.Recover()
	if err != nil || string(snapshot) != `{"version":2}` {
		t.Fatalf("snapshot = %q, %v", snapshot, err)
	}
	if matches, _ := filepath.Glob(filepath.Join(directory, "snapshot-*.tmp")); len(matches) != 0 {
		t.Fatalf("temporary snapshots remain: %v", matches)
	}
}

func TestFileStoreRejectsMalformedWALRecord(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "events.jsonl"), []byte("{malformed}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Recover(); err == nil {
		t.Fatal("Recover() accepted malformed WAL record")
	}
}

func TestFileStoreIgnoresTruncatedFinalRecord(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"type":"job_submitted","timestamp":"1970-01-01T00:00:10Z","data":{"id":"job-1"}}` + "\n" + `{"type":"job_assigned"`)
	if err := os.WriteFile(filepath.Join(directory, "events.jsonl"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, events, err := store.Recover()
	if err != nil || len(events) != 1 || events[0].Type != "job_submitted" {
		t.Fatalf("Recover() = %+v, %v", events, err)
	}
}
