package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type Store interface {
	Append(Event) error
	Snapshot([]byte) error
	Recover() ([]byte, []Event, error)
}

type FileStore struct {
	mu           sync.Mutex
	directory    string
	walPath      string
	snapshotPath string
}

func NewFileStore(directory string) (*FileStore, error) {
	if directory == "" {
		return nil, fmt.Errorf("storage: directory is required")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create directory: %w", err)
	}
	return &FileStore{directory: directory, walPath: filepath.Join(directory, "events.jsonl"), snapshotPath: filepath.Join(directory, "snapshot.json")}, nil
}

func (s *FileStore) Append(event Event) error {
	if event.Type == "" {
		return fmt.Errorf("storage: event type is required")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("storage: encode event: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open WAL: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("storage: append WAL: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("storage: sync WAL: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("storage: close WAL: %w", err)
	}
	return nil
}

func (s *FileStore) Snapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	temporary, err := os.CreateTemp(s.directory, "snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("storage: create snapshot: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("storage: write snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("storage: sync snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("storage: close snapshot: %w", err)
	}
	if err := os.Rename(temporaryName, s.snapshotPath); err != nil {
		return fmt.Errorf("storage: replace snapshot: %w", err)
	}
	return nil
}

func (s *FileStore) Recover() ([]byte, []Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshot []byte
	if data, err := os.ReadFile(s.snapshotPath); err == nil {
		snapshot = data
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("storage: read snapshot: %w", err)
	}
	file, err := os.Open(s.walPath)
	if errors.Is(err, os.ErrNotExist) {
		return snapshot, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("storage: open WAL: %w", err)
	}
	defer file.Close()
	var events []Event
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: read WAL: %w", err)
	}
	records := bytes.Split(data, []byte{'\n'})
	for index, line := range records {
		if len(line) == 0 {
			continue
		}
		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			if index == len(records)-1 && !bytes.HasSuffix(data, []byte{'\n'}) {
				break
			}
			return nil, nil, fmt.Errorf("storage: decode WAL record: %w", err)
		}
		events = append(events, event)
	}
	return snapshot, events, nil
}
