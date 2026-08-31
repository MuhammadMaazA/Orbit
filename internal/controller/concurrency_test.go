package controller

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"orbit/internal/model"
	"orbit/internal/scheduler"
)

func TestControllerConcurrentSubmissionsRemainConsistent(t *testing.T) {
	c, err := NewWithConfig(scheduler.FirstFit{}, Config{MaxAttempts: 2, MaxQueuedJobs: 0, AgingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const count = 100
	var group sync.WaitGroup
	group.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer group.Done()
			if _, err := c.Submit(model.Job{ID: fmt.Sprintf("job-%d", index), CPU: 1}); err != nil {
				t.Errorf("Submit() error = %v", err)
			}
		}(i)
	}
	group.Wait()
	stats := c.Stats()
	if stats.Queued != count || stats.Running != 0 || stats.Completed != 0 {
		t.Fatalf("Stats() = %+v", stats)
	}
}
