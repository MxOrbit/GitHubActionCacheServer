package cleanup

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestOrphanedStorageRunsAfterStorageReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageReady := make(chan struct{})
	calls := make(chan struct{}, 1)
	done := make(chan struct{})
	runner := &Runner{logger: zerolog.Nop()}

	go func() {
		defer close(done)
		runner.runAfterStorageReadyAndPeriodically(
			ctx,
			"cleanup:orphaned-storage",
			storageReady,
			0,
			time.Hour,
			func(context.Context) (int, error) {
				calls <- struct{}{}
				return 1, nil
			},
		)
	}()

	select {
	case <-calls:
		t.Fatal("orphaned storage ran before storage readiness")
	case <-time.After(20 * time.Millisecond):
	}
	close(storageReady)
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("orphaned storage did not run after storage readiness")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("orphaned storage scheduler did not stop")
	}
}

func TestOrphanedStoragePeriodicFallbackDoesNotRequireReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageReady := make(chan struct{})
	calls := make(chan struct{}, 1)
	done := make(chan struct{})
	runner := &Runner{logger: zerolog.Nop()}

	go func() {
		defer close(done)
		runner.runAfterStorageReadyAndPeriodically(
			ctx,
			"cleanup:orphaned-storage",
			storageReady,
			time.Hour,
			10*time.Millisecond,
			func(context.Context) (int, error) {
				calls <- struct{}{}
				cancel()
				return 1, nil
			},
		)
	}()

	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("orphaned storage periodic fallback did not run")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("orphaned storage scheduler did not stop")
	}
}
