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

func TestPeriodicRunnerDrainsInFlightJobAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	runner := &Runner{logger: zerolog.Nop()}
	t.Cleanup(cancel)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	go func() {
		defer close(done)
		runner.runPeriodically(ctx, "cleanup:test", time.Millisecond, func(context.Context) (int, error) {
			started <- struct{}{}
			<-release
			return 1, nil
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup job did not start")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("cleanup scheduler returned before its in-flight job")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup scheduler did not stop after its in-flight job returned")
	}
}
