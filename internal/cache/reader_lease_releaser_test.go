package cache

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestReaderLeaseReleaserShutdownFlushesPendingBatch(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	releaser := newReaderLeaseReleaserWithOptions(
		func(_ context.Context, leaseIDs []string) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, append([]string(nil), leaseIDs...))
			return len(leaseIDs), nil
		},
		zerolog.Nop(),
		64,
		time.Hour,
		128,
	)
	require.True(t, releaser.Enqueue("first"))
	require.True(t, releaser.Enqueue("second"))
	require.True(t, releaser.Enqueue("third"))

	require.NoError(t, releaser.Shutdown(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]string{{"first", "second", "third"}}, calls)
}

func TestReaderLeaseReleaserSplitsBatchesAtLimit(t *testing.T) {
	var mu sync.Mutex
	var released []string
	var batchSizes []int
	releaser := newReaderLeaseReleaserWithOptions(
		func(_ context.Context, leaseIDs []string) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			released = append(released, leaseIDs...)
			batchSizes = append(batchSizes, len(leaseIDs))
			return len(leaseIDs), nil
		},
		zerolog.Nop(),
		2,
		time.Hour,
		8,
	)
	for _, leaseID := range []string{"one", "two", "three", "four", "five"} {
		require.True(t, releaser.Enqueue(leaseID))
	}

	require.NoError(t, releaser.Shutdown(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{"one", "two", "three", "four", "five"}, released)
	for _, size := range batchSizes {
		require.LessOrEqual(t, size, 2)
	}
}

func TestReaderLeaseReleaserRejectsEnqueueAfterShutdown(t *testing.T) {
	releaser := newReaderLeaseReleaserWithOptions(
		func(_ context.Context, leaseIDs []string) (int, error) {
			return len(leaseIDs), nil
		},
		zerolog.Nop(),
		64,
		time.Hour,
		128,
	)

	require.NoError(t, releaser.Shutdown(context.Background()))
	require.False(t, releaser.Enqueue("late"))
}

func TestReaderLeaseReleaserRejectsWhenPendingQueueIsFull(t *testing.T) {
	releaseStarted := make(chan struct{})
	releaser := newReaderLeaseReleaserWithOptions(
		func(ctx context.Context, leaseIDs []string) (int, error) {
			close(releaseStarted)
			<-ctx.Done()
			return 0, ctx.Err()
		},
		zerolog.Nop(),
		1,
		time.Hour,
		2,
	)
	require.True(t, releaser.Enqueue("active"))
	<-releaseStarted
	require.True(t, releaser.Enqueue("pending-one"))
	require.True(t, releaser.Enqueue("pending-two"))
	require.False(t, releaser.Enqueue("overflow"))

	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, releaser.Shutdown(shutdownCtx), context.Canceled)
}

func TestReaderLeaseReleaserShutdownCancelsActiveReleaseBeforeReturning(t *testing.T) {
	releaseStarted := make(chan struct{})
	releaseStopped := make(chan struct{})
	var logs bytes.Buffer
	releaser := newReaderLeaseReleaserWithOptions(
		func(ctx context.Context, leaseIDs []string) (int, error) {
			close(releaseStarted)
			<-ctx.Done()
			close(releaseStopped)
			return 0, ctx.Err()
		},
		zerolog.New(&logs),
		1,
		time.Hour,
		2,
	)
	require.True(t, releaser.Enqueue("active"))
	<-releaseStarted

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, releaser.Shutdown(shutdownCtx), context.DeadlineExceeded)
	select {
	case <-releaseStopped:
	default:
		t.Fatal("release callback was still active after shutdown returned")
	}
	require.Contains(t, logs.String(), "cache reader lease releases abandoned during shutdown")
	require.Contains(t, logs.String(), `"reader_lease_count":1`)
}

func TestReaderLeaseReleaserConcurrentEnqueueAndShutdownDrainsAcceptedIDs(t *testing.T) {
	var accepted atomic.Int64
	var released atomic.Int64
	releaser := newReaderLeaseReleaserWithOptions(
		func(_ context.Context, leaseIDs []string) (int, error) {
			released.Add(int64(len(leaseIDs)))
			return len(leaseIDs), nil
		},
		zerolog.Nop(),
		8,
		time.Millisecond,
		128,
	)
	require.True(t, releaser.Enqueue("seed"))
	accepted.Add(1)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if releaser.Enqueue(string(rune(index + 1))) {
				accepted.Add(1)
			}
		}()
	}
	shutdownDone := make(chan error, 1)
	close(start)
	go func() {
		shutdownDone <- releaser.Shutdown(context.Background())
	}()

	wg.Wait()
	require.NoError(t, <-shutdownDone)
	require.Equal(t, accepted.Load(), released.Load())
}
