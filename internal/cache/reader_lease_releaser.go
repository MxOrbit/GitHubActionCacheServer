package cache

import (
	"context"
	"sync"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/rs/zerolog"
)

const (
	readerLeaseReleaseBatchSize  = 64
	readerLeaseReleaseDelay      = 5 * time.Millisecond
	readerLeaseReleaseMaxPending = 4096
)

type readerLeaseReleaseFunc func(context.Context, []string) (int, error)

type readerLeaseReleaser struct {
	release    readerLeaseReleaseFunc
	logger     zerolog.Logger
	batchSize  int
	flushDelay time.Duration

	mu            sync.Mutex
	accepting     bool
	workerStarted bool
	queue         chan string
	workerCancel  context.CancelFunc
	workerDone    chan struct{}
}

func newReaderLeaseReleaser(lifecycle *storagelifecycle.Service, logger zerolog.Logger) *readerLeaseReleaser {
	return newReaderLeaseReleaserWithOptions(
		lifecycle.ReleaseReaders,
		logger,
		readerLeaseReleaseBatchSize,
		readerLeaseReleaseDelay,
		readerLeaseReleaseMaxPending,
	)
}

func newReaderLeaseReleaserWithOptions(
	release readerLeaseReleaseFunc,
	logger zerolog.Logger,
	batchSize int,
	flushDelay time.Duration,
	maxPending int,
) *readerLeaseReleaser {
	if batchSize < 1 {
		batchSize = 1
	}
	if flushDelay <= 0 {
		flushDelay = readerLeaseReleaseDelay
	}
	if maxPending < batchSize {
		maxPending = batchSize
	}
	return &readerLeaseReleaser{
		release:    release,
		logger:     logger,
		batchSize:  batchSize,
		flushDelay: flushDelay,
		accepting:  true,
		queue:      make(chan string, maxPending),
	}
}

func (r *readerLeaseReleaser) Enqueue(leaseID string) bool {
	if leaseID == "" {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return false
	}
	if !r.workerStarted {
		r.startWorkerLocked()
	}
	select {
	case r.queue <- leaseID:
		return true
	default:
		return false
	}
}

func (r *readerLeaseReleaser) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.accepting {
		r.accepting = false
		if r.workerStarted {
			close(r.queue)
		}
	}
	if !r.workerStarted {
		r.mu.Unlock()
		return nil
	}
	done := r.workerDone
	cancel := r.workerCancel
	r.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	}
}

func (r *readerLeaseReleaser) startWorkerLocked() {
	workerCtx, workerCancel := context.WithCancel(context.Background())
	r.workerStarted = true
	r.workerCancel = workerCancel
	r.workerDone = make(chan struct{})
	go r.runWorker(workerCtx, r.workerDone)
}

func (r *readerLeaseReleaser) runWorker(ctx context.Context, done chan struct{}) {
	defer close(done)
	batch := make([]string, 0, r.batchSize)
	timer := time.NewTimer(r.flushDelay)
	stopTimer(timer)
	var timerC <-chan time.Time
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logAbandoned(len(batch)+len(r.queue), ctx.Err())
			return
		case leaseID, ok := <-r.queue:
			if !ok {
				stopTimer(timer)
				if len(batch) > 0 {
					r.releaseBatch(ctx, batch)
				}
				return
			}
			batch = append(batch, leaseID)
			if len(batch) == 1 {
				timer.Reset(r.flushDelay)
				timerC = timer.C
			}
			if len(batch) == r.batchSize {
				stopTimer(timer)
				timerC = nil
				if !r.releaseBatch(ctx, batch) {
					return
				}
				batch = batch[:0]
			}
		case <-timerC:
			timerC = nil
			if !r.releaseBatch(ctx, batch) {
				return
			}
			batch = batch[:0]
		}
	}
}

func (r *readerLeaseReleaser) releaseBatch(ctx context.Context, batch []string) bool {
	cleanupCtx, cancel := context.WithTimeout(ctx, mergeCleanupTimeout)
	_, err := r.release(cleanupCtx, batch)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			r.logAbandoned(len(batch)+len(r.queue), ctx.Err())
			return false
		}
		r.logger.Error().
			Err(err).
			Int("reader_lease_count", len(batch)).
			Msg("cache reader lease batch release failed")
	}
	if ctx.Err() != nil {
		r.logAbandoned(len(r.queue), ctx.Err())
		return false
	}
	return true
}

func (r *readerLeaseReleaser) logAbandoned(count int, err error) {
	if count == 0 {
		return
	}
	r.logger.Warn().
		Err(err).
		Int("reader_lease_count", count).
		Msg("cache reader lease releases abandoned during shutdown")
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
