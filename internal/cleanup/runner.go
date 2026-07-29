package cleanup

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"
)

const (
	uploadsInterval           = 5 * time.Minute
	storageDeletionsInterval  = 5 * time.Minute
	pendingLocationsInterval  = 5 * time.Minute
	readerLeasesInterval      = time.Hour
	cacheEntriesInterval      = 24 * time.Hour
	storageLocationsInterval  = 24 * time.Hour
	orphanedStorageInterval   = 24 * time.Hour
	orphanedStorageReadyDelay = 30 * time.Second
	partsInterval             = time.Hour
)

type Runner struct {
	service      *Service
	logger       zerolog.Logger
	storageReady <-chan struct{}
}

func NewRunner(service *Service, logger zerolog.Logger, storageReady <-chan struct{}) *Runner {
	return &Runner{
		service:      service,
		logger:       logger,
		storageReady: storageReady,
	}
}

func (r *Runner) Start(ctx context.Context) {
	if r.service.config.Disabled {
		r.logger.Info().Msg("cleanup jobs disabled")
		return
	}

	go r.runPeriodically(ctx, "cleanup:uploads", uploadsInterval, r.service.RunUploads)
	go r.runPeriodically(ctx, "cleanup:storage-deletions", storageDeletionsInterval, r.service.RunStorageDeletions)
	go r.runPeriodically(ctx, "cleanup:pending-storage-locations", pendingLocationsInterval, r.service.RunPendingStorageLocations)
	go r.runPeriodically(ctx, "cleanup:reader-leases", readerLeasesInterval, r.service.RunReaderLeases)
	go r.runPeriodically(ctx, "cleanup:cache-entries", cacheEntriesInterval, r.service.RunCacheEntries)
	go r.runPeriodically(ctx, "cleanup:storage-locations", storageLocationsInterval, r.service.RunStorageLocations)
	go r.runAfterStorageReadyAndPeriodically(
		ctx,
		"cleanup:orphaned-storage",
		r.storageReady,
		orphanedStorageReadyDelay,
		orphanedStorageInterval,
		r.service.RunOrphanedStorage,
	)
	go r.runPeriodically(ctx, "cleanup:parts", partsInterval, r.service.RunParts)
}

func (r *Runner) runPeriodically(ctx context.Context, name string, interval time.Duration, run func(context.Context) (int, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.runJob(ctx, name, run) {
				return
			}
		}
	}
}

func (r *Runner) runAfterStorageReadyAndPeriodically(
	ctx context.Context,
	name string,
	storageReady <-chan struct{},
	readyDelay time.Duration,
	interval time.Duration,
	run func(context.Context) (int, error),
) {
	periodicTimer := time.NewTimer(interval)
	defer periodicTimer.Stop()

	readySignal := storageReady
	var readyTimer *time.Timer
	var readyTimerC <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			stopTimer(readyTimer)
			return
		case <-readySignal:
			readySignal = nil
			readyTimer = time.NewTimer(readyDelay)
			readyTimerC = readyTimer.C
		case <-readyTimerC:
			readyTimerC = nil
			stopTimer(periodicTimer)
			if !r.runJob(ctx, name, run) {
				return
			}
			periodicTimer.Reset(interval)
		case <-periodicTimer.C:
			readySignal = nil
			stopTimer(readyTimer)
			readyTimerC = nil
			if !r.runJob(ctx, name, run) {
				return
			}
			periodicTimer.Reset(interval)
		}
	}
}

func (r *Runner) runJob(ctx context.Context, name string, run func(context.Context) (int, error)) bool {
	count, err := run(ctx)
	event := r.logger.Info()
	if err != nil {
		if ctx.Err() != nil && errorOnlyMatches(err, ctx.Err()) {
			event = r.logger.Debug().Err(err)
		} else {
			event = r.logger.Error().Err(err)
		}
	}
	event.Str("job", name).Int("count", count).Msg("cleanup job finished")
	return ctx.Err() == nil
}

func errorOnlyMatches(err, target error) bool {
	if err == nil || target == nil {
		return false
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		unwrapped := multi.Unwrap()
		if len(unwrapped) == 0 {
			return false
		}
		for _, nested := range unwrapped {
			if !errorOnlyMatches(nested, target) {
				return false
			}
		}
		return true
	}
	if single, ok := err.(interface{ Unwrap() error }); ok {
		return errorOnlyMatches(single.Unwrap(), target)
	}
	return errors.Is(err, target)
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
