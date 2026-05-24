package cleanup

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

const (
	uploadsInterval          = 5 * time.Minute
	cacheEntriesInterval     = 24 * time.Hour
	storageLocationsInterval = 24 * time.Hour
	partsInterval            = time.Hour
	mergesInterval           = time.Hour
)

type Runner struct {
	service *Service
	logger  zerolog.Logger
}

func NewRunner(service *Service, logger zerolog.Logger) *Runner {
	return &Runner{
		service: service,
		logger:  logger,
	}
}

func (r *Runner) Start(ctx context.Context) {
	if r.service.config.Disabled {
		r.logger.Info().Msg("cleanup jobs disabled")
		return
	}

	go r.runPeriodically(ctx, "cleanup:uploads", uploadsInterval, r.service.RunUploads)
	go r.runPeriodically(ctx, "cleanup:cache-entries", cacheEntriesInterval, r.service.RunCacheEntries)
	go r.runPeriodically(ctx, "cleanup:storage-locations", storageLocationsInterval, r.service.RunStorageLocations)
	go r.runPeriodically(ctx, "cleanup:parts", partsInterval, r.service.RunParts)
	go r.runPeriodically(ctx, "cleanup:merges", mergesInterval, r.service.RunMerges)
}

func (r *Runner) runPeriodically(ctx context.Context, name string, interval time.Duration, run func(context.Context) (int, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := run(ctx)
			event := r.logger.Info()
			if err != nil {
				event = r.logger.Error().Err(err)
			}
			event.Str("job", name).Int("count", count).Msg("cleanup job finished")
		}
	}
}
