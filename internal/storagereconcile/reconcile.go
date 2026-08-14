package storagereconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/rs/zerolog"
)

const (
	maxLocationRechecks     = 10
	reconciliationPageSize  = 100
	maxReconciliationErrors = 10
	initialRetryDelay       = time.Minute
	maximumRetryDelay       = time.Hour
)

var ErrPending = errors.New("storage size reconciliation is waiting for concurrent lifecycle work")

type Options struct {
	DB        *ent.Client
	Storage   storage.Adapter
	Lifecycle *storagelifecycle.Service
	Logger    *zerolog.Logger
}

type Result struct {
	Candidates int
	Updated    int
	Purged     int
	Deferred   int
}

type locationOutcome string

const (
	locationResolved locationOutcome = "resolved"
	locationUpdated  locationOutcome = "updated"
	locationPurged   locationOutcome = "purged"
	locationDeferred locationOutcome = "deferred"
)

type sizeResolution struct {
	sizeBytes *int64
	dangling  bool
	deferred  bool
}

type boundedErrorCollector struct {
	limit  int
	count  int
	errors []error
}

func newBoundedErrorCollector(limit int) *boundedErrorCollector {
	return &boundedErrorCollector{limit: limit, errors: make([]error, 0, limit)}
}

func (c *boundedErrorCollector) Add(err error) {
	if err == nil {
		return
	}
	c.count++
	if len(c.errors) < c.limit {
		c.errors = append(c.errors, err)
	}
}

func (c *boundedErrorCollector) Err() error {
	if c.count == 0 {
		return nil
	}
	errorsToJoin := append([]error(nil), c.errors...)
	if omitted := c.count - len(c.errors); omitted > 0 {
		errorsToJoin = append(errorsToJoin, fmt.Errorf("%d additional reconciliation errors omitted", omitted))
	}
	return errors.Join(errorsToJoin...)
}

func Run(ctx context.Context, options Options, ready chan<- struct{}) {
	logger := reconciliationLogger(options.Logger)
	for attempt := 1; ; attempt++ {
		startedAt := time.Now()
		result, err := Reconcile(ctx, options)
		if err == nil {
			if ready != nil {
				close(ready)
			}
			logger.Info().
				Int("attempt", attempt).
				Int("candidates", result.Candidates).
				Int("updated", result.Updated).
				Int("purged", result.Purged).
				Dur("duration", time.Since(startedAt)).
				Msg("storage size reconciliation completed")
			return
		}
		if ctx.Err() != nil {
			return
		}

		retryIn := reconciliationRetryDelay(attempt)
		event := logger.Error()
		if errors.Is(err, ErrPending) {
			event = logger.Info()
		}
		event.
			Err(err).
			Int("attempt", attempt).
			Dur("retry_in", retryIn).
			Int("candidates", result.Candidates).
			Int("updated", result.Updated).
			Int("purged", result.Purged).
			Int("deferred", result.Deferred).
			Msg("storage size reconciliation incomplete")

		timer := time.NewTimer(retryIn)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func Reconcile(ctx context.Context, options Options) (Result, error) {
	if options.DB == nil || options.Storage == nil || options.Lifecycle == nil {
		return Result{}, fmt.Errorf("storage size reconciliation dependencies are required")
	}

	result := Result{}
	cursor := ""
	reconcileErrors := newBoundedErrorCollector(maxReconciliationErrors)
	for {
		predicates := []predicate.StorageLocation{
			storagelocation.SizeBytesIsNil(),
			storagelocation.DeletionRequestedAtIsNil(),
		}
		if cursor != "" {
			predicates = append(predicates, storagelocation.IDGT(cursor))
		}
		locations, err := options.DB.StorageLocation.Query().
			Where(predicates...).
			Order(storagelocation.ByID()).
			Limit(reconciliationPageSize).
			All(ctx)
		if err != nil {
			return result, fmt.Errorf("query storage locations missing sizes: %w", err)
		}
		if len(locations) == 0 {
			break
		}

		for _, candidate := range locations {
			cursor = candidate.ID
			result.Candidates++
			outcome, err := reconcileLocation(ctx, options, candidate.ID)
			if err != nil {
				reconcileErrors.Add(fmt.Errorf("reconcile storage location %s: %w", candidate.ID, err))
				continue
			}
			switch outcome {
			case locationResolved:
				continue
			case locationUpdated:
				result.Updated++
			case locationPurged:
				result.Purged++
			case locationDeferred:
				result.Deferred++
			default:
				reconcileErrors.Add(fmt.Errorf("reconcile storage location %s: unknown outcome %q", candidate.ID, outcome))
			}
		}
	}
	if err := reconcileErrors.Err(); err != nil {
		return result, err
	}
	if result.Deferred > 0 {
		return result, fmt.Errorf("%w: %d locations", ErrPending, result.Deferred)
	}

	remaining, err := options.DB.StorageLocation.Query().
		Where(
			storagelocation.SizeBytesIsNil(),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		Count(ctx)
	if err != nil {
		return result, fmt.Errorf("verify storage size reconciliation completion: %w", err)
	}
	if remaining > 0 {
		return result, fmt.Errorf("%w: %d newly unresolved locations", ErrPending, remaining)
	}
	return result, nil
}

func reconcileLocation(ctx context.Context, options Options, locationID string) (locationOutcome, error) {
	purged := false
	for range maxLocationRechecks {
		location, err := options.DB.StorageLocation.Get(ctx, locationID)
		if err != nil {
			if ent.IsNotFound(err) {
				if purged {
					return locationPurged, nil
				}
				return locationResolved, nil
			}
			return locationDeferred, fmt.Errorf("refresh storage location: %w", err)
		}
		if location.SizeBytes != nil {
			if purged {
				return locationPurged, nil
			}
			return locationResolved, nil
		}
		if location.DeletionRequestedAt != nil {
			return locationPurged, nil
		}

		resolution, err := resolveLocationSize(ctx, options, location)
		if err != nil {
			return locationDeferred, err
		}
		if resolution.deferred {
			return locationDeferred, nil
		}
		if resolution.sizeBytes != nil {
			affected, err := options.DB.StorageLocation.Update().
				Where(
					storagelocation.ID(location.ID),
					storagelocation.SizeBytesIsNil(),
					storagelocation.DeletionRequestedAtIsNil(),
				).
				SetSizeBytes(*resolution.sizeBytes).
				Save(ctx)
			if err != nil {
				return locationDeferred, fmt.Errorf("record reconciled payload size: %w", err)
			}
			if affected > 0 {
				return locationUpdated, nil
			}
			continue
		}
		if !resolution.dangling {
			return locationDeferred, fmt.Errorf("size resolution produced no result")
		}

		changed, err := purgeDanglingLocation(ctx, options, location)
		if err != nil {
			return locationDeferred, err
		}
		purged = purged || changed
	}
	if purged {
		return locationPurged, nil
	}
	return locationDeferred, nil
}

func resolveLocationSize(ctx context.Context, options Options, location *ent.StorageLocation) (sizeResolution, error) {
	if location.PartCount < 1 {
		return sizeResolution{dangling: true}, nil
	}

	if location.MergedAt != nil {
		metadata, err := options.Storage.InspectObject(ctx, storage.MergedObject(location.FolderName))
		if err == nil {
			size := metadata.SizeBytes
			return sizeResolution{sizeBytes: &size}, nil
		}
		if errors.Is(err, storage.ErrObjectNotFound) {
			if options.Lifecycle.MaterializationActive(location) {
				return sizeResolution{deferred: true}, nil
			}
			return sizeResolution{dangling: true}, nil
		}
		return sizeResolution{}, fmt.Errorf("inspect active merged object: %w", err)
	}
	if location.PartsDeletedAt != nil {
		return sizeResolution{dangling: true}, nil
	}

	for range maxLocationRechecks {
		size, sizeErr := options.Storage.InspectIndexedFolder(ctx, storage.PartsFolder(location.FolderName), location.PartCount)
		if sizeErr == nil {
			return sizeResolution{sizeBytes: &size}, nil
		}
		if errors.Is(sizeErr, storage.ErrIndexedObjectLimitExceeded) {
			// Current uploads cannot create this representation, and measuring it
			// exactly would violate the bounded-memory contract. Treat restored or
			// legacy over-limit rows as terminally corrupt so one row cannot block
			// global reconciliation readiness forever.
			return sizeResolution{dangling: true}, nil
		}
		var missing storage.IndexedObjectMissingError
		if !errors.As(sizeErr, &missing) {
			return sizeResolution{}, fmt.Errorf("measure active parts: %w", sizeErr)
		}

		objectName := storage.PartsFolder(location.FolderName) + "/" + strconv.Itoa(missing.Index)
		_, inspectErr := options.Storage.InspectObject(ctx, objectName)
		if inspectErr != nil {
			if errors.Is(inspectErr, storage.ErrObjectNotFound) {
				if options.Lifecycle.MaterializationActive(location) {
					return sizeResolution{deferred: true}, nil
				}
				return sizeResolution{dangling: true}, nil
			}
			return sizeResolution{}, fmt.Errorf("confirm missing active part %d: %w", missing.Index, inspectErr)
		}
		// The listing was incomplete while a point lookup found the object.
		// Retry a fresh bounded listing; persistent disagreement fails closed.
	}
	return sizeResolution{}, fmt.Errorf("measure active parts: listing disagreed with point inspection after %d attempts", maxLocationRechecks)
}

func purgeDanglingLocation(ctx context.Context, options Options, location *ent.StorageLocation) (bool, error) {
	entry, err := options.DB.CacheEntry.Query().
		Where(cacheentry.LocationId(location.ID)).
		First(ctx)
	if err != nil {
		if !ent.IsNotFound(err) {
			return false, fmt.Errorf("query dangling cache entry: %w", err)
		}
		result, err := options.Lifecycle.RequestLocationDeletion(ctx, location.ID, false, true)
		if err != nil {
			return false, err
		}
		return result.Fenced || result.Finalized, nil
	}
	deleted, err := options.Lifecycle.PurgeDanglingCacheEntryIfUnchanged(ctx, entry.ID, location)
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func reconciliationRetryDelay(attempt int) time.Duration {
	delay := initialRetryDelay
	for current := 1; current < attempt && delay < maximumRetryDelay; current++ {
		if delay >= maximumRetryDelay/2 {
			return maximumRetryDelay
		}
		delay *= 2
	}
	return min(delay, maximumRetryDelay)
}

func reconciliationLogger(logger *zerolog.Logger) zerolog.Logger {
	if logger == nil {
		return zerolog.Nop()
	}
	return *logger
}
