package storagereconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/rs/zerolog"
)

const (
	maxLocationRechecks = 10
	initialRetryDelay   = time.Minute
	maximumRetryDelay   = time.Hour
)

var ErrPending = errors.New("storage size reconciliation is waiting for concurrent lifecycle work")

type Options struct {
	DB        *ent.Client
	Storage   storage.Adapter
	Lifecycle *storagelifecycle.Service
	Logger    *zerolog.Logger
}

type Result struct {
	InventoryObjects int64
	InventoryFolders int
	Candidates       int
	Updated          int
	Purged           int
	Deferred         int
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
				Int64("inventory_objects", result.InventoryObjects).
				Int("inventory_folders", result.InventoryFolders).
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

	locations, err := options.DB.StorageLocation.Query().
		Where(
			storagelocation.SizeBytesIsNil(),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		All(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("query storage locations missing sizes: %w", err)
	}
	result := Result{Candidates: len(locations)}
	if len(locations) == 0 {
		return result, nil
	}

	inventory, err := options.Storage.Inventory(ctx)
	if err != nil {
		return result, fmt.Errorf("inventory storage for size reconciliation: %w", err)
	}
	result.InventoryObjects = inventory.ObjectCount
	result.InventoryFolders = len(inventory.Folders)

	var reconcileErrors []error
	for _, candidate := range locations {
		outcome, err := reconcileLocation(ctx, options, inventory, candidate.ID)
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("reconcile storage location %s: %w", candidate.ID, err))
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
			reconcileErrors = append(reconcileErrors, fmt.Errorf("reconcile storage location %s: unknown outcome %q", candidate.ID, outcome))
		}
	}
	if len(reconcileErrors) > 0 {
		return result, errors.Join(reconcileErrors...)
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

func reconcileLocation(ctx context.Context, options Options, inventory storage.Inventory, locationID string) (locationOutcome, error) {
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

		resolution, err := resolveLocationSize(ctx, options, inventory, location)
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

func resolveLocationSize(ctx context.Context, options Options, inventory storage.Inventory, location *ent.StorageLocation) (sizeResolution, error) {
	if location.PartCount < 1 {
		return sizeResolution{dangling: true}, nil
	}
	folder, folderFound := inventory.Folder(location.FolderName)

	if location.MergedAt != nil {
		if folderFound && folder.Merged != nil {
			size := folder.Merged.SizeBytes
			return sizeResolution{sizeBytes: &size}, nil
		}
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
	if folderFound {
		if size, err := folder.LogicalPartsSize(location.PartCount); err == nil {
			return sizeResolution{sizeBytes: &size}, nil
		}
	}

	contents, err := options.Storage.InspectFolder(ctx, storage.PartsFolder(location.FolderName))
	if err != nil {
		return sizeResolution{}, fmt.Errorf("reinspect active parts: %w", err)
	}
	for range location.PartCount + 1 {
		size, sizeErr := contents.LogicalIndexedSize(location.PartCount)
		if sizeErr == nil {
			return sizeResolution{sizeBytes: &size}, nil
		}
		var missing storage.IndexedObjectMissingError
		if !errors.As(sizeErr, &missing) {
			return sizeResolution{}, fmt.Errorf("measure active parts: %w", sizeErr)
		}

		objectName := storage.PartsFolder(location.FolderName) + "/" + strconv.Itoa(missing.Index)
		metadata, inspectErr := options.Storage.InspectObject(ctx, objectName)
		if inspectErr != nil {
			if errors.Is(inspectErr, storage.ErrObjectNotFound) {
				if options.Lifecycle.MaterializationActive(location) {
					return sizeResolution{deferred: true}, nil
				}
				return sizeResolution{dangling: true}, nil
			}
			return sizeResolution{}, fmt.Errorf("confirm missing active part %d: %w", missing.Index, inspectErr)
		}
		metadata.Name = strconv.Itoa(missing.Index)
		contents.Objects = append(contents.Objects, metadata)
	}
	return sizeResolution{}, fmt.Errorf("measure active parts: exceeded targeted reinspection bound")
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
