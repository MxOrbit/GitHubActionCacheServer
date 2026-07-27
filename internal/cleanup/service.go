package cleanup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagedeletion"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
)

const (
	itemsPerPage               = 10
	abandonedUploadThreshold   = time.Minute
	materializedPartsRetention = time.Hour
	storageDeletionRetryBase   = 5 * time.Minute
	storageDeletionRetryMax    = 24 * time.Hour
)

type Service struct {
	db      *ent.Client
	storage storage.Adapter
	config  config.CleanupConfig
}

type Options struct {
	DB      *ent.Client
	Storage storage.Adapter
	Config  config.CleanupConfig
}

func NewService(options Options) *Service {
	return &Service{
		db:      options.DB,
		storage: options.Storage,
		config:  options.Config,
	}
}

func (s *Service) RunUploads(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-abandonedUploadThreshold).UnixMilli()
	deleted := 0

	for {
		uploads, err := s.db.Upload.Query().
			Where(
				upload.CreatedAtLT(cutoff),
				upload.Or(
					upload.LastPartUploadedAtIsNil(),
					upload.LastPartUploadedAtLT(cutoff),
				),
			).
			Limit(itemsPerPage).
			All(ctx)
		if err != nil {
			return deleted, fmt.Errorf("query abandoned uploads: %w", err)
		}
		if len(uploads) == 0 {
			return deleted, nil
		}

		for _, currentUpload := range uploads {
			if err := s.deleteUpload(ctx, currentUpload); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
}

func (s *Service) RunCacheEntries(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-time.Duration(s.config.CacheOlderThanDays) * 24 * time.Hour).UnixMilli()
	deleted := 0

	for {
		locations, err := s.db.StorageLocation.Query().
			Where(storagelocation.LastDownloadedAtLT(cutoff)).
			Limit(itemsPerPage).
			All(ctx)
		if err != nil {
			return deleted, fmt.Errorf("query expired cache entries: %w", err)
		}
		if len(locations) == 0 {
			return deleted, nil
		}

		for _, location := range locations {
			if err := s.deleteStorageLocation(ctx, location, true); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
}

func (s *Service) RunStorageLocations(ctx context.Context) (int, error) {
	deleted := 0

	for {
		locations, err := s.db.StorageLocation.Query().
			Where(storagelocation.Not(storagelocation.HasCacheEntries())).
			Limit(itemsPerPage).
			All(ctx)
		if err != nil {
			return deleted, fmt.Errorf("query orphan storage locations: %w", err)
		}
		if len(locations) == 0 {
			return deleted, nil
		}

		for _, location := range locations {
			if err := s.deleteStorageLocation(ctx, location, false); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
}

func (s *Service) RunParts(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-materializedPartsRetention).UnixMilli()
	deletedParts := 0

	for {
		locations, err := s.db.StorageLocation.Query().
			Where(
				storagelocation.MergedAtLT(cutoff),
				storagelocation.PartsDeletedAtIsNil(),
			).
			Limit(itemsPerPage).
			All(ctx)
		if err != nil {
			return deletedParts, fmt.Errorf("query merged storage locations: %w", err)
		}
		if len(locations) == 0 {
			return deletedParts, nil
		}

		for _, location := range locations {
			task, err := s.schedulePartsDeletion(ctx, location)
			if err != nil {
				return deletedParts, err
			}
			_ = storageoutbox.Process(ctx, s.db, s.storage, task)
			deletedParts += location.PartCount
		}
	}
}

func (s *Service) RunStorageDeletions(ctx context.Context) (int, error) {
	deleted := 0
	var cursor int64
	var processErrors []error
	now := time.Now()
	minimumRetryCutoff := now.Add(-storageDeletionRetryBase).UnixMilli()

	for {
		tasks, err := s.db.StorageDeletion.Query().
			Where(
				storagedeletion.IDGT(cursor),
				storagedeletion.Or(
					storagedeletion.LastAttemptedAtIsNil(),
					storagedeletion.LastAttemptedAtLTE(minimumRetryCutoff),
				),
			).
			Order(storagedeletion.ByID()).
			Limit(itemsPerPage).
			All(ctx)
		if err != nil {
			processErrors = append(processErrors, fmt.Errorf("query storage deletions: %w", err))
			return deleted, errors.Join(processErrors...)
		}
		if len(tasks) == 0 {
			return deleted, errors.Join(processErrors...)
		}

		for _, task := range tasks {
			cursor = task.ID
			if !storageDeletionReady(task, now) {
				continue
			}
			if err := storageoutbox.Process(ctx, s.db, s.storage, task); err != nil {
				processErrors = append(processErrors, err)
				continue
			}
			deleted++
		}
	}
}

func storageDeletionReady(task *ent.StorageDeletion, now time.Time) bool {
	if task.LastAttemptedAt == nil {
		return true
	}
	retryAt := time.UnixMilli(*task.LastAttemptedAt).Add(storageDeletionRetryDelay(task.AttemptCount))
	return !now.Before(retryAt)
}

func storageDeletionRetryDelay(attemptCount int) time.Duration {
	delay := storageDeletionRetryBase
	for attempt := 1; attempt < attemptCount; attempt++ {
		if delay >= storageDeletionRetryMax/2 {
			return storageDeletionRetryMax
		}
		delay *= 2
	}
	return delay
}

func (s *Service) deleteUpload(ctx context.Context, currentUpload *ent.Upload) error {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("start upload cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	task, err := storageoutbox.Enqueue(ctx, tx.Client(), currentUpload.FolderName)
	if err != nil {
		return err
	}
	if err := tx.Upload.DeleteOneID(currentUpload.ID).Exec(ctx); err != nil {
		return fmt.Errorf("delete upload: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upload cleanup: %w", err)
	}
	committed = true

	_ = storageoutbox.Process(ctx, s.db, s.storage, task)
	return nil
}

func (s *Service) deleteStorageLocation(ctx context.Context, location *ent.StorageLocation, deleteCacheEntries bool) error {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("start cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if deleteCacheEntries {
		if _, err := tx.CacheEntry.Delete().
			Where(cacheentry.LocationId(location.ID)).
			Exec(ctx); err != nil {
			return fmt.Errorf("delete cache entries: %w", err)
		}
	}
	task, err := storageoutbox.Enqueue(ctx, tx.Client(), location.FolderName)
	if err != nil {
		return err
	}
	if err := tx.StorageLocation.DeleteOneID(location.ID).Exec(ctx); err != nil {
		if ent.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete storage location: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cleanup transaction: %w", err)
	}
	committed = true

	_ = storageoutbox.Process(ctx, s.db, s.storage, task)
	return nil
}

func (s *Service) schedulePartsDeletion(ctx context.Context, location *ent.StorageLocation) (*ent.StorageDeletion, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("start parts cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	task, err := storageoutbox.Enqueue(ctx, tx.Client(), partsFolderName(location.FolderName))
	if err != nil {
		return nil, err
	}
	if err := tx.StorageLocation.UpdateOneID(location.ID).
		SetPartsDeletedAt(time.Now().UnixMilli()).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("mark parts deleted: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit parts cleanup: %w", err)
	}
	committed = true
	return task, nil
}

func partsFolderName(folderName string) string {
	return folderName + "/parts"
}
