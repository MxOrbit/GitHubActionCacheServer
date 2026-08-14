package cleanup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagedeletion"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
)

const (
	temporaryUploadRetention  = 24 * time.Hour
	orphanFolderBatchSize     = 200
	temporaryUploadBatchSize  = 200
	activeUploadQueryPageSize = 200
	maxOrphanErrors           = 10
)

type orphanErrorCollector struct {
	count  int
	errors []error
}

func (c *orphanErrorCollector) add(err error) {
	if err == nil {
		return
	}
	c.count++
	if len(c.errors) < maxOrphanErrors {
		c.errors = append(c.errors, err)
	}
}

func (c *orphanErrorCollector) err() error {
	if c.count == 0 {
		return nil
	}
	errorsToJoin := append([]error(nil), c.errors...)
	if omitted := c.count - len(c.errors); omitted > 0 {
		errorsToJoin = append(errorsToJoin, fmt.Errorf("%d additional orphan reconciliation errors omitted", omitted))
	}
	return errors.Join(errorsToJoin...)
}

func (s *Service) RunOrphanedStorage(ctx context.Context) (int, error) {
	gracePeriod := s.config.OrphanedStorageGracePeriod
	if gracePeriod <= 0 {
		gracePeriod = time.Duration(config.DefaultOrphanedStorageGraceHours) * time.Hour
	}
	cutoff := s.now().Add(-gracePeriod).UTC()
	queued := 0
	reconcileErrors := &orphanErrorCollector{}
	folderBatch := make([]string, 0, orphanFolderBatchSize)
	flushFolders := func() error {
		if len(folderBatch) == 0 {
			return nil
		}
		batch := append([]string(nil), folderBatch...)
		folderBatch = folderBatch[:0]
		batchQueued, err := s.processOrphanFolderBatch(ctx, batch, cutoff, reconcileErrors)
		queued += batchQueued
		return err
	}
	walkErr := s.storage.WalkTopLevelFolders(ctx, func(folderName string) error {
		folderBatch = append(folderBatch, folderName)
		if len(folderBatch) == orphanFolderBatchSize {
			return flushFolders()
		}
		return nil
	})
	if walkErr != nil {
		reconcileErrors.add(fmt.Errorf("walk storage folders for orphan reconciliation: %w", walkErr))
	}
	if err := flushFolders(); err != nil {
		reconcileErrors.add(err)
	}

	if cleaner, ok := s.storage.(storage.TemporaryUploadCleaner); ok {
		temporaryCutoff := s.now().Add(-temporaryUploadRetention)
		candidates, deleted, cleanupErr := s.runTemporaryUploadCleanup(ctx, cleaner, temporaryCutoff)
		s.logger.Info().
			Int("candidates", candidates).
			Int("deleted", deleted).
			Msg("temporary upload cleanup finished")
		if cleanupErr != nil {
			reconcileErrors.add(fmt.Errorf("cleanup temporary uploads: %w", cleanupErr))
		}
	}
	return queued, reconcileErrors.err()
}

func (s *Service) processOrphanFolderBatch(
	ctx context.Context,
	folderNames []string,
	cutoff time.Time,
	reconcileErrors *orphanErrorCollector,
) (int, error) {
	references, err := s.referencedStorageFolderBatch(ctx, folderNames)
	if err != nil {
		return 0, err
	}
	queued := 0
	for _, folderName := range folderNames {
		if err := ctx.Err(); err != nil {
			return queued, err
		}
		if _, ok := references[folderName]; ok {
			continue
		}

		summary, err := s.storage.InspectFolderSummary(ctx, folderName)
		if err != nil {
			reconcileErrors.add(fmt.Errorf("reinspect orphan storage folder %q: %w", folderName, err))
			continue
		}
		if !summary.Exists {
			continue
		}
		if summary.NewestModifiedAt.IsZero() {
			reconcileErrors.add(fmt.Errorf("reinspect orphan storage folder %q: missing modification time", folderName))
			continue
		}
		if summary.NewestModifiedAt.After(cutoff) {
			continue
		}

		claimed, err := s.claimOrphanedStorageFolder(ctx, folderName)
		if err != nil {
			reconcileErrors.add(fmt.Errorf("claim orphan storage folder %q: %w", folderName, err))
			continue
		}
		if claimed {
			queued++
		}
	}
	return queued, nil
}

func (s *Service) referencedStorageFolderBatch(ctx context.Context, folderNames []string) (map[string]struct{}, error) {
	references := make(map[string]struct{}, len(folderNames))
	queries := []struct {
		name string
		run  func() ([]string, error)
	}{
		{
			name: "uploads",
			run: func() ([]string, error) {
				var names []string
				err := s.db.Upload.Query().
					Where(upload.FolderNameIn(folderNames...)).
					Unique(true).
					Select(upload.FieldFolderName).
					Scan(ctx, &names)
				return names, err
			},
		},
		{
			name: "storage locations",
			run: func() ([]string, error) {
				var names []string
				err := s.db.StorageLocation.Query().
					Where(storagelocation.FolderNameIn(folderNames...)).
					Unique(true).
					Select(storagelocation.FieldFolderName).
					Scan(ctx, &names)
				return names, err
			},
		},
		{
			name: "storage deletions",
			run: func() ([]string, error) {
				deletionNames := make([]string, 0, len(folderNames)*3)
				for _, folderName := range folderNames {
					deletionNames = append(deletionNames, folderName, folderName+"/parts", folderName+"/blocks")
				}
				var names []string
				err := s.db.StorageDeletion.Query().
					Where(storagedeletion.FolderNameIn(deletionNames...)).
					Unique(true).
					Select(storagedeletion.FieldFolderName).
					Scan(ctx, &names)
				return names, err
			},
		},
	}
	for _, query := range queries {
		names, err := query.run()
		if err != nil {
			return nil, fmt.Errorf("query %s for orphan reconciliation: %w", query.name, err)
		}
		for _, name := range names {
			if root := topLevelStorageFolder(name); root != "" {
				references[root] = struct{}{}
			}
		}
	}
	return references, nil
}

func (s *Service) runTemporaryUploadCleanup(
	ctx context.Context,
	cleaner storage.TemporaryUploadCleaner,
	cutoff time.Time,
) (int, int, error) {
	cleanupErrors := &orphanErrorCollector{}
	batch := make([]storage.ObjectMetadata, 0, temporaryUploadBatchSize)
	candidates := 0
	deleted := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		currentBatch := append([]storage.ObjectMetadata(nil), batch...)
		batch = batch[:0]
		activeFolders, err := s.activeUploadFolderBatch(ctx, currentBatch)
		if err != nil {
			return err
		}
		deletable := make([]storage.ObjectMetadata, 0, len(currentBatch))
		for _, candidate := range currentBatch {
			folderName := topLevelStorageFolder(candidate.Name)
			if _, active := activeFolders[folderName]; active {
				s.logger.Debug().
					Str("object_name", candidate.Name).
					Str("folder_name", folderName).
					Msg("temporary upload retained for active upload")
				continue
			}
			deletable = append(deletable, candidate)
		}
		candidates += len(deletable)
		batchDeleted, err := cleaner.CleanupTemporaryUploads(ctx, deletable, cutoff)
		deleted += batchDeleted
		cleanupErrors.add(err)
		return nil
	}

	walkErr := cleaner.WalkTemporaryUploads(ctx, func(candidate storage.ObjectMetadata) error {
		if candidate.ModifiedAt.After(cutoff) {
			return nil
		}
		batch = append(batch, candidate)
		if len(batch) == temporaryUploadBatchSize {
			return flush()
		}
		return nil
	})
	if walkErr != nil {
		cleanupErrors.add(fmt.Errorf("walk temporary uploads: %w", walkErr))
	}
	if err := flush(); err != nil {
		cleanupErrors.add(err)
	}
	return candidates, deleted, cleanupErrors.err()
}

func (s *Service) activeUploadFolderBatch(ctx context.Context, candidates []storage.ObjectMetadata) (map[string]struct{}, error) {
	roots := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if root := topLevelStorageFolder(candidate.Name); root != "" {
			roots[root] = struct{}{}
		}
	}
	predicates := make([]predicate.Upload, 0, len(roots))
	for root := range roots {
		predicates = append(predicates, upload.Or(
			upload.FolderName(root),
			upload.FolderNameHasPrefix(root+"/"),
		))
	}
	active := make(map[string]struct{}, len(predicates))
	if len(predicates) == 0 {
		return active, nil
	}
	// Legacy imports may use nested upload folder names. Preserve that prefix
	// behavior, but keyset-page matches so duplicate or adversarial rows cannot
	// turn a fixed temporary-file batch into an unbounded result slice.
	var cursor int64
	hasCursor := false
	for {
		queryPredicates := []predicate.Upload{upload.Or(predicates...)}
		if hasCursor {
			queryPredicates = append(queryPredicates, upload.IDGT(cursor))
		}
		matches, err := s.db.Upload.Query().
			Where(queryPredicates...).
			Order(upload.ByID()).
			Limit(activeUploadQueryPageSize).
			All(ctx)
		if err != nil {
			return nil, fmt.Errorf("query uploads for temporary upload cleanup: %w", err)
		}
		for _, match := range matches {
			cursor = match.ID
			hasCursor = true
			if root := topLevelStorageFolder(match.FolderName); root != "" {
				active[root] = struct{}{}
			}
		}
		if len(matches) < activeUploadQueryPageSize || len(active) == len(roots) {
			return active, nil
		}
	}
}

func (s *Service) claimOrphanedStorageFolder(ctx context.Context, folderName string) (bool, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return false, fmt.Errorf("start orphan storage claim transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	folderPrefix := folderName + "/"
	hasUpload, err := tx.Upload.Query().
		Where(upload.Or(upload.FolderName(folderName), upload.FolderNameHasPrefix(folderPrefix))).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("recheck orphan storage uploads: %w", err)
	}
	hasLocation, err := tx.StorageLocation.Query().
		Where(storagelocation.Or(storagelocation.FolderName(folderName), storagelocation.FolderNameHasPrefix(folderPrefix))).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("recheck orphan storage locations: %w", err)
	}
	hasDeletion, err := tx.StorageDeletion.Query().
		Where(storagedeletion.Or(storagedeletion.FolderName(folderName), storagedeletion.FolderNameHasPrefix(folderPrefix))).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("recheck orphan storage deletions: %w", err)
	}
	if hasUpload || hasLocation || hasDeletion {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit retained storage folder check: %w", err)
		}
		committed = true
		return false, nil
	}

	if _, err := storageoutbox.Enqueue(ctx, tx.Client(), folderName); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit orphan storage deletion: %w", err)
	}
	committed = true
	return true, nil
}

func topLevelStorageFolder(name string) string {
	root, _, _ := strings.Cut(name, "/")
	return root
}
