package cleanup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagedeletion"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
)

const temporaryUploadRetention = 24 * time.Hour

type storageFolderReferences struct {
	referenced    map[string]struct{}
	activeUploads map[string]struct{}
}

func (s *Service) RunOrphanedStorage(ctx context.Context) (int, error) {
	inventory, err := s.storage.Inventory(ctx)
	if err != nil {
		return 0, fmt.Errorf("inventory storage for orphan reconciliation: %w", err)
	}

	folderReferences, err := s.referencedStorageFolders(ctx)
	if err != nil {
		return 0, err
	}

	gracePeriod := s.config.OrphanedStorageGracePeriod
	if gracePeriod <= 0 {
		gracePeriod = time.Duration(config.DefaultOrphanedStorageGraceHours) * time.Hour
	}
	cutoff := s.now().Add(-gracePeriod).UTC()
	queued := 0
	var reconcileErrors []error
	for _, folder := range inventory.Folders {
		if err := ctx.Err(); err != nil {
			if len(reconcileErrors) > 0 {
				return queued, errors.Join(reconcileErrors...)
			}
			return queued, err
		}
		if _, ok := folderReferences.referenced[folder.FolderName]; ok {
			continue
		}
		if !folder.NewestModifiedAt.IsZero() && folder.NewestModifiedAt.After(cutoff) {
			continue
		}

		// A full inventory is not an atomic snapshot. Reinspect only candidates
		// before claiming them so activity observed later still receives grace.
		contents, err := s.storage.InspectFolder(ctx, folder.FolderName)
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("reinspect orphan storage folder %q: %w", folder.FolderName, err))
			continue
		}
		if !contents.Exists {
			continue
		}
		if contents.NewestModifiedAt.IsZero() {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("reinspect orphan storage folder %q: missing modification time", folder.FolderName))
			continue
		}
		if contents.NewestModifiedAt.After(cutoff) {
			continue
		}

		claimed, err := s.claimOrphanedStorageFolder(ctx, folder.FolderName)
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("claim orphan storage folder %q: %w", folder.FolderName, err))
			continue
		}
		if claimed {
			queued++
		}
	}

	if cleaner, ok := s.storage.(storage.TemporaryUploadCleaner); ok {
		candidates := make([]storage.ObjectMetadata, 0, len(inventory.TemporaryUploads))
		temporaryCutoff := s.now().Add(-temporaryUploadRetention)
		for _, candidate := range inventory.TemporaryUploads {
			if candidate.ModifiedAt.After(temporaryCutoff) {
				continue
			}
			folderName := topLevelStorageFolder(candidate.Name)
			if _, active := folderReferences.activeUploads[folderName]; active {
				s.logger.Debug().
					Str("object_name", candidate.Name).
					Str("folder_name", folderName).
					Msg("temporary upload retained for active upload")
				continue
			}
			candidates = append(candidates, candidate)
		}

		deleted, cleanupErr := cleaner.CleanupTemporaryUploads(ctx, candidates, temporaryCutoff)
		s.logger.Info().
			Int("candidates", len(candidates)).
			Int("deleted", deleted).
			Msg("temporary upload cleanup finished")
		if cleanupErr != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("cleanup temporary uploads: %w", cleanupErr))
		}
	}
	return queued, errors.Join(reconcileErrors...)
}

func (s *Service) referencedStorageFolders(ctx context.Context) (storageFolderReferences, error) {
	references := storageFolderReferences{
		referenced:    make(map[string]struct{}),
		activeUploads: make(map[string]struct{}),
	}
	queries := []struct {
		name         string
		activeUpload bool
		run          func() ([]string, error)
	}{
		{
			name:         "uploads",
			activeUpload: true,
			run: func() ([]string, error) {
				var names []string
				err := s.db.Upload.Query().Select(upload.FieldFolderName).Scan(ctx, &names)
				return names, err
			},
		},
		{
			name: "storage locations",
			run: func() ([]string, error) {
				var names []string
				err := s.db.StorageLocation.Query().Select(storagelocation.FieldFolderName).Scan(ctx, &names)
				return names, err
			},
		},
		{
			name: "storage deletions",
			run: func() ([]string, error) {
				var names []string
				err := s.db.StorageDeletion.Query().Select(storagedeletion.FieldFolderName).Scan(ctx, &names)
				return names, err
			},
		},
	}
	for _, query := range queries {
		names, err := query.run()
		if err != nil {
			return storageFolderReferences{}, fmt.Errorf("query %s for orphan reconciliation: %w", query.name, err)
		}
		for _, name := range names {
			if root := topLevelStorageFolder(name); root != "" {
				references.referenced[root] = struct{}{}
				if query.activeUpload {
					references.activeUploads[root] = struct{}{}
				}
			}
		}
	}
	return references, nil
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
