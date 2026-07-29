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
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
)

func (s *Service) RunOrphanedStorage(ctx context.Context) (int, error) {
	inventory, err := s.storage.Inventory(ctx)
	if err != nil {
		return 0, fmt.Errorf("inventory storage for orphan reconciliation: %w", err)
	}

	referencedFolders, err := s.referencedStorageFolders(ctx)
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
		if _, ok := referencedFolders[folder.FolderName]; ok {
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
	return queued, errors.Join(reconcileErrors...)
}

func (s *Service) referencedStorageFolders(ctx context.Context) (map[string]struct{}, error) {
	referenced := make(map[string]struct{})
	queries := []struct {
		name string
		run  func() ([]string, error)
	}{
		{
			name: "uploads",
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
			return nil, fmt.Errorf("query %s for orphan reconciliation: %w", query.name, err)
		}
		for _, name := range names {
			if root := topLevelStorageFolder(name); root != "" {
				referenced[root] = struct{}{}
			}
		}
	}
	return referenced, nil
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
