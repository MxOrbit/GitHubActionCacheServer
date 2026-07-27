package storageoutbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
)

func Enqueue(ctx context.Context, db *ent.Client, folderName string) (*ent.StorageDeletion, error) {
	task, err := db.StorageDeletion.Create().
		SetFolderName(folderName).
		SetCreatedAt(time.Now().UnixMilli()).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("enqueue storage folder deletion: %w", err)
	}
	return task, nil
}

func Process(ctx context.Context, db *ent.Client, adapter storage.Adapter, task *ent.StorageDeletion) error {
	if err := adapter.DeleteFolder(ctx, task.FolderName); err != nil {
		deleteErr := fmt.Errorf("delete storage folder %q: %w", task.FolderName, err)
		_, updateErr := db.StorageDeletion.UpdateOneID(task.ID).
			AddAttemptCount(1).
			SetLastAttemptedAt(time.Now().UnixMilli()).
			SetLastError(deleteErr.Error()).
			Save(ctx)
		if updateErr != nil && !ent.IsNotFound(updateErr) {
			return errors.Join(deleteErr, fmt.Errorf("record storage deletion failure: %w", updateErr))
		}
		return deleteErr
	}

	if err := db.StorageDeletion.DeleteOneID(task.ID).Exec(ctx); err != nil && !ent.IsNotFound(err) {
		return fmt.Errorf("complete storage folder deletion %q: %w", task.FolderName, err)
	}
	return nil
}
