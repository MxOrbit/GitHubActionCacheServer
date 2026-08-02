package uploadsession

import (
	"context"
	"fmt"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
)

const (
	// TakeoverIdleTimeout is intentionally more aggressive than passive cleanup:
	// a new reservation for the same tuple is direct evidence of demand.
	TakeoverIdleTimeout = 5 * time.Minute
	// CleanupIdleTimeout must remain at least TakeoverIdleTimeout so background
	// cleanup is no more aggressive than an explicit reservation takeover.
	CleanupIdleTimeout = 15 * time.Minute
)

// DeleteResult reports whether a conditional deletion committed and carries
// the durable outbox task for callers that process deletions inline.
type DeleteResult struct {
	Deleted bool
	Task    *ent.StorageDeletion
}

// Inactive returns the canonical idle-session predicate used by both candidate
// selection and fenced deletion.
func Inactive(cutoff int64) []predicate.Upload {
	return []predicate.Upload{
		upload.CreatedAtLT(cutoff),
		upload.Or(
			upload.LastPartUploadedAtIsNil(),
			upload.LastPartUploadedAtLT(cutoff),
		),
	}
}

// InactiveByID scopes the canonical idle-session predicate to one upload.
func InactiveByID(uploadID, cutoff int64) []predicate.Upload {
	predicates := []predicate.Upload{upload.ID(uploadID)}
	return append(predicates, Inactive(cutoff)...)
}

// DeleteIfInactive atomically deletes an idle upload and enqueues its folder.
// A concurrent activity refresh or deletion returns a zero DeleteResult.
func DeleteIfInactive(ctx context.Context, db *ent.Client, uploadID int64, folderName string, cutoff int64) (DeleteResult, error) {
	tx, err := db.Tx(ctx)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("start inactive upload deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	deleted, err := tx.Upload.Delete().
		Where(InactiveByID(uploadID, cutoff)...).
		Exec(ctx)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("delete inactive upload: %w", err)
	}
	if deleted == 0 {
		return DeleteResult{}, nil
	}

	task, err := storageoutbox.Enqueue(ctx, tx.Client(), folderName)
	if err != nil {
		return DeleteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeleteResult{}, fmt.Errorf("commit inactive upload deletion: %w", err)
	}
	committed = true
	return DeleteResult{Deleted: true, Task: task}, nil
}
