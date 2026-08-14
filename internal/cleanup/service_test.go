package cleanup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagedeletion"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/uploadsession"
	"github.com/stretchr/testify/require"
)

func TestRunUploadsDeletesInactiveUploads(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}, Now: func() time.Time { return now }})
	old := now.Add(-uploadsession.CleanupIdleTimeout - time.Minute).UnixMilli()

	require.NoError(t, filesystem.UploadStream(ctx, "old-upload/parts/0", bytes.NewBufferString("old")))
	require.NoError(t, filesystem.UploadStream(ctx, "active-upload/parts/0", bytes.NewBufferString("active")))
	client.Upload.Create().
		SetID(1).
		SetKey("old").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(old).
		SetFolderName("old-upload").
		SaveX(ctx)
	client.Upload.Create().
		SetID(2).
		SetKey("active").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(old).
		SetLastPartUploadedAt(now.UnixMilli()).
		SetFolderName("active-upload").
		SaveX(ctx)

	deleted, err := service.RunUploads(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	uploads, err := client.Upload.Query().All(ctx)
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	require.Equal(t, int64(2), uploads[0].ID)

	count, err := filesystem.CountFilesInFolder(ctx, "old-upload/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRunStorageDeletionsRetriesFailedUploadCleanup(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	failingStorage := &failingDeleteStorage{Adapter: filesystem}
	metricRecorder := &recordedMetrics{}
	now := time.Now()
	service := NewService(Options{
		DB:      client,
		Storage: failingStorage,
		Config:  config.CleanupConfig{CacheOlderThanDays: 90},
		Metrics: metricRecorder,
		Now:     func() time.Time { return now },
	})
	old := now.Add(-uploadsession.CleanupIdleTimeout - time.Minute).UnixMilli()

	require.NoError(t, filesystem.UploadStream(ctx, "old-upload/parts/0", bytes.NewBufferString("old")))
	client.Upload.Create().
		SetID(1).
		SetKey("old").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(old).
		SetFolderName("old-upload").
		SaveX(ctx)

	deleted, err := service.RunUploads(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, client.Upload.Query().CountX(ctx))
	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, 1, task.AttemptCount)
	require.Equal(t, 1, metricRecorder.storageDeletionFailures)

	service = NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})
	deleted, err = service.RunStorageDeletions(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, 1, client.StorageDeletion.Query().CountX(ctx))

	client.StorageDeletion.UpdateOneID(task.ID).
		SetLastAttemptedAt(time.Now().Add(-storageDeletionRetryDelay(task.AttemptCount) - time.Second).UnixMilli()).
		ExecX(ctx)
	deleted, err = service.RunStorageDeletions(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
	count, err := filesystem.CountFilesInFolder(ctx, "old-upload/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestDeleteUploadIfInactiveSkipsRefreshedUploadAndQueuesNothing(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	currentUpload := client.Upload.Create().
		SetID(1).
		SetKey("old").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(now.Add(-uploadsession.CleanupIdleTimeout - time.Minute).UnixMilli()).
		SetFolderName("old-upload").
		SaveX(ctx)
	staleSnapshot := client.Upload.GetX(ctx, currentUpload.ID)
	client.Upload.UpdateOneID(currentUpload.ID).SetLastPartUploadedAt(now.UnixMilli()).ExecX(ctx)

	result, err := uploadsession.DeleteIfInactive(
		ctx,
		client,
		staleSnapshot.ID,
		staleSnapshot.FolderName,
		now.Add(-uploadsession.CleanupIdleTimeout).UnixMilli(),
	)
	require.NoError(t, err)
	require.False(t, result.Deleted)
	require.Equal(t, currentUpload.ID, client.Upload.Query().OnlyX(ctx).ID)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
}

func TestStorageDeletionRetryBackoff(t *testing.T) {
	require.Equal(t, storageDeletionRetryBase, storageDeletionRetryDelay(0))
	require.Equal(t, storageDeletionRetryBase, storageDeletionRetryDelay(1))
	require.Equal(t, 2*storageDeletionRetryBase, storageDeletionRetryDelay(2))
	require.Equal(t, 4*storageDeletionRetryBase, storageDeletionRetryDelay(3))
	require.Equal(t, storageDeletionRetryMax, storageDeletionRetryDelay(100))

	now := time.Now()
	lastAttemptedAt := now.Add(-2 * storageDeletionRetryBase).UnixMilli()
	task := &ent.StorageDeletion{AttemptCount: 3, LastAttemptedAt: &lastAttemptedAt}
	require.False(t, storageDeletionReady(task, now))
	lastAttemptedAt = now.Add(-4*storageDeletionRetryBase - time.Second).UnixMilli()
	require.True(t, storageDeletionReady(task, now))
}

func TestInterruptedStorageDeletionDoesNotCountFailure(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	metricRecorder := &recordedMetrics{}
	service := NewService(Options{
		DB:      client,
		Storage: &failingDeleteStorage{Adapter: filesystem},
		Config:  config.CleanupConfig{CacheOlderThanDays: 90},
		Metrics: metricRecorder,
	})
	task := client.StorageDeletion.Create().
		SetFolderName("interrupted-folder").
		SetCreatedAt(time.Now().UnixMilli()).
		SaveX(ctx)
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	err := service.processStorageDeletion(canceledCtx, task)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, metricRecorder.storageDeletionFailures)
}

func TestRunCacheEntriesDeletesExpiredLocations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	lifecycle := storagelifecycle.NewWithOptions(client, storagelifecycle.Options{Now: func() time.Time { return now }})
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 30}, Lifecycle: lifecycle, Now: func() time.Time { return now }})
	old := now.Add(-31 * 24 * time.Hour).UnixMilli()

	require.NoError(t, filesystem.UploadStream(ctx, "expired/parts/0", bytes.NewBufferString("data")))
	location := client.StorageLocation.Create().
		SetID("expired-location").
		SetFolderName("expired").
		SetPartCount(1).
		SetLastDownloadedAt(old).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("expired-entry").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(old).
		SetLocation(location).
		SaveX(ctx)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	entryCount, err := client.CacheEntry.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, entryCount)
	pending := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, pending.DeletionRequestedAt)

	now = now.Add(storagelifecycle.DeletionGracePeriod + time.Second)
	finalized, err := service.RunPendingStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	locationCount, err := client.StorageLocation.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, locationCount)
}

func TestRunCacheEntriesUsesSavedAtForNeverDownloadedLocations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * 24 * time.Hour).UnixMilli()
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 30},
		Now:     func() time.Time { return now },
	})

	oldLocation := createRetentionLocation(ctx, client, "never-downloaded-old", nil, cutoff-1)
	recentLocation := createRetentionLocation(ctx, client, "never-downloaded-recent", nil, cutoff)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.NotNil(t, client.StorageLocation.GetX(ctx, oldLocation.ID).DeletionRequestedAt)
	require.Zero(t, client.CacheEntry.Query().Where(cacheentry.LocationId(oldLocation.ID)).CountX(ctx))
	require.Nil(t, client.StorageLocation.GetX(ctx, recentLocation.ID).DeletionRequestedAt)
	require.Equal(t, 1, client.CacheEntry.Query().Where(cacheentry.LocationId(recentLocation.ID)).CountX(ctx))
}

func TestRunCacheEntriesRequiresEverySharedEntryToBeExpired(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * 24 * time.Hour).UnixMilli()
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 30},
		Now:     func() time.Time { return now },
	})
	location := createRetentionLocation(ctx, client, "shared", nil, cutoff-1)
	client.CacheEntry.Create().
		SetID("shared-recent-entry").
		SetKey("shared-recent-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(cutoff).
		SetLocation(location).
		SaveX(ctx)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, 2, client.CacheEntry.Query().Where(cacheentry.LocationId(location.ID)).CountX(ctx))

	client.CacheEntry.UpdateOneID("shared-recent-entry").SetUpdatedAt(cutoff - 1).ExecX(ctx)
	deleted, err = service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, client.CacheEntry.Query().Where(cacheentry.LocationId(location.ID)).CountX(ctx))
}

func TestRunCacheEntriesKeepsRecentlyDownloadedLocation(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * 24 * time.Hour).UnixMilli()
	recent := cutoff
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 30},
		Now:     func() time.Time { return now },
	})
	location := createRetentionLocation(ctx, client, "recently-downloaded", &recent, cutoff-1)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
}

func TestRunCacheEntriesSkipsActiveReader(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	lifecycle := storagelifecycle.NewWithOptions(client, storagelifecycle.Options{Now: func() time.Time { return now }})
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 30}, Lifecycle: lifecycle, Now: func() time.Time { return now }})
	old := now.Add(-31 * 24 * time.Hour).UnixMilli()
	require.NoError(t, filesystem.UploadStream(ctx, "active-reader/parts/0", bytes.NewBufferString("data")))
	location := client.StorageLocation.Create().
		SetID("active-reader-location").
		SetFolderName("active-reader").
		SetPartCount(1).
		SetLastDownloadedAt(old).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("active-reader-entry").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(old).
		SetLocation(location).
		SaveX(ctx)
	lease, err := lifecycle.AcquireReader(ctx, "active-reader-entry", storagelifecycle.AcquireReaderOptions{})
	require.NoError(t, err)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	count, err := filesystem.CountFilesInFolder(ctx, "active-reader/parts")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	require.NoError(t, lifecycle.ReleaseReader(ctx, lease.ID))
	deleted, err = service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)

	now = now.Add(storagelifecycle.DeletionGracePeriod + time.Second)
	finalized, err := service.RunPendingStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	count, err = filesystem.CountFilesInFolder(ctx, "active-reader/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRunCacheEntriesZeroDaysDisablesRetention(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := now.Add(-365 * 24 * time.Hour).UnixMilli()
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 0},
		Now:     func() time.Time { return now },
	})
	location := createRetentionLocation(ctx, client, "cleanup-disabled", &old, old)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	require.Equal(t, 1, client.CacheEntry.Query().Where(cacheentry.LocationId(location.ID)).CountX(ctx))
}

func TestRunCacheEntriesDrainsMoreThanOnePage(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := now.Add(-31 * 24 * time.Hour).UnixMilli()
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 30},
		Now:     func() time.Time { return now },
	})
	for index := 0; index < itemsPerPage+1; index++ {
		createRetentionLocation(ctx, client, fmt.Sprintf("paged-%02d", index), nil, old)
	}

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, itemsPerPage+1, deleted)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
}

func TestRunStorageLocationsDeletesOrphans(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	lifecycle := storagelifecycle.NewWithOptions(client, storagelifecycle.Options{Now: func() time.Time { return now }})
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}, Lifecycle: lifecycle})

	require.NoError(t, filesystem.UploadStream(ctx, "orphan/parts/0", bytes.NewBufferString("data")))
	client.StorageLocation.Create().
		SetID("orphan-location").
		SetFolderName("orphan").
		SetPartCount(1).
		SaveX(ctx)

	deleted, err := service.RunStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	pending := client.StorageLocation.Query().OnlyX(ctx)
	require.NotNil(t, pending.DeletionRequestedAt)

	now = now.Add(storagelifecycle.DeletionGracePeriod + time.Second)
	finalized, err := service.RunPendingStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	locationCount, err := client.StorageLocation.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, locationCount)
}

func TestRunOrphanedStorageQueuesOnlyExpiredUnreferencedFolders(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := now.Add(-25 * time.Hour)
	recent := now.Add(-23 * time.Hour)
	for _, folderName := range []string{"old-orphan", "recent-orphan", "upload", "location", "pending"} {
		require.NoError(t, filesystem.UploadStream(ctx, folderName+"/parts/0", bytes.NewBufferString("data")))
	}

	client.Upload.Create().
		SetID(100).
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(now.UnixMilli()).
		SetFolderName("upload").
		SaveX(ctx)
	client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("location").
		SetPartCount(1).
		SaveX(ctx)
	client.StorageDeletion.Create().
		SetFolderName("pending/blocks").
		SetCreatedAt(now.UnixMilli()).
		SaveX(ctx)

	adapter := newOrphanTestStorage(filesystem, map[string]time.Time{
		"old-orphan":    old,
		"recent-orphan": recent,
		"upload":        old,
		"location":      old,
		"pending":       old,
	})
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, queued)
	require.Zero(t, adapter.inspectCount("upload"))
	require.Zero(t, adapter.inspectCount("location"))
	require.Zero(t, adapter.inspectCount("pending"))

	task := client.StorageDeletion.Query().
		Where(storagedeletion.FolderName("old-orphan")).
		OnlyX(ctx)
	contents, err := filesystem.InspectFolderSummary(ctx, "old-orphan")
	require.NoError(t, err)
	require.True(t, contents.Exists, "orphan discovery must not delete storage inline")

	require.NoError(t, service.processStorageDeletion(ctx, task))
	contents, err = filesystem.InspectFolderSummary(ctx, "old-orphan")
	require.NoError(t, err)
	require.False(t, contents.Exists)
	require.Equal(t, 1, client.StorageDeletion.Query().CountX(ctx))
}

func TestRunOrphanedStorageRechecksRecentActivityAndDatabaseAuthorization(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-time.Hour)
	adapter := newOrphanTestStorage(filesystem, map[string]time.Time{
		"became-authorized": old,
		"recently-active":   old,
	})
	adapter.inspections["recently-active"] = storage.FolderSummary{
		FolderName:       "recently-active",
		Exists:           true,
		NewestModifiedAt: recent,
	}
	adapter.onInspect = func(folderName string) {
		if folderName != "became-authorized" {
			return
		}
		client.Upload.Create().
			SetID(101).
			SetKey("key").
			SetVersion("version").
			SetScope("scope").
			SetRepoId("repo").
			SetCreatedAt(now.UnixMilli()).
			SetFolderName(folderName).
			SaveX(ctx)
	}
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.NoError(t, err)
	require.Zero(t, queued)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
	require.Equal(t, 1, client.Upload.Query().CountX(ctx))
}

func TestRunOrphanedStorageFailsClosedBeforeFirstFolder(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	require.NoError(t, filesystem.UploadStream(ctx, "orphan/parts/0", bytes.NewBufferString("data")))
	adapter := newOrphanTestStorage(filesystem, map[string]time.Time{
		"orphan": time.Now().Add(-48 * time.Hour),
	})
	adapter.walkErr = errors.New("incomplete folder walk")
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.ErrorContains(t, err, "incomplete folder walk")
	require.Zero(t, queued)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
	contents, inspectErr := filesystem.InspectFolderSummary(ctx, "orphan")
	require.NoError(t, inspectErr)
	require.True(t, contents.Exists)
}

func TestRunOrphanedStorageKeepsCompletedBatchesWhenWalkLaterFails(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	folders := make(map[string]time.Time, orphanFolderBatchSize+1)
	for index := 0; index <= orphanFolderBatchSize; index++ {
		folders[fmt.Sprintf("folder-%03d", index)] = now.Add(-48 * time.Hour)
	}
	adapter := newOrphanTestStorage(filesystem, folders)
	adapter.walkErr = errors.New("later page failed")
	adapter.walkAfter = orphanFolderBatchSize
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.ErrorContains(t, err, "later page failed")
	require.Equal(t, orphanFolderBatchSize, queued)
	require.Equal(t, orphanFolderBatchSize, client.StorageDeletion.Query().CountX(ctx))
	require.Zero(t, adapter.inspectCount(fmt.Sprintf("folder-%03d", orphanFolderBatchSize)))
}

func TestRunOrphanedStorageFinalClaimFencesUnknownNestedDeletion(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	client.StorageDeletion.Create().
		SetFolderName("folder/future-layout").
		SetCreatedAt(now.UnixMilli()).
		SaveX(ctx)
	adapter := newOrphanTestStorage(filesystem, map[string]time.Time{
		"folder": now.Add(-48 * time.Hour),
	})
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.NoError(t, err)
	require.Zero(t, queued)
	require.Equal(t, 1, adapter.inspectCount("folder"), "the exact-name prefilter may miss future suffixes")
	require.Equal(t, 1, client.StorageDeletion.Query().CountX(ctx), "the transactional prefix fence must prevent a second claim")
}

func TestRunOrphanedStorageAggregatesCandidateFailures(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := now.Add(-48 * time.Hour)
	adapter := newOrphanTestStorage(filesystem, map[string]time.Time{
		"broken": old,
		"valid":  old,
	})
	adapter.inspectErrors["broken"] = errors.New("injected inspect failure")
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.ErrorContains(t, err, "injected inspect failure")
	require.Equal(t, 1, queued)
	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, "valid", task.FolderName)
}

func TestRunOrphanedStorageSweepsExpiredTemporaryUploadsFromLocationFolder(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	root := t.TempDir()
	filesystem, err := storage.NewFilesystemAdapter(root)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)

	require.NoError(t, filesystem.UploadStream(ctx, "location/parts/0", bytes.NewBufferString("committed")))
	partsPath := filepath.Join(root, "location", "parts")
	stalePath := filepath.Join(partsPath, ".upload-stale")
	recentPath := filepath.Join(partsPath, ".upload-recent")
	require.NoError(t, os.WriteFile(stalePath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(recentPath, []byte("recent"), 0o644))
	stale := now.Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(stalePath, stale, stale))

	client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("location").
		SetPartCount(1).
		SaveX(ctx)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.NoError(t, err)
	require.Zero(t, queued)
	require.NoFileExists(t, stalePath)
	require.FileExists(t, recentPath)
	committed, err := os.ReadFile(filepath.Join(partsPath, "0"))
	require.NoError(t, err)
	require.Equal(t, "committed", string(committed))
}

func TestRunOrphanedStorageRetainsStalledTemporaryUploadInActiveFolder(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	root := t.TempDir()
	filesystem, err := storage.NewFilesystemAdapter(root)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)

	require.NoError(t, filesystem.UploadStream(ctx, "shared/parts/0", bytes.NewBufferString("committed")))
	stalledPath := filepath.Join(root, "shared", "parts", ".upload-stalled")
	require.NoError(t, os.WriteFile(stalledPath, []byte("still open elsewhere"), 0o644))
	stalled := now.Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(stalledPath, stalled, stalled))
	stalledFile, err := os.OpenFile(stalledPath, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stalledFile.Close()) })

	client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("shared").
		SetPartCount(1).
		SaveX(ctx)
	client.Upload.Create().
		SetID(100).
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(now.UnixMilli()).
		SetFolderName("shared").
		SaveX(ctx)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Config: config.CleanupConfig{
			OrphanedStorageGracePeriod: 24 * time.Hour,
		},
		Now: func() time.Time { return now },
	})

	queued, err := service.RunOrphanedStorage(ctx)
	require.NoError(t, err)
	require.Zero(t, queued)
	require.FileExists(t, stalledPath)
	committed, err := os.ReadFile(filepath.Join(root, "shared", "parts", "0"))
	require.NoError(t, err)
	require.Equal(t, "committed", string(committed))
}

func TestActiveUploadFolderBatchPaginatesBoundedMatches(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	now := time.Now().UnixMilli()
	for index := 0; index < activeUploadQueryPageSize; index++ {
		client.Upload.Create().
			SetID(int64(index + 1)).
			SetKey(fmt.Sprintf("key-a-%03d", index)).
			SetVersion("version").
			SetScope("scope").
			SetRepoId("repo").
			SetCreatedAt(now).
			SetFolderName(fmt.Sprintf("root-a/session-%03d", index)).
			SaveX(ctx)
	}
	client.Upload.Create().
		SetID(int64(activeUploadQueryPageSize + 1)).
		SetKey("key-z").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(now).
		SetFolderName("root-z/session").
		SaveX(ctx)
	service := NewService(Options{DB: client})

	active, err := service.activeUploadFolderBatch(ctx, []storage.ObjectMetadata{
		{Name: "root-a/parts/.upload-a"},
		{Name: "root-z/parts/.upload-z"},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"root-a": {},
		"root-z": {},
	}, active)
}

func TestReferencedStorageFolderBatchCollapsesDuplicateDeletionRows(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	for index := 0; index < orphanFolderBatchSize+25; index++ {
		client.StorageDeletion.Create().
			SetFolderName("folder/parts").
			SetCreatedAt(int64(index)).
			SaveX(ctx)
	}
	service := NewService(Options{DB: client})

	references, err := service.referencedStorageFolderBatch(ctx, []string{"folder"})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"folder": {}}, references)
}

func TestRunPartsDeletesMergedParts(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})

	require.NoError(t, filesystem.UploadStream(ctx, "merged/parts/0", bytes.NewBufferString("one")))
	require.NoError(t, filesystem.UploadStream(ctx, "merged/parts/1", bytes.NewBufferString("two")))
	location := client.StorageLocation.Create().
		SetID("merged-location").
		SetFolderName("merged").
		SetPartCount(2).
		SetMergedAt(time.Now().Add(-materializedPartsRetention - time.Minute).UnixMilli()).
		SaveX(ctx)

	deleted, err := service.RunParts(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.PartsDeletedAt)
	count, err := filesystem.CountFilesInFolder(ctx, "merged/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestRunPartsKeepsRecentlySupersededParts(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})

	require.NoError(t, filesystem.UploadStream(ctx, "recent/parts/0", bytes.NewBufferString("data")))
	location := client.StorageLocation.Create().
		SetID("recent-location").
		SetFolderName("recent").
		SetPartCount(1).
		SetMergedAt(time.Now().UnixMilli()).
		SaveX(ctx)

	deleted, err := service.RunParts(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.Nil(t, current.PartsDeletedAt)
	count, err := filesystem.CountFilesInFolder(ctx, "recent/parts")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

type failingDeleteStorage struct {
	storage.Adapter
}

type recordedMetrics struct {
	storageDeletionFailures int
}

func (*recordedMetrics) RecordCacheRequest(bool) {}

func (*recordedMetrics) RecordCacheUpload() {}

func (r *recordedMetrics) RecordStorageDeletionFailure() {
	r.storageDeletionFailures++
}

func (*failingDeleteStorage) DeleteFolder(context.Context, string) error {
	return errors.New("injected delete failure")
}

type orphanTestStorage struct {
	storage.Adapter
	folders       []string
	walkErr       error
	walkAfter     int
	inspections   map[string]storage.FolderSummary
	inspectErrors map[string]error
	onInspect     func(string)
	mu            sync.Mutex
	inspectCalls  map[string]int
}

func newOrphanTestStorage(adapter storage.Adapter, modifiedAtByFolder map[string]time.Time) *orphanTestStorage {
	testStorage := &orphanTestStorage{
		Adapter:       adapter,
		inspections:   make(map[string]storage.FolderSummary),
		inspectErrors: make(map[string]error),
		inspectCalls:  make(map[string]int),
	}
	for folderName, modifiedAt := range modifiedAtByFolder {
		testStorage.folders = append(testStorage.folders, folderName)
		testStorage.inspections[folderName] = storage.FolderSummary{
			FolderName:       folderName,
			Exists:           true,
			NewestModifiedAt: modifiedAt,
		}
	}
	slices.Sort(testStorage.folders)
	return testStorage
}

func (s *orphanTestStorage) WalkTopLevelFolders(_ context.Context, visit func(string) error) error {
	if s.walkErr != nil && s.walkAfter == 0 {
		return s.walkErr
	}
	for index, folderName := range s.folders {
		if err := visit(folderName); err != nil {
			return err
		}
		if s.walkErr != nil && index+1 == s.walkAfter {
			return s.walkErr
		}
	}
	return nil
}

func (s *orphanTestStorage) InspectFolderSummary(_ context.Context, folderName string) (storage.FolderSummary, error) {
	s.mu.Lock()
	s.inspectCalls[folderName]++
	s.mu.Unlock()
	if s.onInspect != nil {
		s.onInspect(folderName)
	}
	if err := s.inspectErrors[folderName]; err != nil {
		return storage.FolderSummary{}, err
	}
	return s.inspections[folderName], nil
}

func (s *orphanTestStorage) inspectCount(folderName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inspectCalls[folderName]
}

func createRetentionLocation(ctx context.Context, client *ent.Client, id string, lastDownloadedAt *int64, updatedAt int64) *ent.StorageLocation {
	location := client.StorageLocation.Create().
		SetID(id + "-location").
		SetFolderName(id + "-folder").
		SetPartCount(1).
		SetNillableLastDownloadedAt(lastDownloadedAt).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID(id + "-entry").
		SetKey(id + "-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(updatedAt).
		SetLocation(location).
		SaveX(ctx)
	return location
}
