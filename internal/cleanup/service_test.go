package cleanup

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRunUploadsDeletesInactiveUploads(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})
	now := time.Now().UnixMilli()
	old := time.Now().Add(-2 * time.Minute).UnixMilli()

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
		SetLastPartUploadedAt(now).
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
	service := NewService(Options{DB: client, Storage: failingStorage, Config: config.CleanupConfig{CacheOlderThanDays: 90}})
	old := time.Now().Add(-2 * time.Minute).UnixMilli()

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

func TestRunCacheEntriesDeletesExpiredLocations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	lifecycle := storagelifecycle.NewWithOptions(client, storagelifecycle.Options{Now: func() time.Time { return now }})
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 30}, Lifecycle: lifecycle})
	old := time.Now().Add(-31 * 24 * time.Hour).UnixMilli()

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
		SetUpdatedAt(time.Now().UnixMilli()).
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

func TestRunCacheEntriesFencesActiveReaderBeforePhysicalDeletion(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	lifecycle := storagelifecycle.NewWithOptions(client, storagelifecycle.Options{Now: func() time.Time { return now }})
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 30}, Lifecycle: lifecycle})
	require.NoError(t, filesystem.UploadStream(ctx, "active-reader/parts/0", bytes.NewBufferString("data")))
	location := client.StorageLocation.Create().
		SetID("active-reader-location").
		SetFolderName("active-reader").
		SetPartCount(1).
		SetLastDownloadedAt(time.Now().Add(-31 * 24 * time.Hour).UnixMilli()).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("active-reader-entry").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)
	lease, err := lifecycle.AcquireReader(ctx, "active-reader-entry", storagelifecycle.AcquireReaderOptions{})
	require.NoError(t, err)

	deleted, err := service.RunCacheEntries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	count, err := filesystem.CountFilesInFolder(ctx, "active-reader/parts")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	require.NoError(t, lifecycle.ReleaseReader(ctx, lease.ID))
	now = now.Add(storagelifecycle.DeletionGracePeriod + time.Second)
	finalized, err := service.RunPendingStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	count, err = filesystem.CountFilesInFolder(ctx, "active-reader/parts")
	require.NoError(t, err)
	require.Zero(t, count)
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

func (*failingDeleteStorage) DeleteFolder(context.Context, string) error {
	return errors.New("injected delete failure")
}
