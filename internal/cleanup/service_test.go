package cleanup

import (
	"bytes"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
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

func TestRunCacheEntriesDeletesExpiredLocations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 30}})
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
	locationCount, err := client.StorageLocation.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, locationCount)
}

func TestRunStorageLocationsDeletesOrphans(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})

	require.NoError(t, filesystem.UploadStream(ctx, "orphan/parts/0", bytes.NewBufferString("data")))
	client.StorageLocation.Create().
		SetID("orphan-location").
		SetFolderName("orphan").
		SetPartCount(1).
		SaveX(ctx)

	deleted, err := service.RunStorageLocations(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

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
		SetMergedAt(time.Now().UnixMilli()).
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

func TestRunMergesResetsStalledMerges(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	service := NewService(Options{DB: client, Storage: filesystem, Config: config.CleanupConfig{CacheOlderThanDays: 90}})
	old := time.Now().Add(-16 * time.Minute).UnixMilli()

	location := client.StorageLocation.Create().
		SetID("stalled-location").
		SetFolderName("stalled").
		SetPartCount(1).
		SetMergeStartedAt(old).
		SaveX(ctx)

	updated, err := service.RunMerges(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, updated)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.Nil(t, current.MergeStartedAt)
	require.Nil(t, current.MergedAt)
}
