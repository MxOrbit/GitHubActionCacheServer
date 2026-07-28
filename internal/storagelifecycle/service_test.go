package storagelifecycle

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagereaderlease"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestReaderLeaseFencesDeletionAcrossServiceInstancesUntilCrashExpiry(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "cache.db")
	dbConfig := config.DBConfig{Driver: db.DriverSQLite, SQLitePath: databasePath}
	readerClient, err := db.OpenAndMigrate(ctx, dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readerClient.Close()) })
	deletionClient, err := db.OpenAndMigrate(ctx, dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, deletionClient.Close()) })
	now := time.Now().Truncate(time.Millisecond)
	options := Options{Now: func() time.Time { return now }}
	readerService := NewWithOptions(readerClient, options)
	deletionService := NewWithOptions(deletionClient, options)
	location := createLifecycleEntry(ctx, readerClient, "entry", "location", 2)

	lease, err := readerService.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeParts, lease.Scope)

	result, err := deletionService.RequestLocationDeletion(ctx, location.ID, true, false)
	require.NoError(t, err)
	require.True(t, result.Fenced)
	require.False(t, result.Finalized)
	require.Nil(t, result.Task)
	require.Zero(t, readerClient.CacheEntry.Query().CountX(ctx))
	require.NotNil(t, readerClient.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	require.Equal(t, 1, readerClient.StorageReaderLease.Query().CountX(ctx))

	_, err = readerService.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.ErrorIs(t, err, ErrLocationUnavailable)

	now = now.Add(DeletionGracePeriod + ReaderLeaseDuration + time.Second)
	result, err = deletionService.RequestLocationDeletion(ctx, location.ID, false, true)
	require.NoError(t, err)
	require.True(t, result.Finalized)
	require.NotNil(t, result.Task)
	require.Zero(t, readerClient.StorageReaderLease.Query().CountX(ctx))
	_, err = readerClient.StorageLocation.Get(ctx, location.ID)
	require.True(t, ent.IsNotFound(err))
}

func TestAcquireReaderBacksOffBeforeRetryingFencedReplacement(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	oldLocation := createLifecycleEntry(ctx, client, "entry", "old-location", 1)
	newLocation := client.StorageLocation.Create().
		SetID("new-location").
		SetFolderName("new-location-folder").
		SetPartCount(1).
		SaveX(ctx)
	client.StorageLocation.UpdateOneID(oldLocation.ID).
		SetDeletionRequestedAt(time.Now().UnixMilli()).
		ExecX(ctx)
	service := NewWithOptions(client, Options{ReaderAcquireRetryBackoff: 25 * time.Millisecond})

	updated := make(chan error, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		updated <- client.CacheEntry.UpdateOneID("entry").SetLocation(newLocation).Exec(ctx)
	}()

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, <-updated)
	require.NoError(t, err)
	require.Equal(t, newLocation.ID, lease.Location.ID)
}

func TestAcquireReaderBackoffHonorsContextCancellation(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)
	client.StorageLocation.UpdateOneID(location.ID).
		SetDeletionRequestedAt(time.Now().UnixMilli()).
		ExecX(ctx)
	service := NewWithOptions(client, Options{ReaderAcquireRetryBackoff: time.Second})
	canceledCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err := service.AcquireReader(canceledCtx, "entry", AcquireReaderOptions{})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(startedAt), 500*time.Millisecond)
}

func TestDirectReaderLeaseProtectsFullSignedURLTTL(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{Direct: true})
	require.NoError(t, err)
	require.Equal(t, now.Add(DirectDownloadLeaseDuration), lease.ExpiresAt)
	require.Equal(t, storagereaderlease.ScopeParts, lease.Scope)
	now = now.Add(30 * time.Second)
	require.NoError(t, service.ExtendDirectReader(ctx, lease.ID))
	persisted := client.StorageReaderLease.GetX(ctx, lease.ID)
	require.Equal(t, now.Add(DirectDownloadLeaseDuration).UnixMilli(), persisted.ExpiresAt)
	result, err := service.RequestLocationDeletion(ctx, location.ID, true, false)
	require.NoError(t, err)
	require.False(t, result.Finalized)

	now = now.Add(DirectDownloadLeaseDuration - time.Second)
	result, err = service.RequestLocationDeletion(ctx, location.ID, false, true)
	require.NoError(t, err)
	require.False(t, result.Finalized)

	now = now.Add(2 * time.Second)
	result, err = service.RequestLocationDeletion(ctx, location.ID, false, true)
	require.NoError(t, err)
	require.True(t, result.Finalized)
}

func TestPurgeDanglingCacheEntryIsConditionalAndFencesOnlyChildlessLocation(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	location := createLifecycleEntry(ctx, client, "entry-a", "location", 1)
	client.CacheEntry.Create().
		SetID("entry-b").
		SetKey("key-b").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)
	replacement := client.StorageLocation.Create().
		SetID("replacement").
		SetFolderName("replacement-folder").
		SetPartCount(1).
		SaveX(ctx)

	client.CacheEntry.UpdateOneID("entry-a").SetLocation(replacement).ExecX(ctx)
	deleted, err := service.PurgeDanglingCacheEntry(ctx, "entry-a", location.ID)
	require.NoError(t, err)
	require.False(t, deleted)
	require.Equal(t, replacement.ID, client.CacheEntry.GetX(ctx, "entry-a").LocationId)

	deleted, err = service.PurgeDanglingCacheEntry(ctx, "entry-b", location.ID)
	require.NoError(t, err)
	require.True(t, deleted)
	_, err = client.CacheEntry.Get(ctx, "entry-b")
	require.True(t, ent.IsNotFound(err))
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
}

func TestPurgeDanglingCacheEntryLeavesSharedLocationAvailable(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	location := createLifecycleEntry(ctx, client, "entry-a", "location", 1)
	client.CacheEntry.Create().
		SetID("entry-b").
		SetKey("key-b").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)

	deleted, err := service.PurgeDanglingCacheEntry(ctx, "entry-a", location.ID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	require.Equal(t, location.ID, client.CacheEntry.GetX(ctx, "entry-b").LocationId)
}

func TestPartsDeletionWaitsForPartsReadersButNotMergedReaders(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "location", 2)

	partsLease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeParts, partsLease.Scope)
	client.StorageLocation.UpdateOneID(location.ID).SetMergedAt(now.UnixMilli()).ExecX(ctx)
	mergedLease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{Direct: true})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeStorage, mergedLease.Scope)

	result, err := service.ClaimPartsDeletion(ctx, location.ID)
	require.NoError(t, err)
	require.Nil(t, result.Task)
	require.NoError(t, service.ReleaseReader(ctx, partsLease.ID))

	result, err = service.ClaimPartsDeletion(ctx, location.ID)
	require.NoError(t, err)
	require.NotNil(t, result.Task)
	require.Equal(t, 2, result.PartCount)
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).PartsDeletedAt)
	require.NoError(t, service.ReleaseReader(ctx, mergedLease.ID))
}

func TestReaderLeaseForeignKeyPreventsDeletingDurableEvidence(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)
	_, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)

	client.CacheEntry.DeleteOneID("entry").ExecX(ctx)
	err = client.StorageLocation.DeleteOneID(location.ID).Exec(ctx)
	require.Error(t, err)
	require.Equal(t, 1, client.StorageReaderLease.Query().CountX(ctx))
}

func TestMaterializationLeaseRenewsAndFencesOwners(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	options := Options{Now: func() time.Time { return now }}
	first := NewWithOptions(client, options)
	second := NewWithOptions(client, options)
	location := createLifecycleEntry(ctx, client, "entry", "location", 2)

	lease, err := first.AcquireMaterialization(ctx, location.ID)
	require.NoError(t, err)
	_, err = second.AcquireMaterialization(ctx, location.ID)
	require.ErrorIs(t, err, ErrMaterializationLeaseHeld)

	now = now.Add(time.Minute)
	require.NoError(t, first.RenewMaterialization(ctx, location.ID, lease.Token))
	now = now.Add(MaterializationLeaseDuration - time.Second)
	_, err = second.AcquireMaterialization(ctx, location.ID)
	require.ErrorIs(t, err, ErrMaterializationLeaseHeld)

	require.NoError(t, first.FinishMaterialization(ctx, location.ID, lease.Token))
	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Nil(t, current.MergeLeaseToken)
	require.Nil(t, current.MergeLeaseExpiresAt)
}

func TestRecentLegacyMaterializationIsProtectedDuringUpgrade(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "location", 2)
	client.StorageLocation.UpdateOneID(location.ID).SetMergeStartedAt(now.UnixMilli()).ExecX(ctx)

	_, err := service.AcquireMaterialization(ctx, location.ID)
	require.ErrorIs(t, err, ErrMaterializationLeaseHeld)

	now = now.Add(legacyMaterializationLifetime + time.Second)
	lease, err := service.AcquireMaterialization(ctx, location.ID)
	require.NoError(t, err)
	require.NotEmpty(t, lease.Token)
}

func createLifecycleEntry(ctx context.Context, client *ent.Client, entryID, locationID string, partCount int) *ent.StorageLocation {
	location := client.StorageLocation.Create().
		SetID(locationID).
		SetFolderName(locationID + "-folder").
		SetPartCount(partCount).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID(entryID).
		SetKey(entryID + "-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)
	return location
}
