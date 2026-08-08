package storagelifecycle

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
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

func TestReleaseReadersDeletesBatchAndIgnoresMissingLeases(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	createLifecycleEntry(ctx, client, "first-entry", "first-location", 1)
	createLifecycleEntry(ctx, client, "second-entry", "second-location", 1)
	first, err := service.AcquireReader(ctx, "first-entry", AcquireReaderOptions{})
	require.NoError(t, err)
	second, err := service.AcquireReader(ctx, "second-entry", AcquireReaderOptions{})
	require.NoError(t, err)

	deleted, err := service.ReleaseReaders(ctx, []string{first.ID, "missing-lease", second.ID})

	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
}

func TestConcurrentReaderLeasesPreserveVersionAndAllowLookups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	const readers = 32
	// PartCount 2 keeps the location mergeable, so acquisitions still take the
	// row lock this test pins.
	location := client.StorageLocation.Create().
		SetID("shared-location").
		SetFolderName("shared-folder").
		SetPartCount(2).
		SaveX(ctx)
	for index := range readers {
		client.CacheEntry.Create().
			SetID(fmt.Sprintf("entry-%02d", index)).
			SetKey(fmt.Sprintf("key-%02d", index)).
			SetVersion("version").
			SetScope("scope").
			SetRepoId("repo").
			SetUpdatedAt(time.Now().UnixMilli()).
			SetLocation(location).
			SaveX(ctx)
	}

	start := make(chan struct{})
	errs := make(chan error, readers*2)
	var wg sync.WaitGroup
	for index := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := service.AcquireReader(ctx, fmt.Sprintf("entry-%02d", index), AcquireReaderOptions{})
			if err != nil {
				errs <- err
				return
			}
			if _, err := client.CacheEntry.Query().Where(cacheentry.ID(fmt.Sprintf("entry-%02d", index))).Only(ctx); err != nil {
				errs <- err
			}
			if err := service.ReleaseReader(ctx, lease.ID); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
	require.Equal(t, int64(readers), client.StorageLocation.GetX(ctx, location.ID).LeaseVersion)
}

func TestMergedReaderLeaseSkipsRowWriteAndFencesDeletion(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)
	client.StorageLocation.UpdateOneID(location.ID).SetMergedAt(now.UnixMilli()).ExecX(ctx)
	versionBefore := client.StorageLocation.GetX(ctx, location.ID).LeaseVersion

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeStorage, lease.Scope)
	require.Equal(t, versionBefore, client.StorageLocation.GetX(ctx, location.ID).LeaseVersion)

	result, err := service.RequestLocationDeletion(ctx, location.ID, true, false)
	require.NoError(t, err)
	require.True(t, result.Fenced)
	require.False(t, result.Finalized)

	for step := 0; step < int(DeletionGracePeriod/time.Minute)+1; step++ {
		now = now.Add(time.Minute)
		require.NoError(t, service.RenewReader(ctx, lease.ID))
	}
	result, err = service.RequestLocationDeletion(ctx, location.ID, false, true)
	require.NoError(t, err)
	require.False(t, result.Finalized)

	require.NoError(t, service.ReleaseReader(ctx, lease.ID))
	result, err = service.RequestLocationDeletion(ctx, location.ID, false, true)
	require.NoError(t, err)
	require.True(t, result.Finalized)
}

func TestAcquireReaderSkipsFencedMergedLocation(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := NewWithOptions(client, Options{ReaderAcquireRetryBackoff: time.Millisecond})
	now := time.Now()
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)
	client.StorageLocation.UpdateOneID(location.ID).
		SetMergedAt(now.UnixMilli()).
		SetDeletionRequestedAt(now.UnixMilli()).
		ExecX(ctx)

	_, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.ErrorIs(t, err, ErrLocationUnavailable)
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
	require.Zero(t, client.StorageLocation.GetX(ctx, location.ID).LeaseVersion)
}

func TestUnmergeableLocationsSkipRowWrite(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now()
	service := New(client)

	singlePart := createLifecycleEntry(ctx, client, "single-entry", "single-location", 1)
	unsupported := createLifecycleEntry(ctx, client, "unsupported-entry", "unsupported-location", 2)
	client.StorageLocation.UpdateOneID(unsupported.ID).
		SetMaterializationUnsupportedAt(now.UnixMilli()).
		ExecX(ctx)

	lease, err := service.AcquireReader(ctx, "single-entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeParts, lease.Scope)
	lease, err = service.AcquireReader(ctx, "unsupported-entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeParts, lease.Scope)

	require.Zero(t, client.StorageLocation.GetX(ctx, singlePart.ID).LeaseVersion)
	require.Zero(t, client.StorageLocation.GetX(ctx, unsupported.ID).LeaseVersion)
}

func TestMaterializationDisabledReadersSkipRowWrite(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := NewWithOptions(client, Options{MaterializationDisabled: true})
	location := createLifecycleEntry(ctx, client, "entry", "location", 2)

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	require.Equal(t, storagereaderlease.ScopeParts, lease.Scope)
	require.Zero(t, client.StorageLocation.GetX(ctx, location.ID).LeaseVersion)
}

func TestReaderLeaseInsertConstraintIsClassified(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)

	_, err := client.StorageReaderLease.Create().
		SetID("orphan-lease").
		SetStorageLocationId("missing-location").
		SetScope(storagereaderlease.ScopeStorage).
		SetExpiresAt(time.Now().Add(time.Minute).UnixMilli()).
		Save(ctx)
	require.True(t, ent.IsConstraintError(err))
}

func TestConcurrentMergedReaderLeasesSkipLocationWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	const readers = 32
	location := client.StorageLocation.Create().
		SetID("shared-location").
		SetFolderName("shared-folder").
		SetPartCount(1).
		SetMergedAt(time.Now().UnixMilli()).
		SaveX(ctx)
	for index := range readers {
		client.CacheEntry.Create().
			SetID(fmt.Sprintf("entry-%02d", index)).
			SetKey(fmt.Sprintf("key-%02d", index)).
			SetVersion("version").
			SetScope("scope").
			SetRepoId("repo").
			SetUpdatedAt(time.Now().UnixMilli()).
			SetLocation(location).
			SaveX(ctx)
	}

	start := make(chan struct{})
	errs := make(chan error, readers*2)
	var wg sync.WaitGroup
	for index := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := service.AcquireReader(ctx, fmt.Sprintf("entry-%02d", index), AcquireReaderOptions{})
			if err != nil {
				errs <- err
				return
			}
			if err := service.ReleaseReader(ctx, lease.ID); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
	require.Zero(t, client.StorageLocation.GetX(ctx, location.ID).LeaseVersion)
}

func TestExpiredLocationDeletionRechecksAccessAndReaderLease(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	cutoff := now.Add(-30 * 24 * time.Hour).UnixMilli()
	old := cutoff - 1
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "location", 1)
	client.CacheEntry.UpdateOneID("entry").SetUpdatedAt(old).ExecX(ctx)
	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(old).ExecX(ctx)

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	result, err := service.RequestExpiredLocationDeletion(ctx, location.ID, cutoff)
	require.NoError(t, err)
	require.False(t, result.Fenced)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
	require.NoError(t, service.ReleaseReader(ctx, lease.ID))

	client.CacheEntry.UpdateOneID("entry").SetUpdatedAt(cutoff).ExecX(ctx)
	result, err = service.RequestExpiredLocationDeletion(ctx, location.ID, cutoff)
	require.NoError(t, err)
	require.False(t, result.Fenced)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))

	client.CacheEntry.UpdateOneID("entry").SetUpdatedAt(old).ExecX(ctx)
	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(cutoff).ExecX(ctx)
	result, err = service.RequestExpiredLocationDeletion(ctx, location.ID, cutoff)
	require.NoError(t, err)
	require.False(t, result.Fenced)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))

	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(old).ExecX(ctx)
	result, err = service.RequestExpiredLocationDeletion(ctx, location.ID, cutoff)
	require.NoError(t, err)
	require.True(t, result.Fenced)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
}

func TestCapacityEvictionClaimsOnlyUnchangedUnreadLocation(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "capacity-location", 1)
	client.StorageLocation.UpdateOneID(location.ID).SetSizeBytes(42).SetRecencyAt(now.Add(-time.Hour).UnixMilli()).ExecX(ctx)
	client.CacheEntry.UpdateOneID("entry").SetUpdatedAt(now.Add(-time.Hour).UnixMilli()).ExecX(ctx)
	location = client.StorageLocation.GetX(ctx, location.ID)

	result, err := service.RequestCapacityEviction(ctx, CapacityObservation{
		LocationID:       location.ID,
		LeaseVersion:     location.LeaseVersion,
		LastDownloadedAt: location.LastDownloadedAt,
		RecencyAt:        now.Add(-time.Hour).UnixMilli(),
	})

	require.NoError(t, err)
	require.True(t, result.Claimed)
	require.False(t, result.Pinned)
	require.Equal(t, int64(42), result.SizeBytes)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	require.NotNil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
}

func TestCapacityEvictionSkipsActiveReaderAndStaleAccessObservation(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	service := NewWithOptions(client, Options{Now: func() time.Time { return now }})
	location := createLifecycleEntry(ctx, client, "entry", "capacity-location", 2)
	entry := client.CacheEntry.GetX(ctx, "entry")
	client.StorageLocation.UpdateOneID(location.ID).SetSizeBytes(42).SetRecencyAt(entry.UpdatedAt).ExecX(ctx)

	stale := client.StorageLocation.GetX(ctx, location.ID)
	lease, err := service.AcquireReader(ctx, entry.ID, AcquireReaderOptions{})
	require.NoError(t, err)
	current := client.StorageLocation.GetX(ctx, location.ID)

	staleResult, err := service.RequestCapacityEviction(ctx, CapacityObservation{
		LocationID:       stale.ID,
		LeaseVersion:     stale.LeaseVersion,
		LastDownloadedAt: stale.LastDownloadedAt,
		RecencyAt:        entry.UpdatedAt,
	})
	require.NoError(t, err)
	require.False(t, staleResult.Claimed)
	require.False(t, staleResult.Pinned)

	pinnedResult, err := service.RequestCapacityEviction(ctx, CapacityObservation{
		LocationID:       current.ID,
		LeaseVersion:     current.LeaseVersion,
		LastDownloadedAt: current.LastDownloadedAt,
		RecencyAt:        entry.UpdatedAt,
	})
	require.NoError(t, err)
	require.False(t, pinnedResult.Claimed)
	require.True(t, pinnedResult.Pinned)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
	require.NoError(t, service.ReleaseReader(ctx, lease.ID))
}

func TestCapacityEvictionRechecksMaterializedRecency(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	location := createLifecycleEntry(ctx, client, "entry", "capacity-location", 1)
	entry := client.CacheEntry.GetX(ctx, "entry")
	client.StorageLocation.UpdateOneID(location.ID).SetSizeBytes(42).SetRecencyAt(entry.UpdatedAt).ExecX(ctx)
	location = client.StorageLocation.GetX(ctx, location.ID)
	client.StorageLocation.UpdateOneID(location.ID).SetRecencyAt(entry.UpdatedAt + 1).ExecX(ctx)

	result, err := service.RequestCapacityEviction(ctx, CapacityObservation{
		LocationID:       location.ID,
		LeaseVersion:     location.LeaseVersion,
		LastDownloadedAt: location.LastDownloadedAt,
		RecencyAt:        entry.UpdatedAt,
	})

	require.NoError(t, err)
	require.False(t, result.Claimed)
	require.False(t, result.Pinned)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
	require.Nil(t, client.StorageLocation.GetX(ctx, location.ID).DeletionRequestedAt)
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

func TestConditionalDanglingPurgeTreatsReaderLeaseVersionChangeAsStale(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	observed := createLifecycleEntry(ctx, client, "entry", "location", 2)

	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	deleted, err := service.PurgeDanglingCacheEntryIfUnchanged(ctx, "entry", observed)
	require.NoError(t, err)
	require.False(t, deleted)
	require.True(t, client.CacheEntry.Query().Where(cacheentry.ID("entry")).ExistX(ctx))

	refreshed := client.StorageLocation.GetX(ctx, observed.ID)
	deleted, err = service.PurgeDanglingCacheEntryIfUnchanged(ctx, "entry", refreshed)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NotNil(t, client.StorageLocation.GetX(ctx, observed.ID).DeletionRequestedAt)
	require.NoError(t, service.ReleaseReader(ctx, lease.ID))
}

func TestConditionalDanglingPurgeIgnoresMergedReaderAcquisition(t *testing.T) {
	ctx, client, _ := testutil.NewSQLiteFilesystem(t)
	service := New(client)
	observed := createLifecycleEntry(ctx, client, "entry", "location", 2)
	client.StorageLocation.UpdateOneID(observed.ID).SetMergedAt(time.Now().UnixMilli()).ExecX(ctx)
	observed = client.StorageLocation.GetX(ctx, observed.ID)

	// A merged reader no longer bumps leaseVersion, so the observation stays
	// fresh and the purge proceeds; the fence defers physical deletion.
	lease, err := service.AcquireReader(ctx, "entry", AcquireReaderOptions{})
	require.NoError(t, err)
	deleted, err := service.PurgeDanglingCacheEntryIfUnchanged(ctx, "entry", observed)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NotNil(t, client.StorageLocation.GetX(ctx, observed.ID).DeletionRequestedAt)
	require.Equal(t, 1, client.StorageLocation.Query().CountX(ctx))
	require.NoError(t, service.ReleaseReader(ctx, lease.ID))
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
