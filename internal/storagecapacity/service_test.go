package storagecapacity

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestExplicitBudgetEvictsLeastRecentlyUsedToTarget(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	now := time.Now().Truncate(time.Millisecond)
	old := createCapacityLocation(ctx, client, "old", 6, nil, now.Add(-2*time.Hour).UnixMilli())
	downloadedAt := now.Add(-time.Hour).UnixMilli()
	recent := createCapacityLocation(ctx, client, "recent", 6, &downloadedAt, now.Add(-3*time.Hour).UnixMilli())
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(11),
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Equal(t, int64(12), result.UsageBeforeBytes)
	require.Equal(t, int64(6), result.UsageAfterBytes)
	require.Equal(t, int64(9), result.TargetBytes)
	require.Equal(t, 1, result.ClaimedLocations)
	require.False(t, result.Constrained)
	require.NotNil(t, client.StorageLocation.GetX(ctx, old.ID).DeletionRequestedAt)
	require.Nil(t, client.StorageLocation.GetX(ctx, recent.ID).DeletionRequestedAt)
}

func TestExplicitBudgetDoesNotEvictAtBudgetBoundary(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	createCapacityLocation(ctx, client, "at-budget", 10, nil, 1)
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(10),
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Zero(t, result.ClaimedLocations)
	require.Equal(t, int64(10), result.UsageAfterBytes)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
}

func TestExplicitBudgetFailsClosedWhenActiveSizeIsUnknown(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	location := client.StorageLocation.Create().
		SetID("unknown-location").
		SetFolderName("unknown-folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("unknown-entry").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(1).
		SetLocation(location).
		SaveX(ctx)
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(1),
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.ErrorIs(t, err, ErrUnresolvedSizes)
	require.Equal(t, 1, result.UnresolvedLocations)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
}

func TestFilesystemPendingCreditPreventsRepeatedGraceEviction(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	pending := createCapacityLocation(ctx, client, "pending", 10, nil, 1)
	client.CacheEntry.DeleteOneID("pending-entry").ExecX(ctx)
	client.StorageLocation.UpdateOneID(pending.ID).SetDeletionRequestedAt(time.Now().UnixMilli()).ExecX(ctx)
	active := createCapacityLocation(ctx, client, "active", 10, nil, 2)
	adapter := &fixedUsageAdapter{
		Adapter: filesystem,
		usage:   storage.FilesystemUsage{CapacityBytes: 100, UsedBytes: 100},
	}
	service := NewService(Options{
		DB:      client,
		Storage: adapter,
		Config: config.CacheConfig{
			FilesystemMaxUsagePercent: 90,
		},
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Equal(t, int64(10), result.PendingCreditBytes)
	require.Equal(t, int64(90), result.UsageBeforeBytes)
	require.Zero(t, result.ClaimedLocations)
	require.Nil(t, client.StorageLocation.GetX(ctx, active.ID).DeletionRequestedAt)
}

func TestExplicitBudgetSkipsPinnedCandidateAndContinues(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	old := createCapacityLocation(ctx, client, "old", 6, nil, 1)
	recent := createCapacityLocation(ctx, client, "recent", 6, nil, 2)
	lifecycle := storagelifecycle.New(client)
	lease, err := lifecycle.AcquireReader(ctx, "old-entry", storagelifecycle.AcquireReaderOptions{})
	require.NoError(t, err)
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(11),
		Lifecycle: lifecycle,
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, result.PinnedLocations)
	require.Equal(t, 1, result.ClaimedLocations)
	require.Nil(t, client.StorageLocation.GetX(ctx, old.ID).DeletionRequestedAt)
	require.NotNil(t, client.StorageLocation.GetX(ctx, recent.ID).DeletionRequestedAt)
	require.NoError(t, lifecycle.ReleaseReader(ctx, lease.ID))
}

func TestExplicitBudgetUsesNewestChildSaveAsFallbackRecency(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	shared := createCapacityLocation(ctx, client, "shared", 6, nil, 1)
	client.CacheEntry.Create().
		SetID("shared-newer-entry").
		SetKey("shared-newer-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(100).
		SetLocation(shared).
		SaveX(ctx)
	middle := createCapacityLocation(ctx, client, "middle", 6, nil, 50)
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(11),
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, result.ClaimedLocations)
	require.Nil(t, client.StorageLocation.GetX(ctx, shared.ID).DeletionRequestedAt)
	require.NotNil(t, client.StorageLocation.GetX(ctx, middle.ID).DeletionRequestedAt)
}

func TestExplicitBudgetPaginatesMoreThanOneCandidatePage(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	for index := range 60 {
		createCapacityLocation(ctx, client, fmt.Sprintf("location-%02d", index), 1, nil, int64(index+1))
	}
	service := NewService(Options{
		DB:        client,
		Storage:   filesystem,
		Config:    explicitCapacityConfig(10),
		Lifecycle: storagelifecycle.New(client),
	})

	result, err := service.Enforce(ctx)

	require.NoError(t, err)
	require.Equal(t, 51, result.ClaimedLocations)
	require.Equal(t, int64(9), result.UsageAfterBytes)
	require.False(t, result.Constrained)
	require.Equal(t, 9, client.CacheEntry.Query().CountX(ctx))
}

func TestRunCoalescesPreReadinessSignalIntoStartupPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	service := &Service{
		enabled:         true,
		trigger:         make(chan struct{}, 1),
		enforceInterval: time.Hour,
		retryInitial:    time.Hour,
		retryMaximum:    time.Hour,
	}
	service.enforce = func(context.Context) (Result, error) {
		calls.Add(1)
		return Result{}, nil
	}
	service.Trigger()
	go func() {
		defer close(done)
		service.Run(ctx, ready)
	}()

	close(ready)
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return calls.Load() > 1 }, 50*time.Millisecond, time.Millisecond)
	cancel()
	<-done
}

func createCapacityLocation(ctx context.Context, client *ent.Client, id string, size int64, lastDownloadedAt *int64, updatedAt int64) *ent.StorageLocation {
	location := client.StorageLocation.Create().
		SetID(id + "-location").
		SetFolderName(id + "-folder").
		SetPartCount(1).
		SetSizeBytes(size).
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

func explicitCapacityConfig(maxSize int64) config.CacheConfig {
	return config.CacheConfig{
		MaxSizeBytes:              maxSize,
		MaxSizeBytesConfigured:    true,
		FilesystemMaxUsagePercent: 90,
	}
}

type fixedUsageAdapter struct {
	storage.Adapter
	usage storage.FilesystemUsage
}

func (a *fixedUsageAdapter) FilesystemUsage(context.Context) (storage.FilesystemUsage, error) {
	return a.usage, nil
}
