package storagereconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestReconcilePersistsLogicalSizesFromPartsAndMergedRepresentations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)

	partsLocation := createLegacyLocation(t, ctx, client, "parts-location", "parts-folder", 2, false, false)
	require.NoError(t, filesystem.UploadStream(ctx, storage.PartsFolder(partsLocation.FolderName)+"/0", strings.NewReader("hello ")))
	require.NoError(t, filesystem.UploadStream(ctx, storage.PartsFolder(partsLocation.FolderName)+"/1", strings.NewReader("world")))
	require.NoError(t, filesystem.UploadStream(ctx, partsLocation.FolderName+"/blocks/YmxvY2s", strings.NewReader("duplicate")))
	require.NoError(t, filesystem.UploadStream(ctx, storage.MergedObject(partsLocation.FolderName), strings.NewReader("hello world")))

	mergedLocation := createLegacyLocation(t, ctx, client, "merged-location", "merged-folder", 2, true, true)
	require.NoError(t, filesystem.UploadStream(ctx, storage.MergedObject(mergedLocation.FolderName), strings.NewReader("merged")))

	result, err := Reconcile(ctx, Options{DB: client, Storage: filesystem, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, 2, result.Candidates)
	require.Equal(t, 2, result.Updated)

	partsLocation = client.StorageLocation.GetX(ctx, partsLocation.ID)
	require.NotNil(t, partsLocation.SizeBytes)
	require.Equal(t, int64(11), *partsLocation.SizeBytes)
	mergedLocation = client.StorageLocation.GetX(ctx, mergedLocation.ID)
	require.NotNil(t, mergedLocation.SizeBytes)
	require.Equal(t, int64(6), *mergedLocation.SizeBytes)
}

func TestReconcileFencesMissingAndZeroPartLegacyLocations(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)
	zeroPart := createLegacyLocation(t, ctx, client, "zero-entry", "zero-folder", 0, false, false)
	missing := createLegacyLocation(t, ctx, client, "missing-entry", "missing-folder", 1, false, false)

	result, err := Reconcile(ctx, Options{DB: client, Storage: filesystem, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, 2, result.Purged)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))

	zeroPart = client.StorageLocation.GetX(ctx, zeroPart.ID)
	require.Nil(t, zeroPart.SizeBytes)
	require.NotNil(t, zeroPart.DeletionRequestedAt)
	missing = client.StorageLocation.GetX(ctx, missing.ID)
	require.Nil(t, missing.SizeBytes)
	require.NotNil(t, missing.DeletionRequestedAt)
}

func TestReconcileAbandonsStalePurgeWhenMaterializationFinishes(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)
	location := createLegacyLocation(t, ctx, client, "entry", "folder", 2, false, false)
	partZero := storage.PartsFolder(location.FolderName) + "/0"

	adapter := &materializeOnInspectAdapter{Adapter: filesystem, objectName: partZero}
	adapter.onInspect = func() {
		lease, err := lifecycle.AcquireMaterialization(ctx, location.ID)
		require.NoError(t, err)
		require.NoError(t, filesystem.UploadStream(ctx, storage.MergedObject(location.FolderName), strings.NewReader("recovered")))
		require.NoError(t, lifecycle.FinishMaterialization(ctx, location.ID, lease.Token))
	}

	result, err := Reconcile(ctx, Options{DB: client, Storage: adapter, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, 1, result.Updated)
	require.Zero(t, result.Purged)
	require.True(t, client.CacheEntry.Query().ExistX(ctx))

	location = client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, location.MergedAt)
	require.NotNil(t, location.SizeBytes)
	require.Equal(t, int64(9), *location.SizeBytes)
}

func TestReconcileResumesOnlyRowsStillMissingSizes(t *testing.T) {
	_, client, filesystem := testutil.NewSQLiteFilesystem(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	lifecycle := storagelifecycle.New(client)
	for index, payload := range []string{"first", "second"} {
		entryID := "entry-" + string(rune('a'+index))
		folderName := "folder-" + string(rune('a'+index))
		createLegacyLocation(t, ctx, client, entryID, folderName, 1, false, false)
		require.NoError(t, filesystem.UploadStream(ctx, storage.PartsFolder(folderName)+"/0", strings.NewReader(payload)))
	}

	cancelAfterFirstSizeWrite := true
	client.Use(func(next ent.Mutator) ent.Mutator {
		return ent.MutateFunc(func(mutationCtx context.Context, mutation ent.Mutation) (ent.Value, error) {
			storageMutation, ok := mutation.(*ent.StorageLocationMutation)
			if !ok {
				return next.Mutate(mutationCtx, mutation)
			}
			if _, exists := storageMutation.SizeBytes(); !exists || !cancelAfterFirstSizeWrite {
				return next.Mutate(mutationCtx, mutation)
			}
			value, err := next.Mutate(mutationCtx, mutation)
			if err == nil {
				cancelAfterFirstSizeWrite = false
				cancel()
			}
			return value, err
		})
	})

	result, err := Reconcile(ctx, Options{DB: client, Storage: filesystem, Lifecycle: lifecycle})
	require.Error(t, err)
	require.Equal(t, 1, result.Updated)
	require.Equal(t, 1, client.StorageLocation.Query().Where(storagelocation.SizeBytesNotNil()).CountX(context.Background()))

	result, err = Reconcile(context.Background(), Options{DB: client, Storage: filesystem, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, 1, result.Candidates)
	require.Equal(t, 1, result.Updated)
	require.Equal(t, 2, client.StorageLocation.Query().Where(storagelocation.SizeBytesNotNil()).CountX(context.Background()))
}

func TestReconcileKeepsCompletedRowsWhenLaterInspectionFails(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)
	completed := createLegacyLocation(t, ctx, client, "entry-a", "folder-a", 1, false, false)
	failing := createLegacyLocation(t, ctx, client, "entry-b", "folder-b", 1, false, false)
	require.NoError(t, filesystem.UploadStream(ctx, storage.PartsFolder(completed.FolderName)+"/0", strings.NewReader("payload-a")))
	require.NoError(t, filesystem.UploadStream(ctx, storage.PartsFolder(failing.FolderName)+"/0", strings.NewReader("payload-b")))
	adapter := failingIndexedFolderAdapter{Adapter: filesystem, folderName: storage.PartsFolder(failing.FolderName)}

	result, err := Reconcile(ctx, Options{DB: client, Storage: adapter, Lifecycle: lifecycle})
	require.ErrorContains(t, err, "page failed")
	require.Equal(t, 1, result.Updated)
	require.NotNil(t, client.StorageLocation.GetX(ctx, completed.ID).SizeBytes)
	require.Nil(t, client.StorageLocation.GetX(ctx, failing.ID).SizeBytes)

	result, err = Reconcile(ctx, Options{DB: client, Storage: filesystem, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, 1, result.Updated)
}

func TestReconcileProcessesCandidatesAcrossKeysetPages(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)
	const candidateCount = reconciliationPageSize + 25
	for index := 0; index < candidateCount; index++ {
		id := fmt.Sprintf("entry-%03d", index)
		createLegacyLocation(t, ctx, client, id, "folder-"+id, 1, false, false)
	}
	adapter := &countingIndexedFolderAdapter{Adapter: filesystem, size: 7}

	result, err := Reconcile(ctx, Options{DB: client, Storage: adapter, Lifecycle: lifecycle})
	require.NoError(t, err)
	require.Equal(t, candidateCount, result.Candidates)
	require.Equal(t, candidateCount, result.Updated)
	require.Equal(t, candidateCount, adapter.calls)
	require.Equal(t, candidateCount, client.StorageLocation.Query().Where(storagelocation.SizeBytesNotNil()).CountX(ctx))
}

func TestRunReconcilePurgesOverLimitLegacyPartsAndClosesReadiness(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	lifecycle := storagelifecycle.New(client)
	location := createLegacyLocation(
		t,
		ctx,
		client,
		"over-limit-entry",
		"over-limit-folder",
		storage.MaxIndexedObjects+1,
		false,
		false,
	)
	ready := make(chan struct{})

	Run(ctx, Options{DB: client, Storage: filesystem, Lifecycle: lifecycle}, ready)

	select {
	case <-ready:
	default:
		t.Fatal("reconciliation readiness was not closed")
	}
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.DeletionRequestedAt)
}

func createLegacyLocation(t *testing.T, ctx context.Context, client *ent.Client, entryID, folderName string, partCount int, merged, partsDeleted bool) *ent.StorageLocation {
	t.Helper()
	create := client.StorageLocation.Create().
		SetID(entryID + "-location").
		SetFolderName(folderName).
		SetPartCount(partCount)
	now := time.Now().UnixMilli()
	if merged {
		create.SetMergedAt(now)
	}
	if partsDeleted {
		create.SetPartsDeletedAt(now)
	}
	location := create.SaveX(ctx)
	client.CacheEntry.Create().
		SetID(entryID).
		SetKey(entryID + "-key").
		SetVersion("version").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(now).
		SetLocation(location).
		SaveX(ctx)
	return location
}

type materializeOnInspectAdapter struct {
	storage.Adapter
	objectName string
	onInspect  func()
	once       sync.Once
}

func (a *materializeOnInspectAdapter) InspectObject(ctx context.Context, objectName string) (storage.ObjectMetadata, error) {
	triggered := false
	if objectName == a.objectName {
		a.once.Do(func() {
			triggered = true
			a.onInspect()
		})
	}
	if triggered {
		return storage.ObjectMetadata{}, storage.ObjectNotFoundError{ObjectName: objectName}
	}
	return a.Adapter.InspectObject(ctx, objectName)
}

type failingIndexedFolderAdapter struct {
	storage.Adapter
	folderName string
}

type countingIndexedFolderAdapter struct {
	storage.Adapter
	size  int64
	calls int
}

func (a *countingIndexedFolderAdapter) InspectIndexedFolder(context.Context, string, int) (int64, error) {
	a.calls++
	return a.size, nil
}

func (f failingIndexedFolderAdapter) InspectIndexedFolder(ctx context.Context, folderName string, expectedObjects int) (int64, error) {
	if folderName == f.folderName {
		return 0, errors.New("page failed")
	}
	return f.Adapter.InspectIndexedFolder(ctx, folderName, expectedObjects)
}
