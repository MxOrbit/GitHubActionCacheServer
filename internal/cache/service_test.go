package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestReadScopesByPermissionFiltersUnreadableScopes(t *testing.T) {
	scopes := ReadScopesByPermission(auth.CacheScope{
		Scopes: []auth.Scope{
			{Scope: "refs/heads/none", Permission: 0},
			{Scope: "refs/heads/write", Permission: 2},
			{Scope: "refs/heads/read", Permission: 1},
		},
	})

	require.Equal(t, []string{"refs/heads/write", "refs/heads/read"}, scopes)
}

func TestUploadPartFailureDoesNotPoisonFinalize(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &failOnceStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)

	err = service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("failed"))
	require.ErrorIs(t, err, errInjectedUploadFailure)
	currentUpload := client.Upload.GetX(ctx, upload.UploadID)
	require.Zero(t, currentUpload.StartedPartUploadCount)
	require.Zero(t, currentUpload.FinishedPartUploadCount)

	require.NoError(t, service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("ok")))
	currentUpload = client.Upload.GetX(ctx, upload.UploadID)
	require.Zero(t, currentUpload.StartedPartUploadCount)
	require.Equal(t, 1, currentUpload.FinishedPartUploadCount)
	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
}

func TestCommitBlockListCopiesBlocksInDeclaredOrderAndCanRetry(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &copyTrackingStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "first", bytes.NewBufferString("hello ")))
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "second", bytes.NewBufferString("world")))

	blockIDs := []string{"second", "first"}
	require.NoError(t, service.CommitBlockList(ctx, upload.UploadID, blockIDs))
	require.NoError(t, service.CommitBlockList(ctx, upload.UploadID, blockIDs))

	currentUpload := client.Upload.GetX(ctx, upload.UploadID)
	require.NotNil(t, currentUpload.CommittedPartCount)
	require.Equal(t, len(blockIDs), *currentUpload.CommittedPartCount)
	require.Equal(t, [][2]string{
		{blockObjectName(currentUpload.FolderName, "second"), partObjectName(currentUpload.FolderName, 0)},
		{blockObjectName(currentUpload.FolderName, "first"), partObjectName(currentUpload.FolderName, 1)},
		{blockObjectName(currentUpload.FolderName, "second"), partObjectName(currentUpload.FolderName, 0)},
		{blockObjectName(currentUpload.FolderName, "first"), partObjectName(currentUpload.FolderName, 1)},
	}, adapter.copies)
	require.Equal(t, "world", readStorageObject(t, ctx, filesystem, partObjectName(currentUpload.FolderName, 0)))
	require.Equal(t, "hello ", readStorageObject(t, ctx, filesystem, partObjectName(currentUpload.FolderName, 1)))
	require.Equal(t, "hello ", readStorageObject(t, ctx, filesystem, blockObjectName(currentUpload.FolderName, "first")))
}

func TestCommitBlockListReportsMissingBlock(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})

	upload, err := service.CreateUpload(ctx, "key", "version", writableScope())
	require.NoError(t, err)
	err = service.CommitBlockList(ctx, upload.UploadID, []string{"missing"})
	require.ErrorIs(t, err, ErrPartCountMismatch)
}

func TestCompleteUploadTrustsPersistedPartCount(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("data")))
	currentUpload := client.Upload.GetX(ctx, upload.UploadID)
	require.NotNil(t, currentUpload.CommittedPartCount)
	require.Equal(t, 1, *currentUpload.CommittedPartCount)
	require.Equal(t, 1, currentUpload.FinishedPartUploadCount)
	adapter.reset()

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.Zero(t, adapter.countCalls)
	require.Zero(t, adapter.downloadCalls)
}

func TestCompleteUploadValidatesLegacyUploadWithoutPartCountMetadata(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	client.Upload.Create().
		SetID(42).
		SetKey("legacy-key").
		SetVersion("version").
		SetScope(scope.Scopes[0].Scope).
		SetRepoId(scope.RepoID).
		SetCreatedAt(time.Now().UnixMilli()).
		SetStartedPartUploadCount(1).
		SetFinishedPartUploadCount(1).
		SetFolderName("legacy-upload").
		SaveX(ctx)
	require.NoError(t, filesystem.UploadStream(ctx, "legacy-upload/parts/0", bytes.NewBufferString("data")))

	_, err := service.CompleteUpload(ctx, "legacy-key", "version", scope)
	require.NoError(t, err)
	require.Equal(t, 1, adapter.countCalls)
	require.Equal(t, 1, adapter.downloadCalls)
}

func TestCompleteUploadKeepsLegacyUploadAfterTransientStorageError(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem, countErr: errInjectedStorageFailure}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	client.Upload.Create().
		SetID(42).
		SetKey("legacy-key").
		SetVersion("version").
		SetScope(scope.Scopes[0].Scope).
		SetRepoId(scope.RepoID).
		SetCreatedAt(time.Now().UnixMilli()).
		SetStartedPartUploadCount(1).
		SetFinishedPartUploadCount(1).
		SetFolderName("legacy-upload").
		SaveX(ctx)

	_, err := service.CompleteUpload(ctx, "legacy-key", "version", scope)
	require.ErrorIs(t, err, errInjectedStorageFailure)
	_, err = client.Upload.Get(ctx, 42)
	require.NoError(t, err)
}

func TestCompleteUploadRejectsBlocksWithoutCommittedBlockList(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "block", bytes.NewBufferString("data")))

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.ErrorIs(t, err, ErrNoPartsUploaded)
	_, err = client.Upload.Get(ctx, upload.UploadID)
	require.True(t, ent.IsNotFound(err))
	require.Equal(t, "data", readStorageObject(t, ctx, filesystem, blockObjectName(strconv.FormatInt(upload.UploadID, 10), "block")))
	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, strconv.FormatInt(upload.UploadID, 10), task.FolderName)
	require.Zero(t, task.AttemptCount)
	require.Nil(t, task.LastAttemptedAt)
}

func TestCompleteUploadQueuesDeletionWithoutStorageIO(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &failDeleteStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "block", bytes.NewBufferString("data")))

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.ErrorIs(t, err, ErrNoPartsUploaded)
	_, err = client.Upload.Get(ctx, upload.UploadID)
	require.True(t, ent.IsNotFound(err))
	require.Equal(t, "data", readStorageObject(t, ctx, filesystem, blockObjectName(strconv.FormatInt(upload.UploadID, 10), "block")))
	require.False(t, adapter.deleteCalled)

	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, strconv.FormatInt(upload.UploadID, 10), task.FolderName)
	require.Zero(t, task.AttemptCount)
	require.Nil(t, task.LastAttemptedAt)
	require.Nil(t, task.LastError)
}

func TestDownloadTrustsPartCountAndOpensEachPartOnlyWhenRead(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	service.StopAcceptingMerges()
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello ")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("world")))
	createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.Zero(t, adapter.countCalls)
	require.Zero(t, adapter.downloadCalls)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "hello world", string(body))
	require.Zero(t, adapter.countCalls)
	require.Equal(t, 2, adapter.downloadCalls)
}

func TestMatchCacheEntryUsesOriginalOrder(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})

	createMatchedCacheEntry(ctx, client, "exact-primary", "linux-cache")
	createMatchedCacheEntry(ctx, client, "prefixed-primary", "linux-cache-old")
	createMatchedCacheEntry(ctx, client, "exact-restore", "restore-cache")
	createMatchedCacheEntry(ctx, client, "prefixed-restore", "restore-cache-old")

	match, err := service.MatchCacheEntry(ctx, []string{"linux-cache", "restore-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, "linux-cache", match.Key)

	client.CacheEntry.DeleteOneID("exact-primary").ExecX(ctx)

	match, err = service.MatchCacheEntry(ctx, []string{"linux-cache", "restore-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, "linux-cache-old", match.Key)

	client.CacheEntry.DeleteOneID("prefixed-primary").ExecX(ctx)

	match, err = service.MatchCacheEntry(ctx, []string{"linux-cache", "restore-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, "restore-cache", match.Key)

	client.CacheEntry.DeleteOneID("exact-restore").ExecX(ctx)

	match, err = service.MatchCacheEntry(ctx, []string{"linux-cache", "restore-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, "restore-cache-old", match.Key)
}

func TestCreateUploadReservationIsAtomic(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	scope := writableScope()

	const attempts = 20
	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.CreateUpload(ctx, "key", "version", scope)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	successes := 0
	alreadyExists := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrUploadAlreadyExists):
			alreadyExists++
		default:
			t.Fatalf("unexpected create upload error: %v", err)
		}
	}

	require.Equal(t, 1, successes)
	require.Equal(t, attempts-1, alreadyExists)

	count, err := client.Upload.Query().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	currentUpload := client.Upload.Query().OnlyX(ctx)
	require.NotNil(t, currentUpload.TupleHash)
	require.Equal(t, uploadTupleHash("key", "version", scope.Scopes[0].Scope, scope.RepoID), *currentUpload.TupleHash)
}

func TestUploadTupleHashPreservesFieldBoundaries(t *testing.T) {
	first := uploadTupleHash("ab", "c", "scope", "repo")
	second := uploadTupleHash("a", "bc", "scope", "repo")

	require.Len(t, first, sha256.Size*2)
	require.Equal(t, first, uploadTupleHash("ab", "c", "scope", "repo"))
	require.NotEqual(t, first, second)
}

func TestSinglePartUsesPartsObjectForDirectDownload(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &directURLStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter, EnableDirectDownloads: true})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	location := client.StorageLocation.GetX(ctx, entry.LocationId)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(location.FolderName, 0), bytes.NewBufferString("data")))

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string {
		return "fallback"
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "direct://entry-id-folder/parts/0", result.DownloadURL)
	require.Equal(t, "entry-id-folder/parts/0", adapter.objectName)
	location = client.StorageLocation.GetX(ctx, result.CacheEntry.LocationId)
	require.NotNil(t, location.LastDownloadedAt)
	lease := client.StorageReaderLease.Query().OnlyX(ctx)
	require.Equal(t, "parts", lease.Scope.String())
	require.WithinDuration(t, time.Now().Add(storagelifecycle.DirectDownloadLeaseDuration), time.UnixMilli(lease.ExpiresAt), time.Second)
}

func TestDirectDownloadThrottlesLastDownloadedAtUpdates(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &directURLStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter, EnableDirectDownloads: true})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	location := client.StorageLocation.GetX(ctx, entry.LocationId)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(location.FolderName, 0), bytes.NewBufferString("data")))
	recent := time.Now().Add(-lastDownloadedAtUpdateInterval / 2).UnixMilli()
	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(recent).ExecX(ctx)

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string {
		return "fallback"
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, recent, *client.StorageLocation.GetX(ctx, location.ID).LastDownloadedAt)
}

func TestReplacementFencesOldLocationUntilLazyReaderCloses(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	scope := writableScope()
	require.NoError(t, filesystem.UploadStream(ctx, "old/parts/0", bytes.NewBufferString("hello ")))
	require.NoError(t, filesystem.UploadStream(ctx, "old/parts/1", bytes.NewBufferString("world")))
	oldLocation := client.StorageLocation.Create().
		SetID("old-location").
		SetFolderName("old").
		SetPartCount(2).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("entry-id").
		SetKey("key").
		SetVersion("version").
		SetScope(scope.Scopes[0].Scope).
		SetRepoId(scope.RepoID).
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(oldLocation).
		SaveX(ctx)

	oldStream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	firstPart := make([]byte, len("hello "))
	_, err = io.ReadFull(oldStream, firstPart)
	require.NoError(t, err)
	require.Equal(t, "hello ", string(firstPart))

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("replacement")))
	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)

	pendingOld := client.StorageLocation.GetX(ctx, oldLocation.ID)
	require.NotNil(t, pendingOld.DeletionRequestedAt)
	require.Equal(t, 1, client.StorageReaderLease.Query().CountX(ctx))
	remainder, err := io.ReadAll(oldStream)
	require.NoError(t, err)
	require.Equal(t, "world", string(remainder))
	require.NoError(t, oldStream.Close())
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))

	newStream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	newBody, err := io.ReadAll(newStream)
	require.NoError(t, err)
	require.NoError(t, newStream.Close())
	require.Equal(t, "replacement", string(newBody))
}

func TestDownloadThrottlesLastDownloadedAtUpdates(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})

	recentLocation := createCacheEntryForDownload(ctx, client, "recent-entry", "recent-folder")
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(recentLocation.FolderName, 0), bytes.NewBufferString("recent")))
	recent := time.Now().Add(-lastDownloadedAtUpdateInterval / 2).UnixMilli()
	client.StorageLocation.UpdateOneID(recentLocation.ID).SetLastDownloadedAt(recent).ExecX(ctx)

	recentStream, err := service.Download(ctx, "recent-entry")
	require.NoError(t, err)
	require.NoError(t, recentStream.Close())
	require.Equal(t, recent, *client.StorageLocation.GetX(ctx, recentLocation.ID).LastDownloadedAt)

	staleLocation := createCacheEntryForDownload(ctx, client, "stale-entry", "stale-folder")
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(staleLocation.FolderName, 0), bytes.NewBufferString("stale")))
	stale := time.Now().Add(-lastDownloadedAtUpdateInterval - time.Minute).UnixMilli()
	client.StorageLocation.UpdateOneID(staleLocation.ID).SetLastDownloadedAt(stale).ExecX(ctx)
	touchedAfter := time.Now().UnixMilli()

	staleStream, err := service.Download(ctx, "stale-entry")
	require.NoError(t, err)
	require.NoError(t, staleStream.Close())
	refreshed := client.StorageLocation.GetX(ctx, staleLocation.ID).LastDownloadedAt
	require.NotNil(t, refreshed)
	require.GreaterOrEqual(t, *refreshed, touchedAfter)
}

func TestFailedMergedDownloadDoesNotUpdateLastDownloadedAt(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: &downloadErrorStorage{Adapter: filesystem}})
	location := createCacheEntryForDownload(ctx, client, "entry-id", "folder")
	stale := time.Now().Add(-lastDownloadedAtUpdateInterval - time.Minute).UnixMilli()
	client.StorageLocation.UpdateOneID(location.ID).
		SetMergedAt(time.Now().UnixMilli()).
		SetLastDownloadedAt(stale).
		ExecX(ctx)

	_, err := service.Download(ctx, "entry-id")
	require.ErrorIs(t, err, errInjectedStorageFailure)
	require.Equal(t, stale, *client.StorageLocation.GetX(ctx, location.ID).LastDownloadedAt)
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
}

func TestTouchStorageLocationDoesNotOverwriteNewerConcurrentTimestamp(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	location := createCacheEntryForDownload(ctx, client, "entry-id", "folder")
	stale := time.Now().Add(-lastDownloadedAtUpdateInterval - time.Minute).UnixMilli()
	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(stale).ExecX(ctx)
	staleSnapshot := client.StorageLocation.GetX(ctx, location.ID)

	newer := time.Now().UnixMilli()
	client.StorageLocation.UpdateOneID(location.ID).SetLastDownloadedAt(newer).ExecX(ctx)
	service.touchStorageLocationIfStale(ctx, staleSnapshot)

	require.Equal(t, newer, *client.StorageLocation.GetX(ctx, location.ID).LastDownloadedAt)
}

func TestLegacyMaterializedSinglePartUsesMergedObjectForDirectDownload(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &directURLStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter, EnableDirectDownloads: true})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	client.StorageLocation.UpdateOneID(entry.LocationId).
		SetMergedAt(time.Now().Add(-time.Hour).UnixMilli()).
		SetPartsDeletedAt(time.Now().UnixMilli()).
		ExecX(ctx)
	location := client.StorageLocation.GetX(ctx, entry.LocationId)
	require.NoError(t, filesystem.UploadStream(ctx, mergedObjectName(location.FolderName), bytes.NewBufferString("data")))

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string {
		return "fallback"
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "direct://entry-id-folder/merged", result.DownloadURL)
	require.Equal(t, "entry-id-folder/merged", adapter.objectName)
}

func TestLookupPurgesDanglingEntryAndFallsBackToRestoreKey(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	dangling := createMatchedCacheEntry(ctx, client, "dangling-entry", "primary-key")
	valid := createMatchedCacheEntry(ctx, client, "valid-entry", "restore-key")
	validLocation := client.StorageLocation.GetX(ctx, valid.LocationId)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(validLocation.FolderName, 0), bytes.NewBufferString("data")))

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"primary-key", "restore-key"}, "version", writableScope(), func(id string) string {
		return "download://" + id
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, valid.ID, result.CacheEntry.ID)
	require.Equal(t, "download://"+valid.ID, result.DownloadURL)
	_, err = client.CacheEntry.Get(ctx, dangling.ID)
	require.True(t, ent.IsNotFound(err))
	require.NotNil(t, client.StorageLocation.GetX(ctx, dangling.LocationId).DeletionRequestedAt)
}

func TestLookupUsesConstantCostRepresentationAnchors(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	partsEntry := createMatchedCacheEntry(ctx, client, "parts-entry", "parts-key")
	partsLocation := client.StorageLocation.GetX(ctx, partsEntry.LocationId)
	client.StorageLocation.UpdateOneID(partsLocation.ID).SetPartCount(3).ExecX(ctx)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(partsLocation.FolderName, 0), bytes.NewBufferString("anchor")))

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"parts-key"}, "version", writableScope(), func(string) string { return "fallback" })
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{partObjectName(partsLocation.FolderName, 0)}, adapter.objectExistsCalls)
	require.Zero(t, adapter.countCalls)

	adapter.reset()
	mergedEntry := createMatchedCacheEntry(ctx, client, "merged-entry", "merged-key")
	mergedLocation := client.StorageLocation.GetX(ctx, mergedEntry.LocationId)
	client.StorageLocation.UpdateOneID(mergedLocation.ID).SetMergedAt(time.Now().UnixMilli()).ExecX(ctx)
	require.NoError(t, filesystem.UploadStream(ctx, mergedObjectName(mergedLocation.FolderName), bytes.NewBufferString("merged")))

	result, err = service.GetCacheEntryWithDownloadURL(ctx, []string{"merged-key"}, "version", writableScope(), func(string) string { return "fallback" })
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{mergedObjectName(mergedLocation.FolderName)}, adapter.objectExistsCalls)
	require.Zero(t, adapter.countCalls)
}

func TestLookupTreatsPartsDeletedWithoutMergeAsDanglingWithoutStorageProbe(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &storageCallTrackingAdapter{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	client.StorageLocation.UpdateOneID(entry.LocationId).SetPartsDeletedAt(time.Now().UnixMilli()).ExecX(ctx)

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string { return "fallback" })

	require.NoError(t, err)
	require.Nil(t, result)
	require.Empty(t, adapter.objectExistsCalls)
	_, err = client.CacheEntry.Get(ctx, entry.ID)
	require.True(t, ent.IsNotFound(err))
}

func TestLookupPreservesMetadataWhenStorageProbeFails(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &objectExistsErrorStorage{Adapter: filesystem, err: errInjectedStorageFailure}
	service := NewService(Options{DB: client, Storage: adapter})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string { return "fallback" })

	require.Nil(t, result)
	require.ErrorIs(t, err, errInjectedStorageFailure)
	require.Equal(t, entry.LocationId, client.CacheEntry.GetX(ctx, entry.ID).LocationId)
	require.Nil(t, client.StorageLocation.GetX(ctx, entry.LocationId).DeletionRequestedAt)
}

func TestLookupDoesNotPresignDanglingDirectDownload(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &directURLStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter, EnableDirectDownloads: true})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string { return "fallback" })

	require.NoError(t, err)
	require.Nil(t, result)
	require.Empty(t, adapter.objectName)
	_, err = client.CacheEntry.Get(ctx, entry.ID)
	require.True(t, ent.IsNotFound(err))
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
}

func TestLookupDoesNotDeleteConcurrentReplacement(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingObjectExistsStorage(filesystem, "old-folder/parts/0")
	service := NewService(Options{DB: client, Storage: adapter})
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	oldLocation := client.StorageLocation.GetX(ctx, entry.LocationId)
	client.StorageLocation.UpdateOneID(oldLocation.ID).SetFolderName("old-folder").ExecX(ctx)
	newLocation := client.StorageLocation.Create().
		SetID("new-location").
		SetFolderName("new-folder").
		SetPartCount(1).
		SaveX(ctx)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(newLocation.FolderName, 0), bytes.NewBufferString("replacement")))

	type lookupResult struct {
		match *MatchResult
		err   error
	}
	lookupDone := make(chan lookupResult, 1)
	go func() {
		match, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string { return "fallback" })
		lookupDone <- lookupResult{match: match, err: err}
	}()
	<-adapter.started
	client.CacheEntry.UpdateOneID(entry.ID).SetLocation(newLocation).ExecX(ctx)
	close(adapter.release)

	lookup := <-lookupDone
	require.NoError(t, lookup.err)
	require.NotNil(t, lookup.match)
	require.Equal(t, newLocation.ID, client.CacheEntry.GetX(ctx, entry.ID).LocationId)
}

func TestLookupRevalidatesRepresentationSelectedByDirectLease(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	entry := createMatchedCacheEntry(ctx, client, "entry-id", "key")
	oldLocation := client.StorageLocation.GetX(ctx, entry.LocationId)
	require.NoError(t, filesystem.UploadStream(ctx, partObjectName(oldLocation.FolderName, 0), bytes.NewBufferString("old")))
	newLocation := client.StorageLocation.Create().
		SetID("new-location").
		SetFolderName("new-folder").
		SetPartCount(1).
		SaveX(ctx)
	hook := &afterObjectExistsStorage{
		Adapter: filesystem,
		after: func() {
			client.CacheEntry.UpdateOneID(entry.ID).SetLocation(newLocation).ExecX(ctx)
		},
	}
	direct := &directURLStorage{Adapter: hook}
	service := NewService(Options{DB: client, Storage: direct, EnableDirectDownloads: true})

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"key"}, "version", writableScope(), func(string) string { return "fallback" })

	require.NoError(t, err)
	require.Nil(t, result)
	require.Empty(t, direct.objectName)
	_, err = client.CacheEntry.Get(ctx, entry.ID)
	require.True(t, ent.IsNotFound(err))
	require.Zero(t, client.StorageReaderLease.Query().CountX(ctx))
}

func TestLookupBoundsDanglingPurgeAttempts(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	for index := 0; index < maxDanglingPurgeAttempts+1; index++ {
		entry := createMatchedCacheEntry(ctx, client, fmt.Sprintf("entry-%02d", index), fmt.Sprintf("prefix-%02d", index))
		client.CacheEntry.UpdateOneID(entry.ID).SetUpdatedAt(int64(index + 1)).ExecX(ctx)
	}

	result, err := service.GetCacheEntryWithDownloadURL(ctx, []string{"prefix-"}, "version", writableScope(), func(string) string { return "fallback" })

	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, client.CacheEntry.Query().CountX(ctx))
}

func TestDirectDownloadsScheduleMaterializationAfterFinalize(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingComposeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, EnableDirectDownloads: true, MergeConcurrency: 1})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "first", bytes.NewBufferString("hello ")))
	require.NoError(t, service.UploadBlock(ctx, upload.UploadID, "second", bytes.NewBufferString("world")))
	require.NoError(t, service.CommitBlockList(ctx, upload.UploadID, []string{"first", "second"}))
	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	adapter.waitStarted(t)

	location := client.StorageLocation.Query().OnlyX(ctx)
	require.NotNil(t, location.MergeStartedAt)
	require.Nil(t, location.MergedAt)

	adapter.releaseOne()
	require.NoError(t, service.WaitForMerges(ctx))
	location = client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, location.MergedAt)
	require.Equal(t, "hello world", readStorageObject(t, ctx, filesystem, mergedObjectName(location.FolderName)))
}

func TestFilesystemDownloadStreamsPartsWithoutMaterializing(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello ")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("world")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "hello world", string(body))

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.Nil(t, current.MergeStartedAt)
	require.Nil(t, current.MergedAt)
	_, err = filesystem.CreateDownloadStream(ctx, "folder/merged")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestStalePartsLocationDoesNotRestartCompletedMaterialization(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &trackingComposeStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	mergedAt := time.Now().UnixMilli()
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(2).
		SetMergedAt(mergedAt).
		SaveX(ctx)
	staleLocation := *location
	staleLocation.MergedAt = nil

	service.tryStartMaterialization(&staleLocation)
	require.NoError(t, service.WaitForMerges(ctx))
	require.Zero(t, adapter.callCount())

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Equal(t, mergedAt, *current.MergedAt)
}

func TestExpiredMaterializationLeaseCanBeRescheduledByRequest(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &trackingComposeStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("a")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("b")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)
	client.StorageLocation.UpdateOneID(location.ID).
		SetMergeStartedAt(time.Now().Add(-time.Hour).UnixMilli()).
		SetMergeLeaseToken("expired-owner").
		SetMergeLeaseExpiresAt(time.Now().Add(-time.Minute).UnixMilli()).
		ExecX(ctx)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NoError(t, service.WaitForMerges(ctx))

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Equal(t, 1, adapter.callCount())
}

func TestLostMaterializationOwnerCannotClearSuccessorLease(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	mergeStartedAt := time.Now().UnixMilli()
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(2).
		SetMergeStartedAt(mergeStartedAt).
		SetMergeLeaseToken("successor").
		SetMergeLeaseExpiresAt(time.Now().Add(time.Minute).UnixMilli()).
		SaveX(ctx)

	require.NoError(t, service.lifecycle.ReleaseMaterialization(ctx, location.ID, "old-owner"))

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergeStartedAt)
	require.NotNil(t, current.MergeLeaseToken)
	require.NotNil(t, current.MergeLeaseExpiresAt)
	require.Equal(t, mergeStartedAt, *current.MergeStartedAt)
	require.Equal(t, "successor", *current.MergeLeaseToken)
}

func TestDownloadIsIndependentFromBackgroundMaterialization(t *testing.T) {
	fixture := startBlockingMaterializationDownload(t, "hello", " world")

	current := fixture.client.StorageLocation.GetX(fixture.ctx, fixture.location.ID)
	require.NotNil(t, current.MergeStartedAt)
	require.Nil(t, current.MergedAt)

	body, err := io.ReadAll(fixture.stream)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(body))

	fixture.adapter.releaseOne()
	require.NoError(t, fixture.service.WaitForMerges(fixture.ctx))
	current = fixture.client.StorageLocation.GetX(fixture.ctx, fixture.location.ID)
	require.NotNil(t, current.MergedAt)
	require.Equal(t, "hello world", readStorageObject(t, fixture.ctx, fixture.filesystem, "folder/merged"))
}

func TestSinglePartNeverMaterializes(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &trackingComposeStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("data")))
	location := createCacheEntryForDownload(ctx, client, "entry-id", "folder")

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "data", string(body))
	require.Zero(t, adapter.callCount())

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.Nil(t, current.MergeStartedAt)
	require.Nil(t, current.MergedAt)
}

func TestUnsupportedMaterializationIsPersisted(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &trackingComposeStorage{Adapter: filesystem, errors: []error{storage.ErrComposeUnsupported}}
	service := NewService(Options{DB: client, Storage: adapter})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("a")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("b")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return current.MaterializationUnsupportedAt != nil && current.MergeStartedAt == nil && current.MergeLeaseToken == nil && current.MergeLeaseExpiresAt == nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, adapter.callCount())

	stream, err = service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, adapter.callCount())
}

func TestTransientMaterializationFailureCanRetry(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &trackingComposeStorage{Adapter: filesystem, errors: []error{errInjectedStorageFailure}}
	service := NewService(Options{DB: client, Storage: adapter})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("a")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("b")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return adapter.callCount() == 1 && current.MergeStartedAt == nil && current.MergeLeaseToken == nil && current.MergeLeaseExpiresAt == nil
	}, time.Second, 10*time.Millisecond)

	stream, err = service.Download(ctx, "entry-id")
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.NoError(t, service.WaitForMerges(ctx))
	require.Equal(t, 2, adapter.callCount())
	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
}

func TestBackgroundMaterializationsRespectConcurrencyLimit(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingComposeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1})

	require.NoError(t, filesystem.UploadStream(ctx, "folder-a/parts/0", bytes.NewBufferString("a")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder-a/parts/1", bytes.NewBufferString("a2")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder-b/parts/0", bytes.NewBufferString("b")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder-b/parts/1", bytes.NewBufferString("b2")))
	createCacheEntryForDownloadWithPartCount(ctx, client, "entry-a", "folder-a", 2)
	createCacheEntryForDownloadWithPartCount(ctx, client, "entry-b", "folder-b", 2)

	streamA, err := service.Download(ctx, "entry-a")
	require.NoError(t, err)
	defer streamA.Close()
	bodyA, err := io.ReadAll(streamA)
	require.NoError(t, err)
	require.Equal(t, "aa2", string(bodyA))

	streamB, err := service.Download(ctx, "entry-b")
	require.NoError(t, err)
	defer streamB.Close()
	adapter.waitStarted(t)
	bodyB, err := io.ReadAll(streamB)
	require.NoError(t, err)
	require.Equal(t, "bb2", string(bodyB))

	select {
	case <-adapter.started:
		t.Fatal("second merge started before first merge released")
	case <-time.After(50 * time.Millisecond):
	}

	adapter.releaseOne()
	require.NoError(t, service.WaitForMerges(ctx))
	require.Equal(t, 1, adapter.maxActive())

	currentB := client.StorageLocation.GetX(ctx, "entry-b-location")
	require.Nil(t, currentB.MergeStartedAt)
	require.Nil(t, currentB.MergedAt)

	streamBRetry, err := service.Download(ctx, "entry-b")
	require.NoError(t, err)
	defer streamBRetry.Close()
	adapter.waitStarted(t)
	bodyBRetry, err := io.ReadAll(streamBRetry)
	require.NoError(t, err)
	require.Equal(t, "bb2", string(bodyBRetry))
	adapter.releaseOne()

	require.NoError(t, service.WaitForMerges(ctx))
	require.Equal(t, 1, adapter.maxActive())
}

func TestLongMaterializationRenewsOwnershipAcrossServiceInstances(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingComposeStorage(filesystem)
	lifecycleOptions := storagelifecycle.Options{
		MaterializationLeaseDuration: 80 * time.Millisecond,
		LeaseRenewalInterval:         20 * time.Millisecond,
	}
	firstLifecycle := storagelifecycle.NewWithOptions(client, lifecycleOptions)
	secondLifecycle := storagelifecycle.NewWithOptions(client, lifecycleOptions)
	first := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1, Lifecycle: firstLifecycle})
	second := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1, Lifecycle: secondLifecycle})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello ")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("world")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := first.Download(ctx, "entry-id")
	require.NoError(t, err)
	adapter.waitStarted(t)
	time.Sleep(140 * time.Millisecond)
	second.tryStartMaterialization(client.StorageLocation.GetX(ctx, location.ID))

	select {
	case <-adapter.started:
		t.Fatal("a renewed materialization lease was taken over by another service")
	case <-time.After(60 * time.Millisecond):
	}

	adapter.releaseOne()
	require.NoError(t, first.WaitForMerges(ctx))
	require.NoError(t, second.WaitForMerges(ctx))
	require.NoError(t, stream.Close())
	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Nil(t, current.MergeLeaseToken)
	require.Nil(t, current.MergeLeaseExpiresAt)
}

func TestWaitForMergesCancelsInFlightMergeAndReleasesLease(t *testing.T) {
	fixture := startBlockingMaterializationDownload(t, "hello", " world")

	fixture.service.StopAcceptingMerges()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := fixture.service.WaitForMerges(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.Eventually(t, func() bool {
		current := fixture.client.StorageLocation.GetX(fixture.ctx, fixture.location.ID)
		return current.MergeStartedAt == nil && current.MergeLeaseToken == nil && current.MergeLeaseExpiresAt == nil && current.MergedAt == nil
	}, time.Second, 10*time.Millisecond)

	_, err = fixture.filesystem.CreateDownloadStream(fixture.ctx, "folder/merged")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestFinalizeQueuesCleanupWithoutStorageIO(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &failDeleteStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("data")))

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.False(t, adapter.deleteCalled)
	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, strconv.FormatInt(upload.UploadID, 10)+"/blocks", task.FolderName)
	require.Zero(t, task.AttemptCount)
	require.Nil(t, task.LastAttemptedAt)
}

var errInjectedUploadFailure = errors.New("injected upload failure")
var errInjectedDeleteFailure = errors.New("injected delete failure")
var errInjectedStorageFailure = errors.New("injected storage failure")

type copyTrackingStorage struct {
	storage.Adapter
	copies [][2]string
}

type storageCallTrackingAdapter struct {
	storage.Adapter
	countCalls        int
	downloadCalls     int
	objectExistsCalls []string
	countErr          error
}

func (s *storageCallTrackingAdapter) CountFilesInFolder(ctx context.Context, folderName string) (int, error) {
	s.countCalls++
	if s.countErr != nil {
		return 0, s.countErr
	}
	return s.Adapter.CountFilesInFolder(ctx, folderName)
}

func (s *storageCallTrackingAdapter) CreateDownloadStream(ctx context.Context, objectName string) (io.ReadCloser, error) {
	s.downloadCalls++
	return s.Adapter.CreateDownloadStream(ctx, objectName)
}

func (s *storageCallTrackingAdapter) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	s.objectExistsCalls = append(s.objectExistsCalls, objectName)
	return s.Adapter.ObjectExists(ctx, objectName)
}

func (s *storageCallTrackingAdapter) reset() {
	s.countCalls = 0
	s.downloadCalls = 0
	s.objectExistsCalls = nil
}

func (s *copyTrackingStorage) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string) error {
	s.copies = append(s.copies, [2]string{sourceObjectName, destinationObjectName})
	return s.Adapter.CopyObject(ctx, sourceObjectName, destinationObjectName)
}

func readStorageObject(t *testing.T, ctx context.Context, adapter storage.Adapter, objectName string) string {
	t.Helper()

	stream, err := adapter.CreateDownloadStream(ctx, objectName)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	return string(body)
}

type failOnceStorage struct {
	storage.Adapter
	failed bool
}

func (s *failOnceStorage) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	if !s.failed {
		s.failed = true
		return errInjectedUploadFailure
	}
	return s.Adapter.UploadStream(ctx, objectName, stream)
}

type failDeleteStorage struct {
	storage.Adapter
	deleteCalled bool
}

func (s *failDeleteStorage) DeleteFolder(context.Context, string) error {
	s.deleteCalled = true
	return errInjectedDeleteFailure
}

type directURLStorage struct {
	storage.Adapter
	objectName string
}

type downloadErrorStorage struct {
	storage.Adapter
}

func (*downloadErrorStorage) CreateDownloadStream(context.Context, string) (io.ReadCloser, error) {
	return nil, errInjectedStorageFailure
}

type objectExistsErrorStorage struct {
	storage.Adapter
	err error
}

func (s *objectExistsErrorStorage) ObjectExists(context.Context, string) (bool, error) {
	return false, s.err
}

type blockingObjectExistsStorage struct {
	storage.Adapter
	objectName string
	started    chan struct{}
	release    chan struct{}
	once       sync.Once
}

type afterObjectExistsStorage struct {
	storage.Adapter
	once  sync.Once
	after func()
}

func (s *afterObjectExistsStorage) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	exists, err := s.Adapter.ObjectExists(ctx, objectName)
	if err == nil {
		s.once.Do(s.after)
	}
	return exists, err
}

func newBlockingObjectExistsStorage(adapter storage.Adapter, objectName string) *blockingObjectExistsStorage {
	return &blockingObjectExistsStorage{
		Adapter:    adapter,
		objectName: objectName,
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (s *blockingObjectExistsStorage) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	if objectName == s.objectName {
		s.once.Do(func() { close(s.started) })
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.release:
			return false, nil
		}
	}
	return s.Adapter.ObjectExists(ctx, objectName)
}

func (s *directURLStorage) CreateDownloadURL(_ context.Context, objectName string, _ time.Duration) (string, error) {
	s.objectName = objectName
	return "direct://" + objectName, nil
}

type trackingComposeStorage struct {
	storage.Adapter
	mu     sync.Mutex
	calls  int
	errors []error
}

func (s *trackingComposeStorage) ComposeObjects(ctx context.Context, destinationObjectName string, sourceObjectNames []string) error {
	s.mu.Lock()
	callIndex := s.calls
	s.calls++
	var err error
	if callIndex < len(s.errors) {
		err = s.errors[callIndex]
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return composeTestObjects(ctx, s.Adapter, destinationObjectName, sourceObjectNames)
}

func (s *trackingComposeStorage) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type blockingMaterializationDownloadFixture struct {
	ctx        context.Context
	client     *ent.Client
	filesystem *storage.FilesystemAdapter
	adapter    *blockingComposeStorage
	service    *Service
	location   *ent.StorageLocation
	stream     io.ReadCloser
}

func startBlockingMaterializationDownload(t *testing.T, contents ...string) *blockingMaterializationDownloadFixture {
	t.Helper()

	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingComposeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1})
	for index, content := range contents {
		require.NoError(t, filesystem.UploadStream(ctx, partObjectName("folder", index), bytes.NewBufferString(content)))
	}
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", len(contents))

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = stream.Close()
	})
	adapter.waitStarted(t)

	return &blockingMaterializationDownloadFixture{
		ctx:        ctx,
		client:     client,
		filesystem: filesystem,
		adapter:    adapter,
		service:    service,
		location:   location,
		stream:     stream,
	}
}

type blockingComposeStorage struct {
	storage.Adapter
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	active  int
	max     int
}

func newBlockingComposeStorage(adapter storage.Adapter) *blockingComposeStorage {
	return &blockingComposeStorage{
		Adapter: adapter,
		started: make(chan struct{}, 10),
		release: make(chan struct{}, 10),
	}
}

func (s *blockingComposeStorage) ComposeObjects(ctx context.Context, destinationObjectName string, sourceObjectNames []string) error {
	s.mu.Lock()
	s.active++
	if s.active > s.max {
		s.max = s.active
	}
	s.mu.Unlock()
	s.started <- struct{}{}

	select {
	case <-s.release:
	case <-ctx.Done():
		s.decrementActive()
		return ctx.Err()
	}
	err := composeTestObjects(ctx, s.Adapter, destinationObjectName, sourceObjectNames)
	s.decrementActive()
	return err
}

func (s *blockingComposeStorage) waitStarted(t *testing.T) {
	t.Helper()

	select {
	case <-s.started:
	case <-time.After(time.Second):
		t.Fatal("merge did not start")
	}
}

func (s *blockingComposeStorage) releaseOne() {
	s.release <- struct{}{}
}

func (s *blockingComposeStorage) maxActive() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

func (s *blockingComposeStorage) decrementActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
}

func composeTestObjects(ctx context.Context, adapter storage.Adapter, destinationObjectName string, sourceObjectNames []string) error {
	var body bytes.Buffer
	for _, sourceObjectName := range sourceObjectNames {
		stream, err := adapter.CreateDownloadStream(ctx, sourceObjectName)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(&body, stream)
		closeErr := stream.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return adapter.UploadStream(ctx, destinationObjectName, &body)
}

func newTestServiceDeps(t *testing.T) (context.Context, *ent.Client, *storage.FilesystemAdapter) {
	t.Helper()

	return testutil.NewSQLiteFilesystem(t)
}

func writableScope() auth.CacheScope {
	return auth.CacheScope{
		RepoID: "123",
		Scopes: []auth.Scope{
			{Scope: "refs/heads/main", Permission: 3},
		},
	}
}

func createMatchedCacheEntry(ctx context.Context, client *ent.Client, id string, key string) *ent.CacheEntry {
	location := client.StorageLocation.Create().
		SetID(id + "-location").
		SetFolderName(id + "-folder").
		SetPartCount(1).
		SaveX(ctx)
	return client.CacheEntry.Create().
		SetID(id).
		SetKey(key).
		SetVersion("version").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)
}

func createCacheEntryForDownload(ctx context.Context, client *ent.Client, entryID string, folderName string) *ent.StorageLocation {
	return createCacheEntryForDownloadWithPartCount(ctx, client, entryID, folderName, 1)
}

func createCacheEntryForDownloadWithPartCount(ctx context.Context, client *ent.Client, entryID string, folderName string, partCount int) *ent.StorageLocation {
	location := client.StorageLocation.Create().
		SetID(entryID + "-location").
		SetFolderName(folderName).
		SetPartCount(partCount).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID(entryID).
		SetKey(entryID + "-key").
		SetVersion("version").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)
	return location
}
