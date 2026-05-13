package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
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

	err = service.UploadPart(ctx, upload.UploadID, 0, bytes.NewBufferString("failed"))
	require.ErrorIs(t, err, errInjectedUploadFailure)

	require.NoError(t, service.UploadPart(ctx, upload.UploadID, 0, bytes.NewBufferString("ok")))
	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
}

func TestPrimaryKeyRequiresExactMatch(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("entry-id").
		SetKey("linux-cache-old").
		SetVersion("version").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)

	match, err := service.MatchCacheEntry(ctx, []string{"linux-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.Nil(t, match)

	match, err = service.MatchCacheEntry(ctx, []string{"missing-primary", "linux-cache"}, "version", writableScope())
	require.NoError(t, err)
	require.NotNil(t, match)
	require.Equal(t, "linux-cache-old", match.Key)
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
}

func TestMergeLocationDoesNotStartFromStaleMergedLocation(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	mergedAt := time.Now().UnixMilli()
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SetMergedAt(mergedAt).
		SaveX(ctx)
	staleLocation := *location
	staleLocation.MergedAt = nil

	err := service.mergeLocation(ctx, &staleLocation)
	require.ErrorIs(t, err, errMergeAlreadyStarted)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Equal(t, mergedAt, *current.MergedAt)
}

func TestFailedMergeCleanupDoesNotClearCompletedMerge(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	mergeStartedAt := time.Now().UnixMilli()
	mergedAt := mergeStartedAt + 1
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SetMergeStartedAt(mergeStartedAt).
		SetMergedAt(mergedAt).
		SaveX(ctx)

	require.NoError(t, service.clearOwnMergeStart(ctx, location.ID, mergeStartedAt))

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergeStartedAt)
	require.NotNil(t, current.MergedAt)
	require.Equal(t, mergeStartedAt, *current.MergeStartedAt)
	require.Equal(t, mergedAt, *current.MergedAt)
}

func TestMergeLocationKeepsPartsForConcurrentReaders(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("part")))
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(ctx)

	require.NoError(t, service.mergeLocation(ctx, location))

	count, err := filesystem.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergedAt)
	require.Nil(t, current.PartsDeletedAt)
}

func TestFinalizeCleanupFailureDoesNotFailCommittedUpload(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := &failDeleteStorage{Adapter: filesystem}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()

	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NoError(t, service.UploadPart(ctx, upload.UploadID, 0, bytes.NewBufferString("data")))

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.True(t, adapter.deleteCalled)
}

var errInjectedUploadFailure = errors.New("injected upload failure")
var errInjectedDeleteFailure = errors.New("injected delete failure")

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
