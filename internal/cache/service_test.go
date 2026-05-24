package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cleanup"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
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

func TestDownloadStreamsPartsWhileMergeRunsInBackground(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingMergeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1})
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello")))
	location := createCacheEntryForDownload(ctx, client, "entry-id", "folder")

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	defer stream.Close()
	adapter.waitStarted(t)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.NotNil(t, current.MergeStartedAt)
	require.Nil(t, current.MergedAt)

	body := make([]byte, len("hello"))
	_, err = io.ReadFull(stream, body)
	require.NoError(t, err)
	require.Equal(t, "hello", string(body))

	adapter.releaseOne()
	rest, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return current.MergedAt != nil
	}, time.Second, 10*time.Millisecond)
}

func TestFirstDownloadPartsRemainUntilReturnedStreamCloses(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	service := NewService(Options{DB: client, Storage: filesystem, MergeConcurrency: 1})
	cleanupService := cleanup.NewService(cleanup.Options{
		DB:      client,
		Storage: filesystem,
		Config:  config.CleanupConfig{CacheOlderThanDays: 90},
	})

	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("world")))
	location := createCacheEntryForDownloadWithPartCount(ctx, client, "entry-id", "folder", 2)

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	defer stream.Close()

	firstPart := make([]byte, len("hello"))
	_, err = io.ReadFull(stream, firstPart)
	require.NoError(t, err)
	require.Equal(t, "hello", string(firstPart))

	time.Sleep(50 * time.Millisecond)

	current := client.StorageLocation.GetX(ctx, location.ID)
	require.Nil(t, current.MergedAt)

	deleted, err := cleanupService.RunParts(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)

	rest, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "world", string(rest))

	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return current.MergedAt != nil
	}, time.Second, 10*time.Millisecond)

	deleted, err = cleanupService.RunParts(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
}

func TestBackgroundMergesRespectConcurrencyLimit(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingMergeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1})

	require.NoError(t, filesystem.UploadStream(ctx, "folder-a/parts/0", bytes.NewBufferString("a")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder-b/parts/0", bytes.NewBufferString("b")))
	createCacheEntryForDownload(ctx, client, "entry-a", "folder-a")
	createCacheEntryForDownload(ctx, client, "entry-b", "folder-b")

	streamA, err := service.Download(ctx, "entry-a")
	require.NoError(t, err)
	defer streamA.Close()
	bodyA := make([]byte, len("a"))
	_, err = io.ReadFull(streamA, bodyA)
	require.NoError(t, err)
	require.Equal(t, "a", string(bodyA))

	streamB, err := service.Download(ctx, "entry-b")
	require.NoError(t, err)
	defer streamB.Close()
	adapter.waitStarted(t)

	select {
	case <-adapter.started:
		t.Fatal("second merge started before first merge released")
	case <-time.After(50 * time.Millisecond):
	}

	adapter.releaseOne()
	restA, err := io.ReadAll(streamA)
	require.NoError(t, err)
	require.Empty(t, restA)
	require.NoError(t, service.WaitForMerges(ctx))
	require.Equal(t, 1, adapter.maxActive())

	currentB := client.StorageLocation.GetX(ctx, "entry-b-location")
	require.Nil(t, currentB.MergeStartedAt)
	require.Nil(t, currentB.MergedAt)

	streamBRetry, err := service.Download(ctx, "entry-b")
	require.NoError(t, err)
	defer streamBRetry.Close()
	adapter.waitStarted(t)
	bodyB := make([]byte, len("b"))
	_, err = io.ReadFull(streamBRetry, bodyB)
	require.NoError(t, err)
	require.Equal(t, "b", string(bodyB))
	adapter.releaseOne()
	restB, err := io.ReadAll(streamBRetry)
	require.NoError(t, err)
	require.Empty(t, restB)

	require.NoError(t, service.WaitForMerges(ctx))
	require.Equal(t, 1, adapter.maxActive())
}

func TestWaitForMergesCancelsInFlightMergeAndClearsMergeStart(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newBlockingMergeStorage(filesystem)
	service := NewService(Options{DB: client, Storage: adapter, MergeConcurrency: 1})

	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("hello")))
	location := createCacheEntryForDownload(ctx, client, "entry-id", "folder")

	stream, err := service.Download(ctx, "entry-id")
	require.NoError(t, err)
	defer stream.Close()
	adapter.waitStarted(t)

	service.StopAcceptingMerges()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err = service.WaitForMerges(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return current.MergeStartedAt == nil && current.MergedAt == nil
	}, time.Second, 10*time.Millisecond)

	_, err = filesystem.CreateDownloadStream(ctx, "folder/merged")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
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

type blockingMergeStorage struct {
	storage.Adapter
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	active  int
	max     int
}

func newBlockingMergeStorage(adapter storage.Adapter) *blockingMergeStorage {
	return &blockingMergeStorage{
		Adapter: adapter,
		started: make(chan struct{}, 10),
		release: make(chan struct{}, 10),
	}
}

func (s *blockingMergeStorage) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	if !strings.HasSuffix(objectName, "/merged") {
		return s.Adapter.UploadStream(ctx, objectName, stream)
	}

	s.mu.Lock()
	s.active++
	if s.active > s.max {
		s.max = s.active
	}
	s.mu.Unlock()
	s.started <- struct{}{}

	err := s.Adapter.UploadStream(ctx, objectName, stream)
	if err != nil {
		s.decrementActive()
		return err
	}

	select {
	case <-s.release:
	case <-ctx.Done():
		s.decrementActive()
		return ctx.Err()
	}
	s.decrementActive()
	return nil
}

func (s *blockingMergeStorage) waitStarted(t *testing.T) {
	t.Helper()

	select {
	case <-s.started:
	case <-time.After(time.Second):
		t.Fatal("merge did not start")
	}
}

func (s *blockingMergeStorage) releaseOne() {
	s.release <- struct{}{}
}

func (s *blockingMergeStorage) maxActive() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

func (s *blockingMergeStorage) decrementActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
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
