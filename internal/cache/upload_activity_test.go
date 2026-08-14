package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/uploadsession"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestCreateUploadTakesOverInactiveReservation(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	now := time.Unix(1_800_000_000, 0)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Now:     func() time.Time { return now },
	})
	scope := writableScope()

	first, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	now = now.Add(uploadsession.TakeoverIdleTimeout + time.Second)

	second, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.NotEqual(t, first.UploadID, second.UploadID)
	require.Equal(t, 1, client.Upload.Query().CountX(ctx))
	_, err = client.Upload.Get(ctx, first.UploadID)
	require.True(t, ent.IsNotFound(err))
	task := client.StorageDeletion.Query().OnlyX(ctx)
	require.Equal(t, int64ToString(first.UploadID), task.FolderName)
}

func TestCreateUploadKeepsActiveReservation(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	now := time.Unix(1_800_000_000, 0)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Now:     func() time.Time { return now },
	})
	scope := writableScope()

	first, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	now = now.Add(uploadsession.TakeoverIdleTimeout - time.Second)

	_, err = service.CreateUpload(ctx, "key", "version", scope)
	require.ErrorIs(t, err, ErrUploadAlreadyExists)
	require.Equal(t, first.UploadID, client.Upload.Query().OnlyX(ctx).ID)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
}

func TestConcurrentUploadTakeoverCreatesOneReservation(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	now := time.Unix(1_800_000_000, 0)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Now:     func() time.Time { return now },
	})
	scope := writableScope()
	_, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	now = now.Add(uploadsession.TakeoverIdleTimeout + time.Second)

	const attempts = 10
	results := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, createErr := service.CreateUpload(ctx, "key", "version", scope)
			results <- createErr
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for createErr := range results {
		switch {
		case createErr == nil:
			successes++
		case errors.Is(createErr, ErrUploadAlreadyExists):
			conflicts++
		default:
			t.Fatalf("unexpected takeover error: %v", createErr)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, attempts-1, conflicts)
	require.Equal(t, 1, client.Upload.Query().CountX(ctx))
}

func TestCreateUploadHandlesDuplicateLegacyReservations(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	now := time.Unix(1_800_000_000, 0)
	service := NewService(Options{
		DB:      client,
		Storage: filesystem,
		Now:     func() time.Time { return now },
	})
	scope := writableScope()
	for id := int64(1); id <= maxUploadContentionAttempts; id++ {
		client.Upload.Create().
			SetID(id).
			SetKey("key").
			SetVersion("version").
			SetScope(scope.Scopes[0].Scope).
			SetRepoId(scope.RepoID).
			SetCreatedAt(now.Add(-uploadsession.TakeoverIdleTimeout - time.Second).UnixMilli()).
			SetFolderName(strconv.FormatInt(id, 10)).
			SaveX(ctx)
	}

	created, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	require.Greater(t, created.UploadID, int64(0))
	require.Equal(t, 1, client.Upload.Query().CountX(ctx))
	require.Equal(t, maxUploadContentionAttempts, client.StorageDeletion.Query().CountX(ctx))
}

func TestDeleteUploadIfInactiveDoesNotDeleteRefreshedUpload(t *testing.T) {
	ctx, client, _ := newTestServiceDeps(t)
	now := time.Unix(1_800_000_000, 0)
	currentUpload := createTestUpload(t, ctx, client, now.Add(-uploadsession.TakeoverIdleTimeout-time.Minute))
	staleSnapshot := client.Upload.GetX(ctx, currentUpload.ID)
	client.Upload.UpdateOneID(currentUpload.ID).SetLastPartUploadedAt(now.UnixMilli()).ExecX(ctx)

	result, err := uploadsession.DeleteIfInactive(
		ctx,
		client,
		staleSnapshot.ID,
		staleSnapshot.FolderName,
		now.Add(-uploadsession.TakeoverIdleTimeout).UnixMilli(),
	)
	require.NoError(t, err)
	require.False(t, result.Deleted)
	require.Equal(t, currentUpload.ID, client.Upload.Query().OnlyX(ctx).ID)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
}

func TestUploadActivityRenewalLossCancelsStorageIO(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	adapter := newContextBlockingUploadStorage(filesystem)
	service := NewService(Options{
		DB:                      client,
		Storage:                 adapter,
		UploadHeartbeatInterval: 10 * time.Millisecond,
	})
	upload, err := service.CreateUpload(ctx, "key", "version", writableScope())
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		result <- service.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("data"))
	}()
	adapter.waitStarted(t)
	client.Upload.DeleteOneID(upload.UploadID).ExecX(ctx)

	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrUploadNotFound)
	case <-time.After(2 * time.Second):
		t.Fatal("upload was not canceled after its activity row disappeared")
	}
}

func TestUploadPartMapsMissingFinalUploadRow(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	var uploadID int64
	adapter := &afterUploadStorage{
		Adapter: filesystem,
		after: func() {
			client.Upload.DeleteOneID(uploadID).ExecX(ctx)
		},
	}
	service := NewService(Options{DB: client, Storage: adapter})
	upload, err := service.CreateUpload(ctx, "key", "version", writableScope())
	require.NoError(t, err)
	uploadID = upload.UploadID

	err = service.UploadPart(ctx, uploadID, bytes.NewBufferString("data"))
	require.ErrorIs(t, err, ErrUploadNotFound)
}

func TestBlockListCommitMapsMissingFinalUploadRow(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	var uploadID int64
	adapter := &afterCopyStorage{
		Adapter: filesystem,
		after: func() {
			client.Upload.DeleteOneID(uploadID).ExecX(ctx)
		},
	}
	service := NewService(Options{DB: client, Storage: adapter})
	upload, err := service.CreateUpload(ctx, "key", "version", writableScope())
	require.NoError(t, err)
	uploadID = upload.UploadID
	require.NoError(t, service.UploadBlock(ctx, uploadID, "block", bytes.NewBufferString("data")))

	err = service.CommitBlockList(ctx, uploadID, []string{"block"})
	require.ErrorIs(t, err, ErrUploadNotFound)
}

func TestCompleteUploadMapsMissingFinalUploadRow(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	var uploadID int64
	adapter := &afterInspectStorage{
		Adapter: filesystem,
		after: func() {
			client.Upload.DeleteOneID(uploadID).ExecX(ctx)
		},
	}
	service := NewService(Options{DB: client, Storage: adapter})
	scope := writableScope()
	upload, err := service.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)
	uploadID = upload.UploadID
	require.NoError(t, service.UploadPart(ctx, uploadID, bytes.NewBufferString("data")))

	_, err = service.CompleteUpload(ctx, "key", "version", scope)
	require.ErrorIs(t, err, ErrUploadNotFound)
	require.Zero(t, client.CacheEntry.Query().CountX(ctx))
	require.Zero(t, client.StorageLocation.Query().CountX(ctx))
}

func TestDeleteUploadMapsMissingRow(t *testing.T) {
	ctx, client, filesystem := newTestServiceDeps(t)
	var logs bytes.Buffer
	logger := zerolog.New(&logs).Level(zerolog.DebugLevel)
	service := NewService(Options{DB: client, Storage: filesystem, Logger: &logger})
	currentUpload := createTestUpload(t, ctx, client, time.Now())
	client.Upload.DeleteOneID(currentUpload.ID).ExecX(ctx)

	err := service.deleteUpload(ctx, currentUpload)
	require.ErrorIs(t, err, ErrUploadNotFound)
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))

	service.deleteUploadBestEffort(ctx, currentUpload)
	require.Contains(t, logs.String(), `"level":"debug"`)
	require.NotContains(t, logs.String(), `"level":"error"`)
}

func createTestUpload(t *testing.T, ctx context.Context, client *ent.Client, createdAt time.Time) *ent.Upload {
	t.Helper()
	return client.Upload.Create().
		SetID(42).
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetCreatedAt(createdAt.UnixMilli()).
		SetFolderName("42").
		SaveX(ctx)
}

func int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

type contextBlockingUploadStorage struct {
	storage.Adapter
	started chan struct{}
	once    sync.Once
}

func newContextBlockingUploadStorage(adapter storage.Adapter) *contextBlockingUploadStorage {
	return &contextBlockingUploadStorage{Adapter: adapter, started: make(chan struct{})}
}

func (s *contextBlockingUploadStorage) UploadStream(ctx context.Context, _ string, _ io.Reader) error {
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *contextBlockingUploadStorage) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
}

type afterUploadStorage struct {
	storage.Adapter
	after func()
	once  sync.Once
}

func (s *afterUploadStorage) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	if err := s.Adapter.UploadStream(ctx, objectName, stream); err != nil {
		return err
	}
	s.once.Do(s.after)
	return nil
}

type afterCopyStorage struct {
	storage.Adapter
	after func()
	once  sync.Once
}

func (s *afterCopyStorage) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string) error {
	if err := s.Adapter.CopyObject(ctx, sourceObjectName, destinationObjectName); err != nil {
		return err
	}
	s.once.Do(s.after)
	return nil
}

type afterInspectStorage struct {
	storage.Adapter
	after func()
	once  sync.Once
}

func (s *afterInspectStorage) InspectIndexedFolderSizes(ctx context.Context, folderName string, expectedObjects int) ([]int64, error) {
	sizes, err := s.Adapter.InspectIndexedFolderSizes(ctx, folderName, expectedObjects)
	if err == nil {
		s.once.Do(s.after)
	}
	return sizes, err
}
