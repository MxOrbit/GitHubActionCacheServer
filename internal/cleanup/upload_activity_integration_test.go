package cleanup_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cleanup"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestActiveUploadSurvivesCleanupAfterLongLogicalTime(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	adapter := newBlockingUploadStorage(filesystem)
	t.Cleanup(adapter.releaseUpload)
	var clockMillis atomic.Int64
	clockMillis.Store(time.Unix(1_800_000_000, 0).UnixMilli())
	now := func() time.Time { return time.UnixMilli(clockMillis.Load()) }
	uploads := cache.NewService(cache.Options{
		DB:                      client,
		Storage:                 adapter,
		Now:                     now,
		UploadHeartbeatInterval: 10 * time.Millisecond,
	})
	cleaner := cleanup.NewService(cleanup.Options{DB: client, Storage: filesystem, Now: now})
	scope := auth.CacheScope{
		RepoID: "repo",
		Scopes: []auth.Scope{{Scope: "refs/heads/main", Permission: 2}},
	}
	upload, err := uploads.CreateUpload(ctx, "key", "version", scope)
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		result <- uploads.UploadPart(ctx, upload.UploadID, bytes.NewBufferString("data"))
	}()
	adapter.waitStarted(t)
	advanced := now().Add(40 * time.Minute)
	clockMillis.Store(advanced.UnixMilli())

	require.Eventually(t, func() bool {
		currentUpload, queryErr := client.Upload.Get(ctx, upload.UploadID)
		return queryErr == nil && currentUpload.LastPartUploadedAt != nil && *currentUpload.LastPartUploadedAt >= advanced.UnixMilli()
	}, 2*time.Second, 5*time.Millisecond)

	deleted, err := cleaner.RunUploads(ctx)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, upload.UploadID, client.Upload.Query().OnlyX(ctx).ID)

	adapter.releaseUpload()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("active upload did not finish")
	}
	require.Equal(t, 1, client.Upload.GetX(ctx, upload.UploadID).FinishedPartUploadCount)
}

type blockingUploadStorage struct {
	storage.Adapter
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingUploadStorage(adapter storage.Adapter) *blockingUploadStorage {
	return &blockingUploadStorage{
		Adapter: adapter,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingUploadStorage) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	s.startedOnce.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return s.Adapter.UploadStream(ctx, objectName, stream)
	}
}

func (s *blockingUploadStorage) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
}

func (s *blockingUploadStorage) releaseUpload() {
	s.releaseOnce.Do(func() { close(s.release) })
}
