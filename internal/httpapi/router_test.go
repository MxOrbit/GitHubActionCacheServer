package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/downloadurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestHealthRoutes(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "root", path: "/", body: "OK"},
		{name: "health", path: "/health", body: "healthy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, tt.body, rec.Body.String())
		})
	}
}

func TestUploadRouteIsRegistered(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPut, "/upload/123", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"upload not found"}`, rec.Body.String())
}

func TestDownloadSurfacesImmediateMergeUploadFailure(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("body")))
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("entry-id").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)

	cfg := config.Load()
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: failMergedUploadStorage{Adapter: filesystem},
	})

	signedURL := downloadurl.New("test-secret", time.Minute).Sign("http://cache.test/download/entry-id", "entry-id")
	req := httptest.NewRequest(http.MethodGet, signedURL, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"merge upload failed"}`, rec.Body.String())
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	return NewRouter(zerolog.Nop(), config.Load(), Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})
}

var errMergeUploadFailed = errors.New("merge upload failed")

type failMergedUploadStorage struct {
	storage.Adapter
}

func (s failMergedUploadStorage) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	if strings.HasSuffix(objectName, "/merged") {
		return errMergeUploadFailed
	}
	return s.Adapter.UploadStream(ctx, objectName, stream)
}
