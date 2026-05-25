package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestFallbackProxyForwardsUnknownPath(t *testing.T) {
	type proxiedRequest struct {
		Method      string
		Host        string
		Path        string
		RawQuery    string
		Custom      string
		ContentType string
		Body        string
	}

	proxied := make(chan proxiedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		proxied <- proxiedRequest{
			Method:      r.Method,
			Host:        r.Host,
			Path:        r.URL.Path,
			RawQuery:    r.URL.RawQuery,
			Custom:      r.Header.Get("X-Custom"),
			ContentType: r.Header.Get("Content-Type"),
			Body:        string(body),
		}

		w.Header().Set("X-Upstream", "ok")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := config.Load()
	cfg.Server.DefaultActionsResultsURL = upstream.URL + "/api"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})
	cacheServer := httptest.NewServer(router)
	defer cacheServer.Close()

	req, err := http.NewRequest(http.MethodPatch, cacheServer.URL+"/results/workflow?attempt=1&name=cache", strings.NewReader("payload"))
	require.NoError(t, err)
	req.Header.Set("X-Custom", "custom-value")
	req.Header.Set("Content-Type", "text/plain")
	res, err := cacheServer.Client().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	responseBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, "ok", res.Header.Get("X-Upstream"))
	require.Equal(t, "proxied", string(responseBody))

	got := <-proxied
	require.Equal(t, http.MethodPatch, got.Method)
	require.Equal(t, strings.TrimPrefix(upstream.URL, "http://"), got.Host)
	require.Equal(t, "/api/results/workflow", got.Path)
	require.Equal(t, "attempt=1&name=cache", got.RawQuery)
	require.Equal(t, "custom-value", got.Custom)
	require.Equal(t, "text/plain", got.ContentType)
	require.Equal(t, "payload", got.Body)
}

func TestFallbackProxyDoesNotHandleManagementMisses(t *testing.T) {
	proxied := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := config.Load()
	cfg.Server.DefaultActionsResultsURL = upstream.URL
	cfg.Management.APIKey = "secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})

	req := httptest.NewRequest(http.MethodGet, "/management-api/_missing", nil)
	req.Header.Set("x-api-key", "secret")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"management endpoint not found"}`, rec.Body.String())

	select {
	case <-proxied:
		t.Fatal("management miss was proxied")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestManagementDocsAndSpec(t *testing.T) {
	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := config.Load()
	cfg.Management.APIKey = "secret"
	cfg.Server.APIBaseURL = "https://cache.example"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})

	docsReq := httptest.NewRequest(http.MethodGet, "/management-api/_docs", nil)
	docsRec := httptest.NewRecorder()
	router.ServeHTTP(docsRec, docsReq)

	require.Equal(t, http.StatusOK, docsRec.Code)
	require.Contains(t, docsRec.Header().Get("Content-Type"), "text/html")
	require.Equal(t, "*", docsRec.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, docsRec.Body.String(), "Cache Server Management API")

	specReq := httptest.NewRequest(http.MethodGet, "/management-api/_docs/spec.json", nil)
	specRec := httptest.NewRecorder()
	router.ServeHTTP(specRec, specReq)

	require.Equal(t, http.StatusOK, specRec.Code)
	var spec struct {
		Info struct {
			Title   string `json:"title"`
			Version string `json:"version"`
		} `json:"info"`
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
		Paths map[string]any `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(specRec.Body.Bytes(), &spec))
	require.Equal(t, "Cache Server Management API", spec.Info.Title)
	require.Equal(t, "1.0.0", spec.Info.Version)
	require.Equal(t, "https://cache.example/management-api", spec.Servers[0].URL)
	require.Contains(t, spec.Paths, "/cache-entries/")
	require.Contains(t, spec.Paths, "/storage-locations/{id}")
}

func TestManagementCORSPreflightDoesNotRequireAPIKey(t *testing.T) {
	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := config.Load()
	cfg.Management.APIKey = "secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})

	req := httptest.NewRequest(http.MethodOptions, "/management-api/cache-entries/", nil)
	req.Header.Set("Origin", "https://admin.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), http.MethodGet)
	require.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "X-Api-Key")
}

func TestManagementRPCFindMany(t *testing.T) {
	ctx, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := config.Load()
	cfg.Management.APIKey = "secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})

	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("entry-id").
		SetKey("linux-cache").
		SetVersion("version-1").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)

	req := httptest.NewRequest(http.MethodPost, "/management-api/_rpc/cacheEntries/findMany", strings.NewReader(`{"json":{"key":"linux-cache"},"meta":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var rpcResponse struct {
		JSON struct {
			Total int `json:"total"`
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"json"`
		Meta []any `json:"meta"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rpcResponse))
	require.Equal(t, 1, rpcResponse.JSON.Total)
	require.Equal(t, "entry-id", rpcResponse.JSON.Items[0].ID)
	require.Empty(t, rpcResponse.Meta)
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
