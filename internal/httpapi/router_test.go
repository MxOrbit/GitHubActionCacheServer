package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/downloadurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/gin-gonic/gin"
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

func TestBlockListRejectsMissingUploadBeforeReadingBody(t *testing.T) {
	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := newTestConfig(t)
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
	})
	body := &readCountingBody{}
	// The 404 below only proves the path once the signature is accepted;
	// that acceptance is pinned by TestBlockListRejectsOversizedBody (413).
	req := httptest.NewRequest(http.MethodPut, signedBlockListRequestURI(t, "test-secret", 123), body)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Zero(t, body.reads)
}

func TestBlockListRejectsOversizedBody(t *testing.T) {
	ctx, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cacheService := cache.NewService(cache.Options{DB: client, Storage: storageAdapter})
	upload, err := cacheService.CreateUpload(ctx, "key", "version", auth.CacheScope{
		RepoID: "repository",
		Scopes: []auth.Scope{{Scope: "refs/heads/main", Permission: 3}},
	})
	require.NoError(t, err)
	cfg := newTestConfig(t)
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Cache:    cacheService,
		Verifier: newSkipVerifier(t),
	})
	body := "<BlockList>" + strings.Repeat(" ", (8<<20)+1) + "</BlockList>"
	req := httptest.NewRequest(http.MethodPut, signedBlockListRequestURI(t, "test-secret", upload.UploadID), strings.NewReader(body))
	req.ContentLength = -1
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"block list body too large"}`, rec.Body.String())
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
	cfg := newTestConfig(t)
	cfg.Server.DefaultActionsResultsURL = upstream.URL + "/api"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
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

func TestFallbackProxyTimesOutWaitingForResponseHeaders(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		upstream.Close()
	})

	router := gin.New()
	router.NoRoute(fallbackProxyWithResponseHeaderTimeout(zerolog.Nop(), upstream.URL, 20*time.Millisecond))
	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec := httptest.NewRecorder()
	startedAt := time.Now()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"bad gateway"}`, rec.Body.String())
	require.Less(t, time.Since(startedAt), time.Second)
}

func TestFallbackProxyDoesNotHandleManagementMisses(t *testing.T) {
	proxied := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := newTestConfig(t)
	cfg.Server.DefaultActionsResultsURL = upstream.URL
	cfg.Management.APIKey = "secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
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
	cfg := newTestConfig(t)
	cfg.Management.APIKey = "secret"
	cfg.Server.APIBaseURL = "https://cache.example"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
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

func TestManagementDocsIgnoresForwardedHeaders(t *testing.T) {
	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := newTestConfig(t)
	cfg.Management.APIKey = "secret"
	cfg.Server.APIBaseURL = ""
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
	})

	specReq := httptest.NewRequest(http.MethodGet, "/management-api/_docs/spec.json", nil)
	specReq.Header.Set("X-Forwarded-Host", "attacker.example")
	specReq.Header.Set("X-Forwarded-Proto", "https")
	specRec := httptest.NewRecorder()
	router.ServeHTTP(specRec, specReq)

	require.Equal(t, http.StatusOK, specRec.Code)
	require.Equal(t, "no-store", specRec.Header().Get("Cache-Control"))
	var spec struct {
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
	}
	require.NoError(t, json.Unmarshal(specRec.Body.Bytes(), &spec))
	require.Equal(t, "/management-api", spec.Servers[0].URL)

	docsReq := httptest.NewRequest(http.MethodGet, "/management-api/_docs", nil)
	docsRec := httptest.NewRecorder()
	router.ServeHTTP(docsRec, docsReq)

	require.Equal(t, http.StatusOK, docsRec.Code)
	require.Equal(t, "no-store", docsRec.Header().Get("Cache-Control"))
}

func TestManagementCORSPreflightDoesNotRequireAPIKey(t *testing.T) {
	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg := newTestConfig(t)
	cfg.Management.APIKey = "secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
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

func TestDownloadDoesNotSurfaceBackgroundMaterializationFailure(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("body")))
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/1", bytes.NewBufferString("-tail")))
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(2).
		SetSizeBytes(int64(len("body-tail"))).
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

	cfg := newTestConfig(t)
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  failComposeStorage{Adapter: filesystem},
		Verifier: newSkipVerifier(t),
	})

	signedURL, err := downloadurl.New("test-secret", time.Minute).Sign("http://cache.test/download/entry-id", "entry-id")
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, signedURL, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, strconv.Itoa(len("body-tail")), rec.Header().Get("Content-Length"))
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "attachment", rec.Header().Get("Content-Disposition"))
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "body-tail", rec.Body.String())
	require.Eventually(t, func() bool {
		current := client.StorageLocation.GetX(ctx, location.ID)
		return current.MergeStartedAt == nil && current.MergeLeaseToken == nil && current.MergeLeaseExpiresAt == nil && current.MergedAt == nil
	}, time.Second, 10*time.Millisecond)
}

func TestDownloadWithoutSizeBytesStillPinsContentHeaders(t *testing.T) {
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

	cfg := newTestConfig(t)
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  filesystem,
		Verifier: newSkipVerifier(t),
	})

	signedURL, err := downloadurl.New("test-secret", time.Minute).Sign("http://cache.test/download/entry-id", "entry-id")
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, signedURL, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Header().Get("Content-Length"))
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "attachment", rec.Header().Get("Content-Disposition"))
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "body", rec.Body.String())
}

func TestDownloadLengthMismatchDoesNotReusePayloadContentLengthForError(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	mergedAt := time.Now().UnixMilli()
	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SetSizeBytes(4).
		SetMergedAt(mergedAt).
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

	cfg := newTestConfig(t)
	cfg.Cache.DownloadURLSigningSecret = "test-secret"
	router := NewRouter(zerolog.Nop(), cfg, Dependencies{
		DB:       client,
		Storage:  shortDownloadStorage{Adapter: filesystem},
		Verifier: newSkipVerifier(t),
	})

	signedURL, err := downloadurl.New("test-secret", time.Minute).Sign("http://cache.test/download/entry-id", "entry-id")
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, signedURL, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.NotEqual(t, "4", rec.Header().Get("Content-Length"))
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	require.Empty(t, rec.Header().Get("Content-Disposition"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.JSONEq(t, `{"ok":false,"error":"internal server error"}`, rec.Body.String())
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	return NewRouter(zerolog.Nop(), newTestConfig(t), Dependencies{
		DB:       client,
		Storage:  storageAdapter,
		Verifier: newSkipVerifier(t),
	})
}

func signedBlockListRequestURI(t *testing.T, secret string, uploadID int64) string {
	t.Helper()
	signedURL, err := downloadurl.New(secret, time.Hour).SignUpload("/upload/"+strconv.FormatInt(uploadID, 10), uploadID)
	require.NoError(t, err)
	parsedURL, err := url.Parse(signedURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	query.Set("comp", "blocklist")
	parsedURL.RawQuery = query.Encode()
	return parsedURL.RequestURI()
}

func newSkipVerifier(t *testing.T) *auth.Verifier {
	t.Helper()

	verifier, err := auth.NewVerifier(context.Background(), auth.Options{SkipValidation: true})
	require.NoError(t, err)
	return verifier
}

func newTestConfig(t *testing.T) config.Config {
	t.Helper()

	t.Setenv("API_BASE_URL", "")
	cfg, err := config.Load()
	require.NoError(t, err)
	return cfg
}

var errComposeFailed = errors.New("compose failed")

type readCountingBody struct {
	reads int
}

func (b *readCountingBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("body should not be read")
}

type failComposeStorage struct {
	storage.Adapter
}

func (s failComposeStorage) ComposeObjects(context.Context, string, []string) error {
	return errComposeFailed
}

type shortDownloadStorage struct {
	storage.Adapter
}

func (s shortDownloadStorage) CreateDownloadStream(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
