package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/stretchr/testify/require"
)

const (
	createCacheEntryPath      = "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"
	finalizeCacheEntryPath    = "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload"
	getCacheEntryDownloadPath = "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL"
	defaultCacheEntryVersion  = "version-1"
	uploadRequestIDHeaderName = "x-ms-request-id"
)

type createCacheResponse struct {
	OK              bool   `json:"ok"`
	SignedUploadURL string `json:"signed_upload_url"`
}

type finalizeCacheResponse struct {
	OK      bool   `json:"ok"`
	EntryID string `json:"entry_id"`
}

type matchCacheResponse struct {
	OK                bool   `json:"ok"`
	SignedDownloadURL string `json:"signed_download_url"`
	MatchedKey        string `json:"matched_key"`
}

func TestSQLiteFilesystemSaveAndRestore(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	createBody := cacheBody("linux-build-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadRec := uploadWholeCache(t, router, uploadURL, "cache-content")
	require.NotEmpty(t, uploadRec.Header().Get(uploadRequestIDHeaderName))

	finalizeResponse := finalizeCacheEntry(t, router, token, createBody)
	require.NotEmpty(t, finalizeResponse.EntryID)

	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":          "linux-build-cache",
		"restore_keys": []string{"linux-"},
		"version":      defaultCacheEntryVersion,
	})
	require.Equal(t, "linux-build-cache", matchResponse.MatchedKey)

	downloadURL := parseSignedURL(t, matchResponse.SignedDownloadURL)
	require.NotEmpty(t, downloadURL.Query().Get("expires"))
	require.NotEmpty(t, downloadURL.Query().Get("signature"))

	unsignedDownloadReq := httptest.NewRequest(http.MethodGet, downloadURL.Path, nil)
	unsignedDownloadRec := httptest.NewRecorder()
	router.ServeHTTP(unsignedDownloadRec, unsignedDownloadReq)
	require.Equal(t, http.StatusUnauthorized, unsignedDownloadRec.Code)

	require.Equal(t, "cache-content", downloadCache(t, router, downloadURL))
}

func TestFinalizeAcceptsOfficialToolkitJSONSizeBytes(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	createBody := cacheBody("toolkit-json-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadWholeCache(t, router, uploadURL, "cache-content")

	// The official JavaScript toolkit follows the proto3 JSON mapping and
	// sends int64 fields as quoted strings.
	finalizeResponse := finalizeCacheEntry(t, router, token, map[string]string{
		"key":        "toolkit-json-cache",
		"version":    defaultCacheEntryVersion,
		"size_bytes": "13",
	})
	require.NotEmpty(t, finalizeResponse.EntryID)

	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":     "toolkit-json-cache",
		"version": defaultCacheEntryVersion,
	})
	downloadURL := parseSignedURL(t, matchResponse.SignedDownloadURL)
	require.Equal(t, "cache-content", downloadCache(t, router, downloadURL))
}

func TestDownloadPinsContentHeadersAgainstSniffing(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	payload := "<html><head><script>alert(1)</script></head><body>pwn</body></html>"

	createBody := cacheBody("download-headers-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadWholeCache(t, router, uploadURL, payload)
	finalizeCacheEntry(t, router, token, createBody)

	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":     "download-headers-cache",
		"version": defaultCacheEntryVersion,
	})
	require.Equal(t, "download-headers-cache", matchResponse.MatchedKey)

	// Only a real net/http server reproduces the content sniffing this guards
	// against: gin writes the status header before the body, and
	// httptest.ResponseRecorder skips DetectContentType once that happens.
	srv := httptest.NewServer(router)
	defer srv.Close()

	downloadURL := parseSignedURL(t, matchResponse.SignedDownloadURL)
	resp, err := http.Get(srv.URL + downloadURL.RequestURI())
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, payload, string(body))
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.NotContains(t, resp.Header.Get("Content-Type"), "text/html")
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, "attachment", resp.Header.Get("Content-Disposition"))
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
}

func TestPrometheusMetricsTrackCacheProtocolOutcomes(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	metricsBefore := httptest.NewRecorder()
	router.ServeHTTP(metricsBefore, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, metricsBefore.Code)
	require.Contains(t, metricsBefore.Body.String(), `cache_requests_total{result="hit"} 0`)
	require.Contains(t, metricsBefore.Body.String(), `cache_requests_total{result="miss"} 0`)
	require.Contains(t, metricsBefore.Body.String(), "go_goroutines")
	require.Contains(t, metricsBefore.Body.String(), "process_cpu_seconds_total")

	failedFinalize := postJSON(t, router, finalizeCacheEntryPath, token, cacheBody("missing-upload"))
	require.Equal(t, http.StatusNotFound, failedFinalize.Code)

	miss := postJSON(t, router, getCacheEntryDownloadPath, token, map[string]any{
		"key":     "missing-metrics-cache",
		"version": defaultCacheEntryVersion,
	})
	require.Equal(t, http.StatusOK, miss.Code)
	require.JSONEq(t, `{"ok":false}`, miss.Body.String())

	createBody := cacheBody("metrics-cache-entry")
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadWholeCache(t, router, uploadURL, "payload")
	finalizeCacheEntry(t, router, token, createBody)
	matchCacheEntry(t, router, token, map[string]any{
		"key":          "another-missing-key",
		"restore_keys": []string{"metrics-cache-"},
		"version":      defaultCacheEntryVersion,
	})

	metricsAfter := httptest.NewRecorder()
	router.ServeHTTP(metricsAfter, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, metricsAfter.Code)
	require.Contains(t, metricsAfter.Body.String(), `cache_requests_total{result="hit"} 1`)
	require.Contains(t, metricsAfter.Body.String(), `cache_requests_total{result="miss"} 1`)
	require.Contains(t, metricsAfter.Body.String(), "cache_uploads_total 1")
	require.Contains(t, metricsAfter.Body.String(), "cache_storage_bytes 7")
}

func TestBlockUploadRetryIsIdempotent(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	createBody := cacheBody("retry-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)
	for _, body := range []string{"first attempt", "retry attempt"} {
		uploadWholeCache(t, router, uploadURL, body)
	}

	finalizeCacheEntry(t, router, token, createBody)
	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":     "retry-cache",
		"version": defaultCacheEntryVersion,
	})

	downloadURL := parseSignedURL(t, matchResponse.SignedDownloadURL)
	require.Equal(t, "retry attempt", downloadCache(t, router, downloadURL))
}

func TestOpaqueAzureBlockIDsUseBlockListOrder(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	createBody := cacheBody("opaque-block-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)

	firstBlockID := "b3BhcXVlLWJsb2NrLWZpcnN0"
	secondBlockID := "b3BhcXVlLWJsb2NrLXNlY29uZA=="
	for _, block := range []struct {
		id   string
		body string
	}{
		{id: secondBlockID, body: "world"},
		{id: firstBlockID, body: "hello "},
	} {
		uploadReq := httptest.NewRequest(
			http.MethodPut,
			uploadURL.RequestURI()+"?comp=block&blockid="+url.QueryEscape(block.id),
			bytes.NewBufferString(block.body),
		)
		uploadRec := httptest.NewRecorder()
		router.ServeHTTP(uploadRec, uploadReq)
		require.Equal(t, http.StatusCreated, uploadRec.Code)
	}

	blockList := `<BlockList><Latest>` + firstBlockID + `</Latest><Latest>` + secondBlockID + `</Latest></BlockList>`
	commitReq := httptest.NewRequest(http.MethodPut, uploadURL.RequestURI()+"?comp=blocklist", bytes.NewBufferString(blockList))
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	require.Equal(t, http.StatusCreated, commitRec.Code)

	commitRetryReq := httptest.NewRequest(http.MethodPut, uploadURL.RequestURI()+"?comp=blocklist", bytes.NewBufferString(blockList))
	commitRetryRec := httptest.NewRecorder()
	router.ServeHTTP(commitRetryRec, commitRetryReq)
	require.Equal(t, http.StatusCreated, commitRetryRec.Code)

	finalizeCacheEntry(t, router, token, createBody)
	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":     "opaque-block-cache",
		"version": defaultCacheEntryVersion,
	})

	downloadURL := parseSignedURL(t, matchResponse.SignedDownloadURL)
	require.Equal(t, "hello world", downloadCache(t, router, downloadURL))
}

func TestBlankRestoreKeysAreIgnored(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	createBody := cacheBody("unrelated-cache")
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadWholeCache(t, router, uploadURL, "unrelated")
	finalizeCacheEntry(t, router, token, createBody)

	matchRec := postJSON(t, router, getCacheEntryDownloadPath, token, map[string]any{
		"key":          "missing-cache",
		"restore_keys": []string{""},
		"version":      defaultCacheEntryVersion,
	})
	require.Equal(t, http.StatusOK, matchRec.Code)
	require.JSONEq(t, `{"ok":false}`, matchRec.Body.String())
}

func TestJSONLookupSelfHealsDanglingCacheEntryAsCleanMiss(t *testing.T) {
	app := newTestApp(t)
	token := actionsToken(t)
	body := cacheBody("dangling-json-cache")
	uploadURL := createCacheEntry(t, app.router, token, body)
	uploadWholeCache(t, app.router, uploadURL, "data")
	finalizeCacheEntry(t, app.router, token, body)
	require.NoError(t, app.storage.Clear(context.Background()))

	matchRec := postJSON(t, app.router, getCacheEntryDownloadPath, token, map[string]any{
		"key":     "dangling-json-cache",
		"version": defaultCacheEntryVersion,
	})

	require.Equal(t, http.StatusOK, matchRec.Code)
	require.JSONEq(t, `{"ok":false}`, matchRec.Body.String())
	require.Zero(t, app.db.CacheEntry.Query().CountX(context.Background()))
	location := app.db.StorageLocation.Query().OnlyX(context.Background())
	require.NotNil(t, location.DeletionRequestedAt)
}

func TestAbandonedUploadDoesNotBlockNewSave(t *testing.T) {
	app := newTestApp(t)
	token := actionsToken(t)

	body := cacheBody("abandoned-cache")
	firstCreate := postJSON(t, app.router, createCacheEntryPath, token, body)
	require.Equal(t, http.StatusOK, firstCreate.Code)

	_, err := app.db.Upload.Update().
		Where(upload.Key("abandoned-cache")).
		SetCreatedAt(time.Now().Add(-25 * time.Hour).UnixMilli()).
		Save(context.Background())
	require.NoError(t, err)

	createResponse := createCacheEntryResponse(t, app.router, token, body)
	require.True(t, createResponse.OK)
	require.NotEmpty(t, createResponse.SignedUploadURL)
}

func cacheBody(key string) map[string]string {
	return map[string]string{
		"key":     key,
		"version": defaultCacheEntryVersion,
	}
}

func createCacheEntry(t *testing.T, router http.Handler, token string, body any) *url.URL {
	t.Helper()

	response := createCacheEntryResponse(t, router, token, body)
	require.True(t, response.OK)
	require.NotEmpty(t, response.SignedUploadURL)
	return parseSignedURL(t, response.SignedUploadURL)
}

func createCacheEntryResponse(t *testing.T, router http.Handler, token string, body any) createCacheResponse {
	t.Helper()

	rec := postJSON(t, router, createCacheEntryPath, token, body)
	require.Equal(t, http.StatusOK, rec.Code)

	var response createCacheResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	return response
}

func finalizeCacheEntry(t *testing.T, router http.Handler, token string, body any) finalizeCacheResponse {
	t.Helper()

	rec := postJSON(t, router, finalizeCacheEntryPath, token, body)
	require.Equal(t, http.StatusOK, rec.Code)

	var response finalizeCacheResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.OK)
	return response
}

func matchCacheEntry(t *testing.T, router http.Handler, token string, body any) matchCacheResponse {
	t.Helper()

	rec := postJSON(t, router, getCacheEntryDownloadPath, token, body)
	require.Equal(t, http.StatusOK, rec.Code)

	var response matchCacheResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.OK)
	require.NotEmpty(t, response.SignedDownloadURL)
	return response
}

func parseSignedURL(t *testing.T, rawURL string) *url.URL {
	t.Helper()

	signedURL, err := url.Parse(rawURL)
	require.NoError(t, err)
	return signedURL
}

func uploadWholeCache(t *testing.T, router http.Handler, uploadURL *url.URL, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPut, uploadURL.RequestURI(), bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	return rec
}

func downloadCache(t *testing.T, router http.Handler, downloadURL *url.URL) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, downloadURL.RequestURI(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

func postJSON(t *testing.T, router http.Handler, path string, token string, body any) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}
