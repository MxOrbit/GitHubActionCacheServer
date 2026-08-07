package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// This mirrors the v2 HTTP shape used by actions/cache. Runner-level
// compatibility still needs coverage with falcondev's patched runner image.
func TestActionsCacheV2CompatibleHTTPProtocolSaveAndRestore(t *testing.T) {
	app := newTestApp(t)
	server := httptest.NewServer(app.router)
	t.Cleanup(server.Close)

	token := actionsToken(t)
	key := fmt.Sprintf("actions-cache-v2-%d", time.Now().UnixNano())
	createBody := cacheBody(key)
	createResponse := postServerJSON[createCacheResponse](t, server, createCacheEntryPath, token, createBody)
	require.True(t, createResponse.OK)
	require.NotEmpty(t, createResponse.SignedUploadURL)

	cacheArchive := bytes.Repeat([]byte("actions-cache-v2-content\n"), 128*1024)
	chunks := chunkBytes(cacheArchive, 1024*1024)
	orderedBlockIDs := make([]string, len(chunks))
	blockIDPrefix := uuid.NewString()
	for index := range chunks {
		orderedBlockIDs[index] = actionsCacheBlockID(blockIDPrefix, index)
	}

	for index := len(chunks) - 1; index >= 0; index-- {
		uploadURL := mustParseURL(t, createResponse.SignedUploadURL)
		query := uploadURL.Query()
		query.Set("comp", "block")
		query.Set("blockid", orderedBlockIDs[index])
		uploadURL.RawQuery = query.Encode()

		putServerBody(t, server.Client(), uploadURL.String(), chunks[index])
	}

	commitBlockList(t, server.Client(), createResponse.SignedUploadURL, orderedBlockIDs)

	finalizeResponse := postServerJSON[finalizeCacheResponse](t, server, finalizeCacheEntryPath, token, createBody)
	require.True(t, finalizeResponse.OK)
	require.NotEmpty(t, finalizeResponse.EntryID)

	matchResponse := postServerJSON[matchCacheResponse](t, server, getCacheEntryDownloadPath, token, map[string]any{
		"key":          key,
		"restore_keys": []string{"actions-cache-v2-"},
		"version":      defaultCacheEntryVersion,
	})
	require.True(t, matchResponse.OK)
	require.Equal(t, key, matchResponse.MatchedKey)

	restoredArchive := getServerBody(t, server.Client(), matchResponse.SignedDownloadURL)
	require.Equal(t, cacheArchive, restoredArchive)
}

func actionsCacheBlockID(prefix string, index int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s%012d", prefix, index)))
}

func chunkBytes(body []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	for len(body) > 0 {
		size := chunkSize
		if len(body) < size {
			size = len(body)
		}
		chunks = append(chunks, body[:size])
		body = body[size:]
	}
	return chunks
}

func postServerJSON[T any](t *testing.T, server *httptest.Server, path string, token string, body any) T {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	res, err := server.Client().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	responseBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, string(responseBody))

	var response T
	require.NoError(t, json.Unmarshal(responseBody, &response))
	return response
}

func putServerBody(t *testing.T, client *http.Client, rawURL string, body []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, rawURL, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := client.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	responseBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode, string(responseBody))
	require.NotEmpty(t, res.Header.Get(uploadRequestIDHeaderName))
}

func commitBlockList(t *testing.T, client *http.Client, rawUploadURL string, blockIDs []string) {
	t.Helper()

	body, err := xml.Marshal(blockList{Latest: blockIDs})
	require.NoError(t, err)

	uploadURL := mustParseURL(t, rawUploadURL)
	query := uploadURL.Query()
	query.Set("comp", "blocklist")
	uploadURL.RawQuery = query.Encode()

	putServerBody(t, client, uploadURL.String(), body)
}

func getServerBody(t *testing.T, client *http.Client, rawURL string) []byte {
	t.Helper()

	res, err := client.Get(rawURL)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	return body
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed
}

type blockList struct {
	XMLName xml.Name `xml:"BlockList"`
	Latest  []string `xml:"Latest"`
}
