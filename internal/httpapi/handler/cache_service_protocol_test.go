package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestBindCacheRequestRejectsOversizedBodies(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{name: "json", contentType: "application/json"},
		{name: "protobuf", contentType: protobufContentType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, recorder := newCacheRequestTestContext(
				t,
				tt.contentType,
				bytes.NewReader(bytes.Repeat([]byte{'x'}, maxCacheServiceRequestBodyBytes+1)),
			)
			c.Request.ContentLength = -1

			_, _, _, ok := bindCacheRequest(c, decodeCreateCacheEntryRequest)

			require.False(t, ok)
			require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)
			require.JSONEq(t, `{"ok":false,"error":"request body too large"}`, recorder.Body.String())
		})
	}
}

func TestBindCacheRequestAcceptsLargestOfficialToolkitJSONShape(t *testing.T) {
	// The official toolkit permits at most ten total keys of 512 JavaScript
	// characters. NUL forces the largest JSON escape (six bytes per character),
	// making this a conservative upper bound for the request it can generate.
	key := strings.Repeat("\x00", 512)
	raw, err := json.Marshal(getCacheEntryDownloadURLRequest{
		Key:         key,
		RestoreKeys: []string{key, key, key, key, key, key, key, key, key},
		Version:     strings.Repeat("a", 64),
	})
	require.NoError(t, err)
	require.Greater(t, len(raw), 30<<10)
	require.Less(t, len(raw), maxCacheServiceRequestBodyBytes)

	c, recorder := newCacheRequestTestContext(t, "application/json", bytes.NewReader(raw))
	c.Set("cache_scope", auth.CacheScope{RepoID: "repository"})

	body, scope, wireFormat, ok := bindCacheRequest(c, decodeGetCacheEntryDownloadURLRequest)

	require.True(t, ok)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, cacheWireJSON, wireFormat)
	require.Equal(t, "repository", scope.RepoID)
	require.Equal(t, key, body.Key)
	require.Len(t, body.RestoreKeys, 9)
}

func newCacheRequestTestContext(t *testing.T, contentType string, body *bytes.Reader) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", body)
	c.Request.Header.Set("Content-Type", contentType)
	return c, recorder
}
