package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
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

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	return NewRouter(zerolog.Nop(), config.Load(), Dependencies{
		DB:      client,
		Storage: storageAdapter,
	})
}
