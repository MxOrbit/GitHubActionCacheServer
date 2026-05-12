package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestHealthRoutes(t *testing.T) {
	router := NewRouter(zerolog.Nop())

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

func TestPlaceholderRoutes(t *testing.T) {
	router := NewRouter(zerolog.Nop())
	req := httptest.NewRequest(http.MethodPut, "/upload/123", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotImplemented, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"not implemented"}`, rec.Body.String())
}
