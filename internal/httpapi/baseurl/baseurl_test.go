package baseurl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromRequestUsesOverride(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)

	require.Equal(t, "https://cache.example", FromRequest(req, " https://cache.example/ "))
}

func TestFromRequestUsesForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "cache.example")

	require.Equal(t, "https://cache.example", FromRequest(req, ""))
}

func TestFromRequestUsesFirstForwardedValue(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)
	req.Header.Set("X-Forwarded-Proto", "https, http")
	req.Header.Set("X-Forwarded-Host", "cache.example, proxy.local")

	require.Equal(t, "https://cache.example", FromRequest(req, ""))
}

func TestFromRequestFallsBackToRequestHost(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://cache.local/health", nil)

	require.Equal(t, "http://cache.local", FromRequest(req, ""))
}
