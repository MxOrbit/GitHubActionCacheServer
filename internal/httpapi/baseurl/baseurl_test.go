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

func TestFromRequestRejectsMalformedForwardedHost(t *testing.T) {
	for _, host := range []string{
		"attacker.com/x?a=",
		"evil.com@proxy.local",
		"evil .com",
		"cache.example:abc",
		"cache.example:99999",
		"::1",
		"",
	} {
		req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)
		req.Header.Set("X-Forwarded-Host", host)

		require.Equal(t, "http://internal.local", FromRequest(req, ""), "host %q", host)
	}
}

func TestFromRequestAcceptsBracketedIPv6ForwardedHost(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "[::1]:3000")

	require.Equal(t, "https://[::1]:3000", FromRequest(req, ""))
}

func TestFromRequestValidatesForwardedProto(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://internal.local/health", nil)
	req.Header.Set("X-Forwarded-Host", "cache.example")

	req.Header.Set("X-Forwarded-Proto", "javascript")
	require.Equal(t, "http://cache.example", FromRequest(req, ""))

	req.Header.Set("X-Forwarded-Proto", "HTTPS")
	require.Equal(t, "https://cache.example", FromRequest(req, ""))
}

func TestValidAuthority(t *testing.T) {
	valid := []string{
		"cache.example", "cache.example:8080", "cache.example:65535", "localhost", "cache_server:3000",
		"cache.example.", "1.2.3.4", "1.2.3.4:80",
		"[::1]", "[::1]:3000", "[2001:db8::1]", "[::ffff:1.2.3.4]",
	}
	for _, s := range valid {
		require.True(t, validAuthority(s), "%q", s)
	}

	invalid := []string{
		"", "attacker.com/x?a=", "evil.com@proxy.local", "evil .com",
		"cache.example:abc", "cache.example:+80", "cache.example:99999", "cache.example:",
		"cache.example:0", "cache.example:65536", "cache.example:655355", ":8080",
		"::1", "[abc]", "[abc", "abc]", "[a:1", "[::1]extra", "[::1]:",
		"[fe80::1%eth0]", "[1.2.3.4]", "a..b", "[", "[]", ".",
	}
	for _, s := range invalid {
		require.False(t, validAuthority(s), "%q", s)
	}
}
