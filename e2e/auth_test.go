package e2e

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestCacheServiceRequiresBearerToken(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := httpapi.NewRouter(zerolog.Nop())

	req := httptest.NewRequest(
		http.MethodPost,
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		nil,
	)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"authorization header missing or malformed"}`, rec.Body.String())
}

func TestCacheServiceAcceptsDecodedActionsTokenWhenValidationIsSkipped(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := httpapi.NewRouter(zerolog.Nop())
	token := actionsToken(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotImplemented, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"not implemented"}`, rec.Body.String())
}

func TestUploadRouteDoesNotUseJWTAuth(t *testing.T) {
	router := httpapi.NewRouter(zerolog.Nop())
	req := httptest.NewRequest(http.MethodPut, "/upload/123", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotImplemented, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"not implemented"}`, rec.Body.String())
}

func actionsToken(t *testing.T) string {
	t.Helper()

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"ac":            `[{"Scope":"refs/heads/main","Permission":3}]`,
		"repository_id": "123",
	}).SignedString([]byte("mock-secret-key"))

	require.NoError(t, err)
	return token
}
