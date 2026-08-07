package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestCacheServiceRequiresBearerToken(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		nil,
	)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"unauthorized"}`, rec.Body.String())
}

func TestCacheServiceReturnsUnavailableWhenKeyUnknown(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier, err := auth.NewVerifier(t.Context(), auth.Options{
		Issuer:  config.DefaultTokenIssuer,
		JWKSURL: jwks.Server.URL,
	})
	require.NoError(t, err)

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	router := httpapi.NewRouter(zerolog.Nop(), cfg, httpapi.Dependencies{
		DB:        client,
		Storage:   storageAdapter,
		Lifecycle: storagelifecycle.New(client),
		Verifier:  verifier,
	})

	token := jwks.Sign(t, "unknown-kid", jwt.MapClaims{
		"iss":           config.DefaultTokenIssuer,
		"ac":            `[{"Scope":"refs/heads/main","Permission":3}]`,
		"repository_id": "123",
		"exp":           time.Now().Add(time.Hour).Unix(),
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "300", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"ok":false,"error":"service unavailable"}`, rec.Body.String())
}

func TestCacheServiceAcceptsDecodedActionsTokenWhenValidationIsSkipped(t *testing.T) {
	router := newTestRouter(t)
	token := actionsToken(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"invalid body"}`, rec.Body.String())
}

func TestUploadRouteDoesNotUseJWTAuth(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPut, "/upload/123", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"upload not found"}`, rec.Body.String())
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

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	return newTestApp(t).router
}

func newSkipVerifier(t *testing.T) *auth.Verifier {
	t.Helper()

	verifier, err := auth.NewVerifier(context.Background(), auth.Options{SkipValidation: true})
	require.NoError(t, err)
	return verifier
}

type testApp struct {
	router    http.Handler
	db        *ent.Client
	storage   storage.Adapter
	lifecycle *storagelifecycle.Service
}

func newTestApp(t *testing.T) testApp {
	t.Helper()

	_, client, storageAdapter := testutil.NewSQLiteFilesystem(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	lifecycle := storagelifecycle.New(client)
	router := httpapi.NewRouter(zerolog.Nop(), cfg, httpapi.Dependencies{
		DB:        client,
		Storage:   storageAdapter,
		Lifecycle: lifecycle,
		Verifier:  newSkipVerifier(t),
	})

	return testApp{
		router:    router,
		db:        client,
		storage:   storageAdapter,
		lifecycle: lifecycle,
	}
}
