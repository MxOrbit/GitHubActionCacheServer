package e2e

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestUnsignedUploadURLIsRejected(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPut, "/upload/123", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"ok":false,"error":"upload not found"}`, rec.Body.String())
}

func TestSignedUploadURLWorksWithoutJWTAuth(t *testing.T) {
	router := newTestRouter(t)
	uploadURL := createCacheEntry(t, router, actionsToken(t), cacheBody("signed-upload-no-jwt"))

	req := httptest.NewRequest(http.MethodPut, uploadURL.RequestURI(), bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestSignedUploadURLWorksOnAliasRoute(t *testing.T) {
	router := newTestRouter(t)
	uploadURL := createCacheEntry(t, router, actionsToken(t), cacheBody("signed-upload-alias"))
	uploadURL.Path = strings.Replace(uploadURL.Path, "/devstoreaccount1/upload/", "/upload/", 1)

	req := httptest.NewRequest(http.MethodPut, uploadURL.RequestURI(), bytes.NewBufferString("payload"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestUploadRouteRejectsBadSignature(t *testing.T) {
	router := newTestRouter(t)
	uploadURL := createCacheEntry(t, router, actionsToken(t), cacheBody("signed-upload-bad-sig"))

	unsignedReq := httptest.NewRequest(http.MethodPut, uploadURL.Path, bytes.NewBufferString("payload"))
	unsignedRec := httptest.NewRecorder()
	router.ServeHTTP(unsignedRec, unsignedReq)
	require.Equal(t, http.StatusNotFound, unsignedRec.Code)
	require.JSONEq(t, `{"ok":false,"error":"upload not found"}`, unsignedRec.Body.String())

	tampered := *uploadURL
	tamperedQuery := tampered.Query()
	tamperedQuery.Set("sig", "deadbeef")
	tampered.RawQuery = tamperedQuery.Encode()
	tamperedReq := httptest.NewRequest(http.MethodPut, tampered.RequestURI(), bytes.NewBufferString("payload"))
	tamperedRec := httptest.NewRecorder()
	router.ServeHTTP(tamperedRec, tamperedReq)
	require.Equal(t, http.StatusNotFound, tamperedRec.Code)
	require.JSONEq(t, `{"ok":false,"error":"upload not found"}`, tamperedRec.Body.String())
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
