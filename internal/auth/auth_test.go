package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const testIssuer = "https://token.actions.githubusercontent.com"

func newJWKSVerifier(t *testing.T, jwks *testutil.JWKS) *Verifier {
	t.Helper()

	verifier, err := NewVerifier(t.Context(), Options{
		Issuer:  testIssuer,
		JWKSURL: jwks.Server.URL,
	})
	require.NoError(t, err)
	return verifier
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss":           testIssuer,
		"ac":            `[{"Scope":"refs/heads/main","Permission":3}]`,
		"repository_id": "123",
		"exp":           time.Now().Add(time.Hour).Unix(),
	}
}

func bearer(token string) string {
	return "Bearer " + token
}

func TestCacheScopeAcceptsValidToken(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	scope, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, validClaims())))
	require.NoError(t, err)
	require.Equal(t, "123", scope.RepoID)
}

func TestCacheScopeUnknownKIDIsUnavailableEvenWhenRefreshRateLimited(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	// First unknown KID consumes the unknown-KID refresh budget.
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, "unknown-1", validClaims())))
	require.ErrorIs(t, err, ErrKeyUnavailable)

	// The rate limiter rejects the second refresh outright; it must still be
	// a 503-class server failure, never a 401.
	_, err = verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, "unknown-2", validClaims())))
	require.ErrorIs(t, err, ErrKeyUnavailable)
}

func TestCacheScopeNonStringKIDIsUnavailable(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	// Intentional: key resolution faults map to 503 even for malformed kids.
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, 123, validClaims())))
	require.ErrorIs(t, err, ErrKeyUnavailable)
}

func TestCacheScopeRejectsNonRS256Token(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims())
	token.Header["kid"] = jwks.KID
	signed, err := token.SignedString([]byte("attacker-controlled"))
	require.NoError(t, err)

	_, err = verifier.CacheScope(context.Background(), bearer(signed))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestCacheScopeRejectsBadSignature(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, validClaims())
	token.Header["kid"] = jwks.KID
	signed, err := token.SignedString(otherKey)
	require.NoError(t, err)

	_, err = verifier.CacheScope(context.Background(), bearer(signed))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestCacheScopeRejectsExpiredToken(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	claims := validClaims()
	claims["exp"] = time.Now().Add(-time.Hour).Unix()
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, claims)))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestCacheScopeRejectsTokenWithoutExpiration(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	claims := validClaims()
	delete(claims, "exp")
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, claims)))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestCacheScopeAcceptsJustExpiredTokenWithinLeeway(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	claims := validClaims()
	claims["exp"] = time.Now().Add(-10 * time.Second).Unix()
	scope, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, claims)))
	require.NoError(t, err)
	require.Equal(t, "123", scope.RepoID)
}

func TestCacheScopeRejectsTokenExpiredBeyondLeeway(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	claims := validClaims()
	claims["exp"] = time.Now().Add(-40 * time.Second).Unix()
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, claims)))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestCacheScopeRejectsWrongIssuer(t *testing.T) {
	jwks := testutil.NewJWKS(t)
	verifier := newJWKSVerifier(t, jwks)

	claims := validClaims()
	claims["iss"] = "https://evil.example"
	_, err := verifier.CacheScope(context.Background(), bearer(jwks.Sign(t, jwks.KID, claims)))
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestNewVerifierRejectsMalformedJWKSURL(t *testing.T) {
	_, err := NewVerifier(context.Background(), Options{
		Issuer:  testIssuer,
		JWKSURL: "://invalid",
	})
	require.Error(t, err)
}

func TestNewVerifierRejectsUnreachableJWKS(t *testing.T) {
	server := httptest.NewServer(nil)
	server.Close()

	_, err := NewVerifier(t.Context(), Options{
		Issuer:  testIssuer,
		JWKSURL: server.URL,
	})
	require.Error(t, err)
}

func TestCacheScopeSkipValidationDecodesWithoutVerification(t *testing.T) {
	verifier, err := NewVerifier(context.Background(), Options{SkipValidation: true})
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"ac":            `[{"Scope":"refs/heads/main","Permission":3}]`,
		"repository_id": "123",
	})
	signed, err := token.SignedString([]byte("mock-secret-key"))
	require.NoError(t, err)

	scope, err := verifier.CacheScope(context.Background(), bearer(signed))
	require.NoError(t, err)
	require.Equal(t, "123", scope.RepoID)
}
