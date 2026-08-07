package testutil

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// JWKS is a local JWKS endpoint backed by a single RSA key.
type JWKS struct {
	Server *httptest.Server
	Key    *rsa.PrivateKey
	KID    string
}

func NewJWKS(t testing.TB) *JWKS {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	j := &JWKS{Key: key, KID: "test-kid"}
	body := fmt.Sprintf(
		`{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":%q,"n":%q,"e":%q}]}`,
		j.KID,
		base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
	)
	j.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(j.Server.Close)

	return j
}

// Sign issues an RS256 token carrying the given kid and claims.
func (j *JWKS) Sign(t testing.TB, kid any, claims jwt.MapClaims) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(j.Key)
	require.NoError(t, err)
	return signed
}
