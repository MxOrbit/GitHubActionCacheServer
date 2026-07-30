package downloadurl

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignerSignsAndVerifiesDownloadURL(t *testing.T) {
	signer := New("secret", 10*time.Minute)
	now := time.Unix(100, 0)
	signer.now = func() time.Time { return now }

	signed, err := signer.Sign("http://cache.example/download/entry-id", "entry-id")
	require.NoError(t, err)
	require.Contains(t, signed, "expires=")
	require.Contains(t, signed, "signature=")

	parsed, err := url.Parse(signed)
	require.NoError(t, err)
	require.True(t, signer.Verify("entry-id", parsed.Query().Get("expires"), parsed.Query().Get("signature")))
	require.False(t, signer.Verify("other-entry-id", parsed.Query().Get("expires"), parsed.Query().Get("signature")))
}

func TestSignerRejectsExpiredURL(t *testing.T) {
	signer := New("secret", 10*time.Minute)
	signer.now = func() time.Time { return time.Unix(1000, 0) }

	require.False(t, signer.Verify("entry-id", "999", "bad"))
}

func TestSignerReturnsInvalidURLError(t *testing.T) {
	signer := New("secret", 10*time.Minute)

	signed, err := signer.Sign("http://%", "entry-id")

	require.Error(t, err)
	require.Empty(t, signed)
}
