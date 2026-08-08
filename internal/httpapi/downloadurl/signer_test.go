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

	parsed, err := url.Parse(signed)
	require.NoError(t, err)
	require.True(t, parsed.Query().Has("expires"))
	require.True(t, parsed.Query().Has("sig"))
	require.False(t, parsed.Query().Has("signature"))
	require.True(t, signer.Verify("entry-id", parsed.Query().Get("expires"), parsed.Query().Get("sig")))
	require.False(t, signer.Verify("other-entry-id", parsed.Query().Get("expires"), parsed.Query().Get("sig")))
}

func TestSignerSignsAndVerifiesUploadURL(t *testing.T) {
	signer := New("secret", 24*time.Hour)
	now := time.Unix(100, 0)
	signer.now = func() time.Time { return now }

	signed, err := signer.SignUpload("http://cache.example/devstoreaccount1/upload/12345", 12345)
	require.NoError(t, err)

	parsed, err := url.Parse(signed)
	require.NoError(t, err)
	require.True(t, parsed.Query().Has("expires"))
	require.True(t, parsed.Query().Has("sig"))
	require.False(t, parsed.Query().Has("signature"))
	expires := parsed.Query().Get("expires")
	sig := parsed.Query().Get("sig")
	require.True(t, signer.VerifyUpload(12345, expires, sig))
	require.False(t, signer.VerifyUpload(54321, expires, sig))
	require.False(t, signer.VerifyUpload(12345, expires+"1", sig))
	tampered := sig[:len(sig)-1]
	if sig[len(sig)-1] == '0' {
		tampered += "1"
	} else {
		tampered += "0"
	}
	require.False(t, signer.VerifyUpload(12345, expires, tampered))
}

func TestSignerRejectsExpiredUploadURL(t *testing.T) {
	signer := New("secret", 10*time.Minute)
	signer.now = func() time.Time { return time.Unix(100, 0) }
	signed, err := signer.SignUpload("http://cache.example/upload/123", 123)
	require.NoError(t, err)
	parsed, err := url.Parse(signed)
	require.NoError(t, err)

	signer.now = func() time.Time { return time.Unix(100+10*60, 0) }
	require.False(t, signer.VerifyUpload(123, parsed.Query().Get("expires"), parsed.Query().Get("sig")))
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
