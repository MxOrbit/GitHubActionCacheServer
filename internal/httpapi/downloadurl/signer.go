package downloadurl

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"time"
)

type Signer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

func New(secret string, ttl time.Duration) *Signer {
	if secret != "" {
		return &Signer{secret: []byte(secret), ttl: ttl, now: time.Now}
	}

	generated := make([]byte, 32)
	if _, err := rand.Read(generated); err != nil {
		panic(err)
	}
	return &Signer{secret: generated, ttl: ttl, now: time.Now}
}

func (s *Signer) Sign(rawURL string, cacheEntryID string) (string, error) {
	expires := s.now().Add(s.ttl).Unix()
	return s.sign(rawURL, expires, s.signature(cacheEntryID, expires))
}

func (s *Signer) Verify(cacheEntryID string, expiresValue string, signature string) bool {
	expires, ok := s.unexpired(expiresValue)
	return ok && hmac.Equal([]byte(signature), []byte(s.signature(cacheEntryID, expires)))
}

func (s *Signer) SignUpload(rawURL string, uploadID int64) (string, error) {
	expires := s.now().Add(s.ttl).Unix()
	return s.sign(rawURL, expires, s.uploadSignature(uploadID, expires))
}

func (s *Signer) VerifyUpload(uploadID int64, expiresValue string, sig string) bool {
	expires, ok := s.unexpired(expiresValue)
	return ok && hmac.Equal([]byte(sig), []byte(s.uploadSignature(uploadID, expires)))
}

// sign attaches expiry and signature query parameters. The HMAC covers only
// the ID and expiry: clients rebuild the query string, so the URL itself,
// parameter order, comp, and blockid must stay unsigned.
func (s *Signer) sign(rawURL string, expires int64, sig string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("expires", strconv.FormatInt(expires, 10))
	query.Set("sig", sig)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (s *Signer) unexpired(expiresValue string) (int64, bool) {
	expires, err := strconv.ParseInt(expiresValue, 10, 64)
	if err != nil {
		return 0, false
	}
	return expires, s.now().Before(time.Unix(expires, 0))
}

// TODO: add a "download\n" domain prefix once the legacy signature-param fallback is removed.
func (s *Signer) signature(cacheEntryID string, expires int64) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(cacheEntryID))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(strconv.FormatInt(expires, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Signer) uploadSignature(uploadID int64, expires int64) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte("upload\n"))
	mac.Write([]byte(strconv.FormatInt(uploadID, 10)))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(strconv.FormatInt(expires, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}
