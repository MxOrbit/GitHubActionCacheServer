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

func (s *Signer) Sign(rawURL string, cacheEntryID string) string {
	expires := s.now().Add(s.ttl).Unix()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	query := parsed.Query()
	query.Set("expires", strconv.FormatInt(expires, 10))
	query.Set("signature", s.signature(cacheEntryID, expires))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func (s *Signer) Verify(cacheEntryID string, expiresValue string, signature string) bool {
	expires, err := strconv.ParseInt(expiresValue, 10, 64)
	if err != nil {
		return false
	}
	if !s.now().Before(time.Unix(expires, 0)) {
		return false
	}

	expected := s.signature(cacheEntryID, expires)
	return hmac.Equal([]byte(signature), []byte(expected))
}

func (s *Signer) signature(cacheEntryID string, expires int64) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(cacheEntryID))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(strconv.FormatInt(expires, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}
