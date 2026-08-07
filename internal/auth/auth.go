package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

var (
	ErrInvalidToken         = errors.New("invalid token")
	ErrMissingAuthorization = errors.New("authorization header missing or malformed")
	ErrMissingCacheScopes   = errors.New("token does not contain cache scopes")
	ErrInvalidCacheScopes   = errors.New("invalid JSON in cache scopes")
	ErrNoCacheScopes        = errors.New("token does not contain any cache scopes")
	ErrMissingRepositoryID  = errors.New("token does not contain repository id")
	// ErrKeyUnavailable marks server-side key resolution failures (degraded
	// JWKS), mapped to 503 instead of a client-fault 401.
	ErrKeyUnavailable = errors.New("verification key unavailable")
)

const (
	jwksHTTPTimeout           = 15 * time.Second
	jwksRateLimitWaitMax      = 15 * time.Second
	jwksRefreshInterval       = 15 * time.Minute
	jwksRefreshUnknownKIDRate = 5 * time.Minute
)

type Scope struct {
	Scope      string `json:"Scope"`
	Permission int    `json:"Permission"`
}

type CacheScope struct {
	Scopes []Scope
	RepoID string
}

type Options struct {
	Issuer         string
	JWKSURL        string
	SkipValidation bool
	Logger         *zerolog.Logger
}

type Verifier struct {
	issuer         string
	skipValidation bool
	jwks           keyfunc.Keyfunc
}

type claims struct {
	AC           string `json:"ac"`
	RepositoryID string `json:"repository_id"`
	jwt.RegisteredClaims
}

func NewVerifier(ctx context.Context, options Options) (*Verifier, error) {
	verifier := &Verifier{
		issuer:         options.Issuer,
		skipValidation: options.SkipValidation,
	}
	if options.SkipValidation {
		return verifier, nil
	}

	logger := options.Logger
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	noErrorReturnFirstHTTPReq := false
	jwks, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{options.JWKSURL}, keyfunc.Override{
		HTTPTimeout:               jwksHTTPTimeout,
		RateLimitWaitMax:          jwksRateLimitWaitMax,
		RefreshInterval:           jwksRefreshInterval,
		RefreshUnknownKID:         rate.NewLimiter(rate.Every(jwksRefreshUnknownKIDRate), 1),
		NoErrorReturnFirstHTTPReq: &noErrorReturnFirstHTTPReq,
		RefreshErrorHandlerFunc: func(u string) func(context.Context, error) {
			return func(_ context.Context, err error) {
				logger.Error().Err(err).Str("url", u).Msg("JWKS refresh failed")
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("jwks initialization from %q failed: %w", options.JWKSURL, err)
	}
	verifier.jwks = jwks
	return verifier, nil
}

func (v *Verifier) CacheScope(ctx context.Context, authorization string) (CacheScope, error) {
	tokenString, ok := bearerToken(authorization)
	if !ok {
		return CacheScope{}, ErrMissingAuthorization
	}

	claims, err := v.verify(ctx, tokenString)
	if err != nil {
		return CacheScope{}, err
	}

	if claims.AC == "" {
		return CacheScope{}, ErrMissingCacheScopes
	}

	var scopes []Scope
	if err := json.Unmarshal([]byte(claims.AC), &scopes); err != nil {
		return CacheScope{}, fmt.Errorf("%w: %v", ErrInvalidCacheScopes, err)
	}
	if len(scopes) == 0 {
		return CacheScope{}, ErrNoCacheScopes
	}
	if claims.RepositoryID == "" {
		return CacheScope{}, ErrMissingRepositoryID
	}

	return CacheScope{
		Scopes: scopes,
		RepoID: claims.RepositoryID,
	}, nil
}

func (v *Verifier) verify(ctx context.Context, tokenString string) (*claims, error) {
	claims := new(claims)
	if v.skipValidation {
		_, _, err := jwt.NewParser().ParseUnverified(tokenString, claims)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
		}
		return claims, nil
	}

	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		v.keyfuncFor(ctx),
		jwt.WithIssuer(v.issuer),
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
	)
	if err != nil {
		if errors.Is(err, ErrKeyUnavailable) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

// keyfuncFor wraps key lookup so that any resolution failure (unknown KID,
// degraded JWKS, exhausted refresh rate limit) surfaces as ErrKeyUnavailable.
func (v *Verifier) keyfuncFor(ctx context.Context) jwt.Keyfunc {
	// WithoutCancel: a client disconnect must not abort a shared JWKS refresh
	// whose rate-limit budget is already spent.
	inner := v.jwks.KeyfuncCtx(context.WithoutCancel(ctx))
	return func(token *jwt.Token) (any, error) {
		key, err := inner(token)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrKeyUnavailable, err)
		}
		return key, nil
	}
}

func bearerToken(authorization string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return "", false
	}

	token := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
	return token, token != ""
}
