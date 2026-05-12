package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken           = errors.New("invalid token")
	ErrMissingAuthorization   = errors.New("authorization header missing or malformed")
	ErrMissingCacheScopes     = errors.New("token does not contain cache scopes")
	ErrInvalidCacheScopes     = errors.New("invalid JSON in cache scopes")
	ErrNoCacheScopes          = errors.New("token does not contain any cache scopes")
	ErrMissingRepositoryID    = errors.New("token does not contain repository id")
	ErrVerifierInitialization = errors.New("token verifier initialization failed")
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
}

type Verifier struct {
	issuer         string
	jwksURL        string
	skipValidation bool

	jwksOnce sync.Once
	jwks     keyfunc.Keyfunc
	jwksErr  error
}

type claims struct {
	AC           string `json:"ac"`
	RepositoryID string `json:"repository_id"`
	jwt.RegisteredClaims
}

func NewVerifier(options Options) *Verifier {
	return &Verifier{
		issuer:         options.Issuer,
		jwksURL:        options.JWKSURL,
		skipValidation: options.SkipValidation,
	}
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

	jwks, err := v.keyfunc()
	if err != nil {
		return nil, err
	}

	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		jwks.KeyfuncCtx(ctx),
		jwt.WithIssuer(v.issuer),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

func (v *Verifier) keyfunc() (keyfunc.Keyfunc, error) {
	v.jwksOnce.Do(func() {
		v.jwks, v.jwksErr = keyfunc.NewDefaultCtx(context.Background(), []string{v.jwksURL})
	})
	if v.jwksErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrVerifierInitialization, v.jwksErr)
	}
	return v.jwks, nil
}

func bearerToken(authorization string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return "", false
	}

	token := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
	return token, token != ""
}
