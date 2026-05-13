package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ADDR", "")
	t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", "")
	t.Setenv("GITHUB_ACTIONS_TOKEN_JWKS_URL", "")
	t.Setenv("SKIP_TOKEN_VALIDATION", "")
	t.Setenv("DB_DRIVER", "")
	t.Setenv("DB_SQLITE_PATH", "")
	t.Setenv("DB_POSTGRES_URL", "")

	cfg := Load()

	require.Equal(t, DefaultAddr, cfg.Server.Addr)
	require.Equal(t, DefaultTokenIssuer, cfg.Auth.TokenIssuer)
	require.Equal(t, DefaultTokenJWKSURL, cfg.Auth.TokenJWKSURL)
	require.False(t, cfg.Auth.SkipTokenValidation)
	require.Equal(t, "sqlite", cfg.DB.Driver)
	require.Equal(t, ".data/sqlite.db", cfg.DB.SQLitePath)
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("ADDR", ":8080")
	t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", "https://issuer.example")
	t.Setenv("GITHUB_ACTIONS_TOKEN_JWKS_URL", "https://issuer.example/.well-known/jwks")
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	t.Setenv("DB_DRIVER", "postgres")
	t.Setenv("DB_POSTGRES_URL", "postgres://example")

	cfg := Load()

	require.Equal(t, ":8080", cfg.Server.Addr)
	require.Equal(t, "https://issuer.example", cfg.Auth.TokenIssuer)
	require.Equal(t, "https://issuer.example/.well-known/jwks", cfg.Auth.TokenJWKSURL)
	require.True(t, cfg.Auth.SkipTokenValidation)
	require.Equal(t, "postgres", cfg.DB.Driver)
	require.Equal(t, "postgres://example", cfg.DB.PostgresURL)
}
