package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ADDR", "")
	t.Setenv("API_BASE_URL", "")
	t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", "")
	t.Setenv("GITHUB_ACTIONS_TOKEN_JWKS_URL", "")
	t.Setenv("SKIP_TOKEN_VALIDATION", "")
	t.Setenv("DB_DRIVER", "")
	t.Setenv("DB_SQLITE_PATH", "")
	t.Setenv("DB_POSTGRES_URL", "")
	t.Setenv("STORAGE_DRIVER", "")
	t.Setenv("STORAGE_FILESYSTEM_PATH", "")
	t.Setenv("STORAGE_S3_BUCKET", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("STORAGE_S3_FORCE_PATH_STYLE", "")
	t.Setenv("STORAGE_S3_KEY_PREFIX", "")
	t.Setenv("ENABLE_DIRECT_DOWNLOADS", "")
	t.Setenv("DOWNLOAD_URL_SIGNING_SECRET", "")

	cfg := Load()

	require.Equal(t, DefaultAddr, cfg.Server.Addr)
	require.Empty(t, cfg.Server.APIBaseURL)
	require.Equal(t, DefaultTokenIssuer, cfg.Auth.TokenIssuer)
	require.Equal(t, DefaultTokenJWKSURL, cfg.Auth.TokenJWKSURL)
	require.False(t, cfg.Auth.SkipTokenValidation)
	require.Equal(t, "sqlite", cfg.DB.Driver)
	require.Equal(t, ".data/sqlite.db", cfg.DB.SQLitePath)
	require.Equal(t, "filesystem", cfg.Storage.Driver)
	require.Equal(t, ".data/storage/filesystem", cfg.Storage.FilesystemPath)
	require.Equal(t, "us-east-1", cfg.Storage.S3Region)
	require.True(t, cfg.Storage.S3ForcePathStyle)
	require.Equal(t, "gh-actions-cache", cfg.Storage.S3KeyPrefix)
	require.False(t, cfg.Cache.EnableDirectDownloads)
	require.Empty(t, cfg.Cache.DownloadURLSigningSecret)
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("ADDR", ":8080")
	t.Setenv("API_BASE_URL", "https://cache.example")
	t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", "https://issuer.example")
	t.Setenv("GITHUB_ACTIONS_TOKEN_JWKS_URL", "https://issuer.example/.well-known/jwks")
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	t.Setenv("DB_DRIVER", "postgres")
	t.Setenv("DB_POSTGRES_URL", "postgres://example")
	t.Setenv("STORAGE_DRIVER", "s3")
	t.Setenv("STORAGE_S3_BUCKET", "cache-bucket")
	t.Setenv("AWS_REGION", "ap-east-1")
	t.Setenv("AWS_ENDPOINT_URL", "https://s3.example")
	t.Setenv("STORAGE_S3_FORCE_PATH_STYLE", "false")
	t.Setenv("STORAGE_S3_KEY_PREFIX", "custom-prefix")
	t.Setenv("ENABLE_DIRECT_DOWNLOADS", "true")
	t.Setenv("DOWNLOAD_URL_SIGNING_SECRET", "secret")

	cfg := Load()

	require.Equal(t, ":8080", cfg.Server.Addr)
	require.Equal(t, "https://cache.example", cfg.Server.APIBaseURL)
	require.Equal(t, "https://issuer.example", cfg.Auth.TokenIssuer)
	require.Equal(t, "https://issuer.example/.well-known/jwks", cfg.Auth.TokenJWKSURL)
	require.True(t, cfg.Auth.SkipTokenValidation)
	require.Equal(t, "postgres", cfg.DB.Driver)
	require.Equal(t, "postgres://example", cfg.DB.PostgresURL)
	require.Equal(t, "s3", cfg.Storage.Driver)
	require.Equal(t, "cache-bucket", cfg.Storage.S3Bucket)
	require.Equal(t, "ap-east-1", cfg.Storage.S3Region)
	require.Equal(t, "https://s3.example", cfg.Storage.S3EndpointURL)
	require.False(t, cfg.Storage.S3ForcePathStyle)
	require.Equal(t, "custom-prefix", cfg.Storage.S3KeyPrefix)
	require.True(t, cfg.Cache.EnableDirectDownloads)
	require.Equal(t, "secret", cfg.Cache.DownloadURLSigningSecret)
}
