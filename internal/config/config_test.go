package config

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("ADDR", "")
	t.Setenv("API_BASE_URL", "")
	t.Setenv("DEFAULT_ACTIONS_RESULTS_URL", "")
	t.Setenv("ACTIONS_TOKEN_ISSUER", "")
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
	t.Setenv("STORAGE_S3_UPLOAD_PART_SIZE_BYTES", "")
	t.Setenv("STORAGE_S3_UPLOAD_CONCURRENCY", "")
	t.Setenv("STORAGE_S3_MULTIPART_ABORT_TIMEOUT", "")
	t.Setenv("ENABLE_DIRECT_DOWNLOADS", "")
	t.Setenv("DOWNLOAD_URL_SIGNING_SECRET", "")
	t.Setenv("CACHE_MERGE_CONCURRENCY", "")
	t.Setenv("CACHE_MAX_SIZE_BYTES", "")
	t.Setenv("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "")
	t.Setenv("MANAGEMENT_API_KEY", "")
	t.Setenv("DISABLE_CLEANUP_JOBS", "")
	t.Setenv("CACHE_CLEANUP_OLDER_THAN_DAYS", "")
	t.Setenv("ORPHANED_STORAGE_GRACE_PERIOD_HOURS", "")
	t.Setenv("DEBUG", "")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, DefaultAddr, cfg.Server.Addr)
	require.Empty(t, cfg.Server.APIBaseURL)
	require.Equal(t, DefaultActionsResultsURL, cfg.Server.DefaultActionsResultsURL)
	require.Equal(t, DefaultTokenIssuer, cfg.Auth.TokenIssuer)
	require.Equal(t, DefaultTokenIssuer+"/.well-known/jwks", cfg.Auth.TokenJWKSURL)
	require.False(t, cfg.Auth.SkipTokenValidation)
	require.Equal(t, "sqlite", cfg.DB.Driver)
	require.Equal(t, ".data/sqlite.db", cfg.DB.SQLitePath)
	require.Equal(t, "filesystem", cfg.Storage.Driver)
	require.Equal(t, ".data/storage/filesystem", cfg.Storage.FilesystemPath)
	require.Equal(t, "us-east-1", cfg.Storage.S3Region)
	require.True(t, cfg.Storage.S3ForcePathStyle)
	require.Equal(t, "gh-actions-cache", cfg.Storage.S3KeyPrefix)
	require.Equal(t, int64(DefaultS3UploadPartSizeBytes), cfg.Storage.S3UploadPartSizeBytes)
	require.False(t, cfg.Storage.S3UploadPartSizeBytesConfigured)
	require.Equal(t, DefaultS3UploadConcurrency, cfg.Storage.S3UploadConcurrency)
	require.Equal(t, DefaultS3MultipartAbortTimeout, cfg.Storage.S3MultipartAbortTimeout)
	require.False(t, cfg.Cache.EnableDirectDownloads)
	require.Empty(t, cfg.Cache.DownloadURLSigningSecret)
	require.Equal(t, runtime.NumCPU(), cfg.Cache.MergeConcurrency)
	require.False(t, cfg.Cache.MaxSizeBytesConfigured)
	require.Zero(t, cfg.Cache.MaxSizeBytes)
	require.Equal(t, float64(DefaultFilesystemMaxUsagePercent), cfg.Cache.FilesystemMaxUsagePercent)
	require.Empty(t, cfg.Management.APIKey)
	require.False(t, cfg.Cleanup.Disabled)
	require.Equal(t, DefaultCacheOlderThanDays, cfg.Cleanup.CacheOlderThanDays)
	require.Equal(t, 24*time.Hour, cfg.Cleanup.OrphanedStorageGracePeriod)
	require.False(t, cfg.Debug)
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("ADDR", ":8080")
	t.Setenv("API_BASE_URL", "https://cache.example")
	t.Setenv("DEFAULT_ACTIONS_RESULTS_URL", "https://results.example")
	t.Setenv("ACTIONS_TOKEN_ISSUER", "https://issuer.example")
	t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", "https://legacy-issuer.example")
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
	t.Setenv("STORAGE_S3_UPLOAD_PART_SIZE_BYTES", "10485760")
	t.Setenv("STORAGE_S3_UPLOAD_CONCURRENCY", "3")
	t.Setenv("STORAGE_S3_MULTIPART_ABORT_TIMEOUT", "45s")
	t.Setenv("ENABLE_DIRECT_DOWNLOADS", "true")
	t.Setenv("DOWNLOAD_URL_SIGNING_SECRET", "secret")
	t.Setenv("CACHE_MERGE_CONCURRENCY", "2")
	t.Setenv("CACHE_MAX_SIZE_BYTES", "1073741824")
	t.Setenv("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "82.5")
	t.Setenv("MANAGEMENT_API_KEY", "management-secret")
	t.Setenv("DISABLE_CLEANUP_JOBS", "true")
	t.Setenv("CACHE_CLEANUP_OLDER_THAN_DAYS", "30")
	t.Setenv("ORPHANED_STORAGE_GRACE_PERIOD_HOURS", "48")
	t.Setenv("DEBUG", "true")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, ":8080", cfg.Server.Addr)
	require.Equal(t, "https://cache.example", cfg.Server.APIBaseURL)
	require.Equal(t, "https://results.example", cfg.Server.DefaultActionsResultsURL)
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
	require.Equal(t, int64(10*1024*1024), cfg.Storage.S3UploadPartSizeBytes)
	require.True(t, cfg.Storage.S3UploadPartSizeBytesConfigured)
	require.Equal(t, 3, cfg.Storage.S3UploadConcurrency)
	require.Equal(t, 45*time.Second, cfg.Storage.S3MultipartAbortTimeout)
	require.True(t, cfg.Cache.EnableDirectDownloads)
	require.Equal(t, "secret", cfg.Cache.DownloadURLSigningSecret)
	require.Equal(t, 2, cfg.Cache.MergeConcurrency)
	require.True(t, cfg.Cache.MaxSizeBytesConfigured)
	require.Equal(t, int64(1073741824), cfg.Cache.MaxSizeBytes)
	require.Equal(t, 82.5, cfg.Cache.FilesystemMaxUsagePercent)
	require.Equal(t, "management-secret", cfg.Management.APIKey)
	require.True(t, cfg.Cleanup.Disabled)
	require.Equal(t, 30, cfg.Cleanup.CacheOlderThanDays)
	require.Equal(t, 48*time.Hour, cfg.Cleanup.OrphanedStorageGracePeriod)
	require.True(t, cfg.Debug)
}

func TestLoadTokenIssuerConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		issuer       string
		legacyIssuer string
		jwksURL      string
		wantIssuer   string
		wantJWKSURL  string
	}{
		{
			name:        "defaults",
			wantIssuer:  DefaultTokenIssuer,
			wantJWKSURL: DefaultTokenIssuer + "/.well-known/jwks",
		},
		{
			name:        "canonical issuer derives JWKS and trims trailing slashes",
			issuer:      "https://ghes.example///",
			wantIssuer:  "https://ghes.example",
			wantJWKSURL: "https://ghes.example/.well-known/jwks",
		},
		{
			name:         "legacy issuer derives JWKS",
			legacyIssuer: "https://legacy-ghes.example/",
			wantIssuer:   "https://legacy-ghes.example",
			wantJWKSURL:  "https://legacy-ghes.example/.well-known/jwks",
		},
		{
			name:         "canonical issuer takes precedence over legacy issuer",
			issuer:       "https://canonical.example",
			legacyIssuer: "https://legacy.example",
			wantIssuer:   "https://canonical.example",
			wantJWKSURL:  "https://canonical.example/.well-known/jwks",
		},
		{
			name:         "explicit JWKS URL overrides derived URL",
			issuer:       "https://ghes.example/",
			legacyIssuer: "https://legacy.example",
			jwksURL:      "https://keys.example/custom-jwks",
			wantIssuer:   "https://ghes.example",
			wantJWKSURL:  "https://keys.example/custom-jwks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ACTIONS_TOKEN_ISSUER", tt.issuer)
			t.Setenv("GITHUB_ACTIONS_TOKEN_ISSUER", tt.legacyIssuer)
			t.Setenv("GITHUB_ACTIONS_TOKEN_JWKS_URL", tt.jwksURL)

			cfg, err := Load()
			require.NoError(t, err)
			require.Equal(t, tt.wantIssuer, cfg.Auth.TokenIssuer)
			require.Equal(t, tt.wantJWKSURL, cfg.Auth.TokenJWKSURL)
		})
	}
}

func TestLoadPreservesS3UploadPartSizeBelowMinimumForValidation(t *testing.T) {
	t.Setenv("STORAGE_S3_UPLOAD_PART_SIZE_BYTES", "1048576")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, int64(1048576), cfg.Storage.S3UploadPartSizeBytes)
	require.True(t, cfg.Storage.S3UploadPartSizeBytesConfigured)
}

func TestLoadRejectsMalformedS3UploadPartSize(t *testing.T) {
	tests := []string{"10MiB", "abc", "9223372036854775808"}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			t.Setenv("STORAGE_S3_UPLOAD_PART_SIZE_BYTES", value)

			cfg, err := Load()

			require.Zero(t, cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "STORAGE_S3_UPLOAD_PART_SIZE_BYTES")
			require.Contains(t, err.Error(), value)
		})
	}
}

func TestLoadRejectsInvalidCapacityConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "zero byte budget", key: "CACHE_MAX_SIZE_BYTES", value: "0"},
		{name: "negative byte budget", key: "CACHE_MAX_SIZE_BYTES", value: "-1"},
		{name: "malformed byte budget", key: "CACHE_MAX_SIZE_BYTES", value: "1GiB"},
		{name: "zero filesystem percent", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "0"},
		{name: "negative filesystem percent", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "-1"},
		{name: "filesystem percent above one hundred", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "100.1"},
		{name: "filesystem percent NaN", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "NaN"},
		{name: "filesystem percent infinity", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "+Inf"},
		{name: "malformed filesystem percent", key: "CACHE_FILESYSTEM_MAX_USAGE_PERCENT", value: "high"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CACHE_MAX_SIZE_BYTES", "")
			t.Setenv("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "")
			t.Setenv(tt.key, tt.value)

			cfg, err := Load()

			require.Zero(t, cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.key)
			require.Contains(t, err.Error(), tt.value)
		})
	}
}

func TestLoadRejectsInvalidOrphanedStorageGracePeriod(t *testing.T) {
	for _, value := range []string{"0", "-1", "1.5", "forever", "2562048"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("ORPHANED_STORAGE_GRACE_PERIOD_HOURS", value)

			cfg, err := Load()

			require.Zero(t, cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "ORPHANED_STORAGE_GRACE_PERIOD_HOURS")
			require.Contains(t, err.Error(), value)
		})
	}
}

func TestLoadAcceptsCleanupRetentionBounds(t *testing.T) {
	tests := []struct {
		value string
		want  int
	}{
		{value: "0", want: 0},
		{value: strconv.Itoa(MaximumCacheOlderThanDays), want: MaximumCacheOlderThanDays},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Setenv("CACHE_CLEANUP_OLDER_THAN_DAYS", tt.value)

			cfg, err := Load()

			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Cleanup.CacheOlderThanDays)
		})
	}
}

func TestLoadRejectsInvalidCleanupRetention(t *testing.T) {
	for _, value := range []string{"-1", strconv.Itoa(MaximumCacheOlderThanDays + 1), "1.5", "forever", "9223372036854775808"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("CACHE_CLEANUP_OLDER_THAN_DAYS", value)

			cfg, err := Load()

			require.Zero(t, cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "CACHE_CLEANUP_OLDER_THAN_DAYS")
			require.Contains(t, err.Error(), value)
		})
	}
}
