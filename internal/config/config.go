package config

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/tools"
)

const (
	DefaultAddr              = ":3000"
	DefaultActionsResultsURL = "https://results-receiver.actions.githubusercontent.com"
	DefaultTokenIssuer       = "https://token.actions.githubusercontent.com"

	MinS3UploadPartSizeBytes         = 5 * 1024 * 1024
	DefaultS3KeyPrefix               = "gh-actions-cache"
	DefaultS3UploadPartSizeBytes     = MinS3UploadPartSizeBytes
	DefaultS3UploadConcurrency       = 1
	DefaultS3MultipartAbortTimeout   = 30 * time.Second
	DefaultFilesystemMaxUsagePercent = 90
	DefaultOrphanedStorageGraceHours = 24
)

type Config struct {
	Server     ServerConfig
	Auth       AuthConfig
	DB         DBConfig
	Storage    StorageConfig
	Cache      CacheConfig
	Management ManagementConfig
	Cleanup    CleanupConfig
	Debug      bool
}

type ServerConfig struct {
	Addr                     string
	APIBaseURL               string
	DefaultActionsResultsURL string
}

type AuthConfig struct {
	TokenIssuer         string
	TokenJWKSURL        string
	SkipTokenValidation bool
}

type DBConfig struct {
	Driver string

	SQLitePath string

	PostgresURL      string
	PostgresDatabase string
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string

	MySQLDatabase string
	MySQLHost     string
	MySQLPort     string
	MySQLUser     string
	MySQLPassword string
}

type StorageConfig struct {
	Driver string

	FilesystemPath string

	S3Bucket         string
	S3Region         string
	S3EndpointURL    string
	S3ForcePathStyle bool
	S3KeyPrefix      string

	S3UploadPartSizeBytes           int64
	S3UploadPartSizeBytesConfigured bool
	S3UploadConcurrency             int
	S3MultipartAbortTimeout         time.Duration
}

type CacheConfig struct {
	EnableDirectDownloads     bool
	DownloadURLSigningSecret  string
	MergeConcurrency          int
	MaxSizeBytes              int64
	MaxSizeBytesConfigured    bool
	FilesystemMaxUsagePercent float64
}

type ManagementConfig struct {
	APIKey string
}

type CleanupConfig struct {
	Disabled                   bool
	CacheOlderThanDays         int
	OrphanedStorageGracePeriod time.Duration
}

func Load() (Config, error) {
	s3UploadPartSizeBytes, s3UploadPartSizeBytesConfigured, err := int64Env("STORAGE_S3_UPLOAD_PART_SIZE_BYTES", DefaultS3UploadPartSizeBytes)
	if err != nil {
		return Config{}, err
	}
	cacheMaxSizeBytes, cacheMaxSizeBytesConfigured, err := optionalPositiveInt64Env("CACHE_MAX_SIZE_BYTES")
	if err != nil {
		return Config{}, err
	}
	filesystemMaxUsagePercent, err := percentageEnv("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", DefaultFilesystemMaxUsagePercent)
	if err != nil {
		return Config{}, err
	}
	orphanedStorageGracePeriod, err := positiveHoursEnv("ORPHANED_STORAGE_GRACE_PERIOD_HOURS", DefaultOrphanedStorageGraceHours)
	if err != nil {
		return Config{}, err
	}
	tokenIssuer := strings.TrimRight(
		tools.EnvOrDefault(
			"ACTIONS_TOKEN_ISSUER",
			tools.EnvOrDefault("GITHUB_ACTIONS_TOKEN_ISSUER", DefaultTokenIssuer),
		),
		"/",
	)
	tokenJWKSURL := tools.EnvOrDefault("GITHUB_ACTIONS_TOKEN_JWKS_URL", tokenIssuer+"/.well-known/jwks")

	return Config{
		Server: ServerConfig{
			Addr:                     tools.EnvOrDefault("ADDR", DefaultAddr),
			APIBaseURL:               tools.EnvOrDefault("API_BASE_URL", ""),
			DefaultActionsResultsURL: tools.EnvOrDefault("DEFAULT_ACTIONS_RESULTS_URL", DefaultActionsResultsURL),
		},
		Auth: AuthConfig{
			TokenIssuer:         tokenIssuer,
			TokenJWKSURL:        tokenJWKSURL,
			SkipTokenValidation: tools.ParseBool(tools.EnvOrDefault("SKIP_TOKEN_VALIDATION", "false")),
		},
		DB: DBConfig{
			Driver: tools.EnvOrDefault("DB_DRIVER", "sqlite"),

			SQLitePath: tools.EnvOrDefault("DB_SQLITE_PATH", ".data/sqlite.db"),

			PostgresURL:      tools.EnvOrDefault("DB_POSTGRES_URL", ""),
			PostgresDatabase: tools.EnvOrDefault("DB_POSTGRES_DATABASE", ""),
			PostgresHost:     tools.EnvOrDefault("DB_POSTGRES_HOST", ""),
			PostgresPort:     tools.EnvOrDefault("DB_POSTGRES_PORT", "5432"),
			PostgresUser:     tools.EnvOrDefault("DB_POSTGRES_USER", ""),
			PostgresPassword: tools.EnvOrDefault("DB_POSTGRES_PASSWORD", ""),

			MySQLDatabase: tools.EnvOrDefault("DB_MYSQL_DATABASE", ""),
			MySQLHost:     tools.EnvOrDefault("DB_MYSQL_HOST", ""),
			MySQLPort:     tools.EnvOrDefault("DB_MYSQL_PORT", "3306"),
			MySQLUser:     tools.EnvOrDefault("DB_MYSQL_USER", ""),
			MySQLPassword: tools.EnvOrDefault("DB_MYSQL_PASSWORD", ""),
		},
		Storage: StorageConfig{
			Driver:                          tools.EnvOrDefault("STORAGE_DRIVER", "filesystem"),
			FilesystemPath:                  tools.EnvOrDefault("STORAGE_FILESYSTEM_PATH", ".data/storage/filesystem"),
			S3Bucket:                        tools.EnvOrDefault("STORAGE_S3_BUCKET", ""),
			S3Region:                        tools.EnvOrDefault("AWS_REGION", "us-east-1"),
			S3EndpointURL:                   tools.EnvOrDefault("AWS_ENDPOINT_URL", ""),
			S3ForcePathStyle:                tools.ParseBool(tools.EnvOrDefault("STORAGE_S3_FORCE_PATH_STYLE", "true")),
			S3KeyPrefix:                     tools.EnvOrDefault("STORAGE_S3_KEY_PREFIX", DefaultS3KeyPrefix),
			S3UploadPartSizeBytes:           s3UploadPartSizeBytes,
			S3UploadPartSizeBytesConfigured: s3UploadPartSizeBytesConfigured,
			S3UploadConcurrency:             positiveIntEnv("STORAGE_S3_UPLOAD_CONCURRENCY", DefaultS3UploadConcurrency),
			S3MultipartAbortTimeout:         positiveDurationEnv("STORAGE_S3_MULTIPART_ABORT_TIMEOUT", DefaultS3MultipartAbortTimeout),
		},
		Cache: CacheConfig{
			EnableDirectDownloads:     tools.ParseBool(tools.EnvOrDefault("ENABLE_DIRECT_DOWNLOADS", "false")),
			DownloadURLSigningSecret:  tools.EnvOrDefault("DOWNLOAD_URL_SIGNING_SECRET", ""),
			MergeConcurrency:          positiveIntEnv("CACHE_MERGE_CONCURRENCY", defaultMergeConcurrency()),
			MaxSizeBytes:              cacheMaxSizeBytes,
			MaxSizeBytesConfigured:    cacheMaxSizeBytesConfigured,
			FilesystemMaxUsagePercent: filesystemMaxUsagePercent,
		},
		Management: ManagementConfig{
			APIKey: tools.EnvOrDefault("MANAGEMENT_API_KEY", ""),
		},
		Cleanup: CleanupConfig{
			Disabled:                   tools.ParseBool(tools.EnvOrDefault("DISABLE_CLEANUP_JOBS", "false")),
			CacheOlderThanDays:         tools.ParseInt(tools.EnvOrDefault("CACHE_CLEANUP_OLDER_THAN_DAYS", "90"), 90),
			OrphanedStorageGracePeriod: orphanedStorageGracePeriod,
		},
		Debug: tools.ParseBool(tools.EnvOrDefault("DEBUG", "false")),
	}, nil
}

func defaultMergeConcurrency() int {
	if cpus := runtime.NumCPU(); cpus > 0 {
		return cpus
	}
	return 1
}

func positiveIntEnv(key string, fallback int) int {
	value := tools.ParseInt(tools.EnvOrDefault(key, strconv.Itoa(fallback)), fallback)
	if value < 1 {
		return fallback
	}
	return value
}

func int64Env(key string, fallback int64) (int64, bool, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("%s must be an integer number of bytes: %q", key, raw)
	}
	return value, true, nil
}

func optionalPositiveInt64Env(key string) (int64, bool, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, true, fmt.Errorf("%s must be a positive integer number of bytes: %q", key, raw)
	}
	return value, true, nil
}

func percentageEnv(key string, fallback float64) (float64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 100 {
		return 0, fmt.Errorf("%s must be a number greater than 0 and at most 100: %q", key, raw)
	}
	return value, nil
}

func positiveHoursEnv(key string, fallback int64) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		raw = strconv.FormatInt(fallback, 10)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	maximumHours := int64(math.MaxInt64 / int64(time.Hour))
	if err != nil || value < 1 || value > maximumHours {
		return 0, fmt.Errorf("%s must be a positive integer number of hours: %q", key, raw)
	}
	return time.Duration(value) * time.Hour, nil
}

func positiveDurationEnv(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(tools.EnvOrDefault(key, fallback.String()))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
