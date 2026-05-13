package config

import (
	"os"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/tools"
)

const (
	DefaultAddr         = ":3000"
	DefaultTokenIssuer  = "https://token.actions.githubusercontent.com"
	DefaultTokenJWKSURL = "https://token.actions.githubusercontent.com/.well-known/jwks"
)

type Config struct {
	Server  ServerConfig
	Auth    AuthConfig
	DB      DBConfig
	Storage StorageConfig
	Cache   CacheConfig
}

type ServerConfig struct {
	Addr       string
	APIBaseURL string
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
}

type CacheConfig struct {
	EnableDirectDownloads    bool
	DownloadURLSigningSecret string
}

func Load() Config {
	return Config{
		Server: ServerConfig{
			Addr:       envOrDefault("ADDR", DefaultAddr),
			APIBaseURL: envOrDefault("API_BASE_URL", ""),
		},
		Auth: AuthConfig{
			TokenIssuer:         envOrDefault("GITHUB_ACTIONS_TOKEN_ISSUER", DefaultTokenIssuer),
			TokenJWKSURL:        envOrDefault("GITHUB_ACTIONS_TOKEN_JWKS_URL", DefaultTokenJWKSURL),
			SkipTokenValidation: tools.ParseBool(envOrDefault("SKIP_TOKEN_VALIDATION", "false")),
		},
		DB: DBConfig{
			Driver: envOrDefault("DB_DRIVER", "sqlite"),

			SQLitePath: envOrDefault("DB_SQLITE_PATH", ".data/sqlite.db"),

			PostgresURL:      envOrDefault("DB_POSTGRES_URL", ""),
			PostgresDatabase: envOrDefault("DB_POSTGRES_DATABASE", ""),
			PostgresHost:     envOrDefault("DB_POSTGRES_HOST", ""),
			PostgresPort:     envOrDefault("DB_POSTGRES_PORT", "5432"),
			PostgresUser:     envOrDefault("DB_POSTGRES_USER", ""),
			PostgresPassword: envOrDefault("DB_POSTGRES_PASSWORD", ""),

			MySQLDatabase: envOrDefault("DB_MYSQL_DATABASE", ""),
			MySQLHost:     envOrDefault("DB_MYSQL_HOST", ""),
			MySQLPort:     envOrDefault("DB_MYSQL_PORT", "3306"),
			MySQLUser:     envOrDefault("DB_MYSQL_USER", ""),
			MySQLPassword: envOrDefault("DB_MYSQL_PASSWORD", ""),
		},
		Storage: StorageConfig{
			Driver:           envOrDefault("STORAGE_DRIVER", "filesystem"),
			FilesystemPath:   envOrDefault("STORAGE_FILESYSTEM_PATH", ".data/storage/filesystem"),
			S3Bucket:         envOrDefault("STORAGE_S3_BUCKET", ""),
			S3Region:         envOrDefault("AWS_REGION", "us-east-1"),
			S3EndpointURL:    envOrDefault("AWS_ENDPOINT_URL", ""),
			S3ForcePathStyle: tools.ParseBool(envOrDefault("STORAGE_S3_FORCE_PATH_STYLE", "true")),
			S3KeyPrefix:      envOrDefault("STORAGE_S3_KEY_PREFIX", "gh-actions-cache"),
		},
		Cache: CacheConfig{
			EnableDirectDownloads:    tools.ParseBool(envOrDefault("ENABLE_DIRECT_DOWNLOADS", "false")),
			DownloadURLSigningSecret: envOrDefault("DOWNLOAD_URL_SIGNING_SECRET", ""),
		},
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
