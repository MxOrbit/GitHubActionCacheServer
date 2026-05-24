package config

import "github.com/MxOrbit/GitHubActionCacheServer/internal/tools"

const (
	DefaultAddr         = ":3000"
	DefaultTokenIssuer  = "https://token.actions.githubusercontent.com"
	DefaultTokenJWKSURL = "https://token.actions.githubusercontent.com/.well-known/jwks"
)

type Config struct {
	Server     ServerConfig
	Auth       AuthConfig
	DB         DBConfig
	Storage    StorageConfig
	Cache      CacheConfig
	Management ManagementConfig
	Cleanup    CleanupConfig
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

type ManagementConfig struct {
	APIKey string
}

type CleanupConfig struct {
	Disabled           bool
	CacheOlderThanDays int
}

func Load() Config {
	return Config{
		Server: ServerConfig{
			Addr:       tools.EnvOrDefault("ADDR", DefaultAddr),
			APIBaseURL: tools.EnvOrDefault("API_BASE_URL", ""),
		},
		Auth: AuthConfig{
			TokenIssuer:         tools.EnvOrDefault("GITHUB_ACTIONS_TOKEN_ISSUER", DefaultTokenIssuer),
			TokenJWKSURL:        tools.EnvOrDefault("GITHUB_ACTIONS_TOKEN_JWKS_URL", DefaultTokenJWKSURL),
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
			Driver:           tools.EnvOrDefault("STORAGE_DRIVER", "filesystem"),
			FilesystemPath:   tools.EnvOrDefault("STORAGE_FILESYSTEM_PATH", ".data/storage/filesystem"),
			S3Bucket:         tools.EnvOrDefault("STORAGE_S3_BUCKET", ""),
			S3Region:         tools.EnvOrDefault("AWS_REGION", "us-east-1"),
			S3EndpointURL:    tools.EnvOrDefault("AWS_ENDPOINT_URL", ""),
			S3ForcePathStyle: tools.ParseBool(tools.EnvOrDefault("STORAGE_S3_FORCE_PATH_STYLE", "true")),
			S3KeyPrefix:      tools.EnvOrDefault("STORAGE_S3_KEY_PREFIX", "gh-actions-cache"),
		},
		Cache: CacheConfig{
			EnableDirectDownloads:    tools.ParseBool(tools.EnvOrDefault("ENABLE_DIRECT_DOWNLOADS", "false")),
			DownloadURLSigningSecret: tools.EnvOrDefault("DOWNLOAD_URL_SIGNING_SECRET", ""),
		},
		Management: ManagementConfig{
			APIKey: tools.EnvOrDefault("MANAGEMENT_API_KEY", ""),
		},
		Cleanup: CleanupConfig{
			Disabled:           tools.ParseBool(tools.EnvOrDefault("DISABLE_CLEANUP_JOBS", "false")),
			CacheOlderThanDays: tools.ParseInt(tools.EnvOrDefault("CACHE_CLEANUP_OLDER_THAN_DAYS", "90"), 90),
		},
	}
}
