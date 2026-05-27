package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const externalS3CacheSize = 5*1024*1024 + 1024

func TestExternalPostgresFilesystemSaveAndRestore(t *testing.T) {
	dbCfg, ok := externalPostgresConfig()
	if !ok {
		t.Skip("set E2E_POSTGRES_URL to run PostgreSQL integration coverage")
	}

	ctx := context.Background()
	client := openExternalDB(t, ctx, dbCfg)
	filesystem := testutil.NewFilesystemAdapter(t)
	router := newExternalRouter(t, client, filesystem)

	runSaveRestoreFlow(t, router, uniqueIntegrationCacheKey("postgres"), "postgres-cache-content")
}

func TestExternalMySQLFilesystemSaveAndRestore(t *testing.T) {
	dbCfg, ok := externalMySQLConfig()
	if !ok {
		t.Skip("set E2E_MYSQL_HOST, E2E_MYSQL_DATABASE and E2E_MYSQL_USER to run MySQL integration coverage")
	}

	ctx := context.Background()
	client := openExternalDB(t, ctx, dbCfg)
	filesystem := testutil.NewFilesystemAdapter(t)
	router := newExternalRouter(t, client, filesystem)

	runSaveRestoreFlow(t, router, uniqueIntegrationCacheKey("mysql"), "mysql-cache-content")
}

func TestExternalS3MinIOSaveAndRestore(t *testing.T) {
	storageCfg, ok := externalS3Config()
	if !ok {
		t.Skip("set E2E_S3_ENDPOINT_URL and E2E_S3_BUCKET to run S3/MinIO integration coverage")
	}
	if os.Getenv("AWS_EC2_METADATA_DISABLED") == "" {
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	}

	ctx, client := testutil.NewSQLiteClient(t)
	ensureExternalS3Bucket(t, ctx, storageCfg)
	s3Storage, err := storage.NewS3Adapter(ctx, storageCfg)
	require.NoError(t, err)
	require.NoError(t, s3Storage.Clear(ctx))
	t.Cleanup(func() {
		require.NoError(t, s3Storage.Clear(context.Background()))
	})

	router := newExternalRouter(t, client, s3Storage)
	runSaveRestoreFlow(t, router, uniqueIntegrationCacheKey("s3-minio"), strings.Repeat("s", externalS3CacheSize))
}

func runSaveRestoreFlow(t *testing.T, router http.Handler, key string, content string) {
	t.Helper()

	token := actionsToken(t)
	createBody := cacheBody(key)
	uploadURL := createCacheEntry(t, router, token, createBody)
	uploadWholeCache(t, router, uploadURL, content)
	finalizeCacheEntry(t, router, token, createBody)

	matchResponse := matchCacheEntry(t, router, token, map[string]any{
		"key":          key,
		"restore_keys": []string{strings.TrimSuffix(key, "-missing")},
		"version":      defaultCacheEntryVersion,
	})
	require.Equal(t, key, matchResponse.MatchedKey)
	require.Equal(t, content, downloadCache(t, router, parseSignedURL(t, matchResponse.SignedDownloadURL)))
}

func openExternalDB(t *testing.T, ctx context.Context, cfg config.DBConfig) *ent.Client {
	t.Helper()

	client, err := db.OpenAndMigrate(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		clearExternalDB(t, context.Background(), client)
		require.NoError(t, client.Close())
	})

	clearExternalDB(t, ctx, client)
	return client
}

func clearExternalDB(t *testing.T, ctx context.Context, client *ent.Client) {
	t.Helper()

	_, err := client.Upload.Delete().Exec(ctx)
	require.NoError(t, err)
	_, err = client.CacheEntry.Delete().Exec(ctx)
	require.NoError(t, err)
	_, err = client.StorageLocation.Delete().Exec(ctx)
	require.NoError(t, err)
}

func newExternalRouter(t *testing.T, client *ent.Client, storageAdapter storage.Adapter) http.Handler {
	t.Helper()

	cfg, err := config.Load()
	require.NoError(t, err)
	cfg.Auth.SkipTokenValidation = true
	cfg.Cache.DownloadURLSigningSecret = "integration-test-secret"
	cacheService := cache.NewService(cache.Options{
		DB:               client,
		Storage:          storageAdapter,
		MergeConcurrency: cfg.Cache.MergeConcurrency,
	})
	t.Cleanup(func() {
		cacheService.StopAcceptingMerges()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, cacheService.WaitForMerges(ctx))
	})

	return httpapi.NewRouter(zerolog.Nop(), cfg, httpapi.Dependencies{
		DB:      client,
		Storage: storageAdapter,
		Cache:   cacheService,
	})
}

func externalPostgresConfig() (config.DBConfig, bool) {
	dsn := strings.TrimSpace(os.Getenv("E2E_POSTGRES_URL"))
	if dsn == "" {
		return config.DBConfig{}, false
	}
	return config.DBConfig{
		Driver:      db.DriverPostgres,
		PostgresURL: dsn,
	}, true
}

func externalMySQLConfig() (config.DBConfig, bool) {
	host := strings.TrimSpace(os.Getenv("E2E_MYSQL_HOST"))
	database := strings.TrimSpace(os.Getenv("E2E_MYSQL_DATABASE"))
	user := strings.TrimSpace(os.Getenv("E2E_MYSQL_USER"))
	if host == "" || database == "" || user == "" {
		return config.DBConfig{}, false
	}
	return config.DBConfig{
		Driver:        db.DriverMySQL,
		MySQLHost:     host,
		MySQLPort:     envOrDefault("E2E_MYSQL_PORT", "3306"),
		MySQLDatabase: database,
		MySQLUser:     user,
		MySQLPassword: os.Getenv("E2E_MYSQL_PASSWORD"),
	}, true
}

func externalS3Config() (config.StorageConfig, bool) {
	endpoint := strings.TrimSpace(os.Getenv("E2E_S3_ENDPOINT_URL"))
	bucket := strings.TrimSpace(os.Getenv("E2E_S3_BUCKET"))
	if endpoint == "" || bucket == "" {
		return config.StorageConfig{}, false
	}
	return config.StorageConfig{
		Driver:           storage.DriverS3,
		S3Bucket:         bucket,
		S3Region:         envOrDefault("E2E_S3_REGION", "us-east-1"),
		S3EndpointURL:    endpoint,
		S3ForcePathStyle: true,
		S3KeyPrefix:      "gh-actions-cache-e2e",
	}, true
}

func ensureExternalS3Bucket(t *testing.T, ctx context.Context, cfg config.StorageConfig) {
	t.Helper()

	if os.Getenv("E2E_S3_CREATE_BUCKET") != "true" {
		return
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.S3Region))
	require.NoError(t, err)
	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.S3ForcePathStyle
		if cfg.S3EndpointURL != "" {
			options.BaseEndpoint = aws.String(cfg.S3EndpointURL)
		}
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(cfg.S3Bucket),
	})
	if err != nil && !isBucketAlreadyExists(err) {
		require.NoError(t, err)
	}
}

func isBucketAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
		return true
	default:
		return false
	}
}

func uniqueIntegrationCacheKey(prefix string) string {
	return fmt.Sprintf("%s-cache-%d", prefix, time.Now().UnixNano())
}

func envOrDefault(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
