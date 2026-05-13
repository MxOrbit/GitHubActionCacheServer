package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func TestOpenAndMigrateSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	for _, table := range []string{"cache_entries", "storage_locations", "uploads"} {
		t.Run(table, func(t *testing.T) {
			var name string
			err := sqlDB.QueryRowContext(
				ctx,
				`select name from sqlite_master where type = 'table' and name = ?`,
				table,
			).Scan(&name)

			require.NoError(t, err)
			require.Equal(t, table, name)
		})
	}
}

func TestStorageLocationDeleteCascadesCacheEntries(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	location := client.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(ctx)

	client.CacheEntry.Create().
		SetID("cache-entry-id").
		SetKey("key").
		SetVersion("version").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(ctx)

	require.NoError(t, client.StorageLocation.DeleteOneID(location.ID).Exec(ctx))

	exists, err := client.CacheEntry.Query().
		Where(cacheentry.ID("cache-entry-id")).
		Exist(ctx)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestMySQLDSNEscapesDatabaseName(t *testing.T) {
	dsn := mysqlDSN(config.DBConfig{
		MySQLUser:     "user",
		MySQLPassword: "pa:ss@word?",
		MySQLHost:     "127.0.0.1",
		MySQLPort:     "3306",
		MySQLDatabase: "cache/db?name",
	})

	parsed, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, "user", parsed.User)
	require.Equal(t, "pa:ss@word?", parsed.Passwd)
	require.Equal(t, "cache/db?name", parsed.DBName)
	require.True(t, parsed.ParseTime)
	require.Contains(t, dsn, "/cache%2Fdb%3Fname?")
	require.False(t, strings.Contains(dsn, "/cache/db?name?"))
}
