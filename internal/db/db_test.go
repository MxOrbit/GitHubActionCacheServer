package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entschema "entgo.io/ent/dialect/sql/schema"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/migrate"
	"github.com/go-sql-driver/mysql"
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

func TestOpenAndMigrateSQLiteFromOriginalSchema(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	for _, stmt := range []string{
		`create table storage_locations (
			id text primary key,
			folderName text not null,
			partCount integer not null,
			mergeStartedAt bigint,
			mergedAt bigint,
			partsDeletedAt bigint,
			lastDownloadedAt bigint
		)`,
		`create table cache_entries (
			id text primary key,
			key text not null,
			version text not null,
			updatedAt bigint not null,
			locationId text not null references storage_locations(id) on delete cascade,
			scope text not null,
			repoId text not null
		)`,
		`create table uploads (
			id bigint primary key,
			key text not null,
			version text not null,
			createdAt bigint not null,
			lastPartUploadedAt bigint,
			folderName text not null,
			finishedPartUploadCount integer not null default 0,
			startedPartUploadCount integer not null default 0,
			scope text not null,
			repoId text not null
		)`,
		`create index idx_cache_entries_key_version on cache_entries (key, version)`,
		`create index idx_uploads_key_version on uploads (key, version)`,
		`create index idx_cache_entries_scope on cache_entries (scope)`,
		`create index idx_uploads_scope on uploads (scope)`,
		`create index idx_cache_entries_repoId on cache_entries (repoId)`,
		`create index idx_uploads_repoId on uploads (repoId)`,
	} {
		_, err := sqlDB.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	require.NoError(t, sqlDB.Close())

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	sqlDB, err = sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "updatedAt"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "repoId"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "folderName"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "lastPartUploadedAt"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "updated_at"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "repo_id"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "folder_name"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "last_part_uploaded_at"))
}

func TestUploadIDDoesNotRequireDatabaseIdentity(t *testing.T) {
	require.False(t, migrate.UploadsColumns[0].Increment)
}

func TestGeneratedSchemaMatchesOriginalColumns(t *testing.T) {
	require.Equal(t, []string{
		"id", "key", "version", "scope", "repoId", "updatedAt", "locationId",
	}, columnNames(migrate.CacheEntriesColumns))
	require.Equal(t, []string{
		"id", "folderName", "partCount", "mergeStartedAt", "mergedAt", "partsDeletedAt", "lastDownloadedAt",
	}, columnNames(migrate.StorageLocationsColumns))
	require.Equal(t, []string{
		"id", "key", "version", "scope", "repoId", "createdAt", "lastPartUploadedAt", "startedPartUploadCount", "finishedPartUploadCount", "folderName",
	}, columnNames(migrate.UploadsColumns))
}

func TestGeneratedSchemaMatchesOriginalStringTypes(t *testing.T) {
	require.Equal(t, map[string]string{
		dialect.MySQL:    "varchar(36)",
		dialect.Postgres: "text",
		dialect.SQLite:   "text",
	}, migrate.CacheEntriesColumns[0].SchemaType)
	require.Equal(t, map[string]string{
		dialect.MySQL:    "varchar(512)",
		dialect.Postgres: "text",
		dialect.SQLite:   "text",
	}, migrate.CacheEntriesColumns[1].SchemaType)
	require.Equal(t, map[string]string{
		dialect.MySQL:    "text",
		dialect.Postgres: "text",
		dialect.SQLite:   "text",
	}, migrate.UploadsColumns[9].SchemaType)
}

func TestGeneratedSchemaMatchesOriginalIndexNames(t *testing.T) {
	require.Equal(t, []string{
		"idx_cache_entries_key_version",
		"idx_cache_entries_scope",
		"idx_cache_entries_repoId",
	}, indexNames(migrate.CacheEntriesTable.Indexes))
	require.Equal(t, []string{
		"idx_uploads_key_version",
		"idx_uploads_scope",
		"idx_uploads_repoId",
	}, indexNames(migrate.UploadsTable.Indexes[:3]))
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

func columnNames(columns []*entschema.Column) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}

func indexNames(indexes []*entschema.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func sqliteColumnExists(ctx context.Context, t *testing.T, db *sql.DB, table string, column string) bool {
	t.Helper()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			name         string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		if name == column {
			return true
		}
	}
	require.NoError(t, rows.Err())
	return false
}
