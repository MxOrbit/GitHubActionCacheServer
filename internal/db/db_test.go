package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
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

	for _, table := range []string{"cache_entries", "storage_deletions", "storage_locations", "uploads"} {
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
	_, err = sqlDB.ExecContext(ctx, `insert into uploads (
		id, key, version, createdAt, folderName, scope, repoId
	) values
		(1, 'legacy-key', 'version', 1, 'legacy-1', 'scope', 'repo'),
		(2, 'legacy-key', 'version', 2, 'legacy-2', 'scope', 'repo')`)
	require.NoError(t, err)
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
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "materializationUnsupportedAt"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "deletionRequestedAt"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "mergeLeaseToken"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "sizeBytes"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "recencyAt"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "lastPartUploadedAt"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "committedPartCount"))
	require.True(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "tupleHash"))
	var legacyUploads int
	require.NoError(t, sqlDB.QueryRowContext(ctx, `select count(*) from uploads where tupleHash is null`).Scan(&legacyUploads))
	require.Equal(t, 2, legacyUploads)
	var storageDeletionsTable string
	require.NoError(t, sqlDB.QueryRowContext(
		ctx,
		`select name from sqlite_master where type = 'table' and name = 'storage_deletions'`,
	).Scan(&storageDeletionsTable))
	require.Equal(t, "storage_deletions", storageDeletionsTable)
	var storageReaderLeasesTable string
	require.NoError(t, sqlDB.QueryRowContext(
		ctx,
		`select name from sqlite_master where type = 'table' and name = 'storage_reader_leases'`,
	).Scan(&storageReaderLeasesTable))
	require.Equal(t, "storage_reader_leases", storageReaderLeasesTable)
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "updated_at"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "cache_entries", "repo_id"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "storage_locations", "folder_name"))
	require.False(t, sqliteColumnExists(ctx, t, sqlDB, "uploads", "last_part_uploaded_at"))
	require.True(t, sqliteIndexExists(ctx, t, sqlDB, "idx_cache_entries_repo_scope_version_key"))
	require.True(t, sqliteIndexExists(ctx, t, sqlDB, "idx_cache_entries_location_updated_at"))
	require.True(t, sqliteIndexExists(ctx, t, sqlDB, "idx_uploads_tuple_hash"))
	require.True(t, sqliteIndexExists(ctx, t, sqlDB, "idx_storage_locations_recency"))
}

func TestUploadIDDoesNotRequireDatabaseIdentity(t *testing.T) {
	require.False(t, migrate.UploadsColumns[0].Increment)
}

func TestOpenAndMigrateBackfillsStorageLocationRecency(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)

	fallback := client.StorageLocation.Create().
		SetID("fallback").
		SetFolderName("fallback-folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("fallback-entry").
		SetKey("fallback-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(100).
		SetLocation(fallback).
		SaveX(ctx)
	downloaded := client.StorageLocation.Create().
		SetID("downloaded").
		SetFolderName("downloaded-folder").
		SetPartCount(1).
		SetLastDownloadedAt(200).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("downloaded-entry").
		SetKey("downloaded-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(100).
		SetLocation(downloaded).
		SaveX(ctx)
	pending := client.StorageLocation.Create().
		SetID("pending").
		SetFolderName("pending-folder").
		SetPartCount(1).
		SetDeletionRequestedAt(1).
		SaveX(ctx)
	stale := client.StorageLocation.Create().
		SetID("stale").
		SetFolderName("stale-folder").
		SetPartCount(1).
		SetLastDownloadedAt(200).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("stale-entry").
		SetKey("stale-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(100).
		SetLocation(stale).
		SaveX(ctx)
	maintained := client.StorageLocation.Create().
		SetID("maintained").
		SetFolderName("maintained-folder").
		SetPartCount(1).
		SetLastDownloadedAt(100).
		SetRecencyAt(200).
		SaveX(ctx)
	require.NoError(t, client.Close())

	// Simulate rows written before the recencyAt column existed, plus a row an
	// older binary touched (recencyAt < lastDownloadedAt) after a rollback.
	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE storage_locations SET recencyAt = 0 WHERE id != 'maintained'`)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE storage_locations SET recencyAt = 5 WHERE id = 'stale'`)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	client, err = OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	require.Equal(t, int64(100), client.StorageLocation.GetX(ctx, fallback.ID).RecencyAt)
	require.Equal(t, int64(200), client.StorageLocation.GetX(ctx, downloaded.ID).RecencyAt)
	require.Zero(t, client.StorageLocation.GetX(ctx, pending.ID).RecencyAt)
	require.Equal(t, int64(200), client.StorageLocation.GetX(ctx, stale.ID).RecencyAt)
	require.Equal(t, int64(200), client.StorageLocation.GetX(ctx, maintained.ID).RecencyAt)
}

func TestStorageDeletionIDUsesDatabaseIdentity(t *testing.T) {
	require.True(t, migrate.StorageDeletionsColumns[0].Increment)
}

func TestMySQLMigrationLockResult(t *testing.T) {
	tests := []struct {
		name       string
		result     sql.NullInt64
		acquired   bool
		wantErr    bool
		errorMatch string
	}{
		{name: "acquired", result: sql.NullInt64{Int64: 1, Valid: true}, acquired: true},
		{name: "timeout", result: sql.NullInt64{Int64: 0, Valid: true}},
		{name: "null", result: sql.NullInt64{}, wantErr: true, errorMatch: "returned NULL"},
		{name: "unexpected", result: sql.NullInt64{Int64: 2, Valid: true}, wantErr: true, errorMatch: "unexpected GET_LOCK result"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			acquired, err := mysqlLockAcquired(test.result)
			require.Equal(t, test.acquired, acquired)
			if test.wantErr {
				require.ErrorContains(t, err, test.errorMatch)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMySQLMigrationLockNameIsDatabaseScopedAndBounded(t *testing.T) {
	first := mysqlMigrationLockName("cache-a")
	require.Equal(t, first, mysqlMigrationLockName("cache-a"))
	require.NotEqual(t, first, mysqlMigrationLockName("cache-b"))
	require.LessOrEqual(t, len(first), 64)
}

func TestGeneratedSchemaMatchesOriginalColumns(t *testing.T) {
	require.Equal(t, []string{
		"id", "key", "version", "scope", "repoId", "updatedAt", "locationId",
	}, columnNames(migrate.CacheEntriesColumns))
	require.Equal(t, []string{
		"id", "folderName", "createdAt", "attemptCount", "lastAttemptedAt", "lastError",
	}, columnNames(migrate.StorageDeletionsColumns))
	require.Equal(t, []string{
		"id", "folderName", "partCount", "sizeBytes", "leaseVersion", "deletionRequestedAt", "mergeStartedAt", "mergeLeaseToken", "mergeLeaseExpiresAt", "mergedAt", "materializationUnsupportedAt", "partsDeletedAt", "lastDownloadedAt", "recencyAt",
	}, columnNames(migrate.StorageLocationsColumns))
	require.Equal(t, []string{
		"id", "scope", "expiresAt", "storageLocationId",
	}, columnNames(migrate.StorageReaderLeasesColumns))
	require.Equal(t, []string{
		"id", "key", "version", "scope", "repoId", "createdAt", "lastPartUploadedAt", "startedPartUploadCount", "finishedPartUploadCount", "folderName", "committedPartCount", "tupleHash",
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
		"idx_cache_entries_repo_scope_version_key",
		"idx_cache_entries_location_updated_at",
	}, indexNames(migrate.CacheEntriesTable.Indexes))
	require.Equal(t, []string{
		"idx_uploads_key_version",
		"idx_uploads_scope",
		"idx_uploads_repoId",
		"idx_uploads_tuple_hash",
	}, indexNames(migrate.UploadsTable.Indexes))
	require.Equal(t, []string{
		"idx_storage_locations_recency",
	}, indexNames(migrate.StorageLocationsTable.Indexes))
}

func TestMySQLUploadUniqueIndexUsesFixedLengthTupleHash(t *testing.T) {
	uniqueIndex := migrate.UploadsTable.Indexes[3]
	require.True(t, uniqueIndex.Unique)
	require.Equal(t, []string{"tupleHash"}, columnNames(uniqueIndex.Columns))
	require.Equal(t, "varchar(64)", migrate.UploadsColumns[11].SchemaType[dialect.MySQL])
}

func TestCacheMatchIndexAnnotations(t *testing.T) {
	matchIndex := migrate.CacheEntriesTable.Indexes[3]
	require.Equal(t, []string{"repoId", "scope", "version", "key"}, columnNames(matchIndex.Columns))
	require.Equal(t, map[string]uint{
		"repoId":  64,
		"scope":   191,
		"version": 64,
		"key":     191,
	}, matchIndex.Annotation.PrefixColumns)
	require.Equal(t, map[string]string{
		"key": "text_pattern_ops",
	}, matchIndex.Annotation.OpClassColumns)
}

func TestSQLiteCacheMatchQueryPlans(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	tests := []struct {
		name  string
		query string
		args  []any
		want  string
	}{
		{
			name: "exact",
			query: `EXPLAIN QUERY PLAN
				SELECT id FROM cache_entries
				WHERE repoId = ? AND scope = ? AND version = ? AND key = ?
				LIMIT 1`,
			args: []any{"123", "refs/heads/main", "version", "linux-cache"},
			want: "idx_cache_entries_repo_scope_version_key (repoId=? AND scope=? AND version=? AND key=?)",
		},
		{
			name: "restore prefix",
			query: `EXPLAIN QUERY PLAN
				SELECT id FROM cache_entries
				WHERE repoId = ? AND scope = ? AND version = ? AND key LIKE ?
					AND substr(key, 1, length(?)) COLLATE BINARY = ?
				ORDER BY updatedAt DESC
				LIMIT 1`,
			args: []any{"123", "refs/heads/main", "version", "linux-%", "linux-", "linux-"},
			want: "idx_cache_entries_repo_scope_version_key (repoId=? AND scope=? AND version=?)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := sqliteQueryPlan(ctx, t, sqlDB, test.query, test.args...)
			require.Contains(t, plan, test.want)
		})
	}
}

func TestSQLiteRetentionLookupUsesLocationUpdatedAtIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache-server.db")

	client, err := OpenAndMigrate(ctx, config.DBConfig{
		Driver:     DriverSQLite,
		SQLitePath: dbPath,
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	plan := sqliteQueryPlan(ctx, t, sqlDB, `EXPLAIN QUERY PLAN
		SELECT id FROM cache_entries
		WHERE locationId = ? AND updatedAt >= ?
		LIMIT 1`, "location-id", int64(123))
	require.Contains(t, plan, "idx_cache_entries_location_updated_at (locationId=? AND updatedAt>?)")
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
	require.Equal(t, config.DefaultDBMySQLTLS, parsed.TLSConfig)
	require.Contains(t, dsn, "/cache%2Fdb%3Fname?")
	require.False(t, strings.Contains(dsn, "/cache/db?name?"))
}

func TestPostgresDSNUsesConfiguredSSLMode(t *testing.T) {
	tests := []struct {
		name    string
		sslMode string
		want    string
	}{
		{name: "default", want: config.DefaultDBPostgresSSLMode},
		{name: "configured", sslMode: "verify-full", want: "verify-full"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := postgresDSN(config.DBConfig{
				PostgresDatabase: "cache/db",
				PostgresHost:     "db.example",
				PostgresPort:     "5432",
				PostgresUser:     "cache",
				PostgresPassword: "secret",
				PostgresSSLMode:  tt.sslMode,
			})
			require.NoError(t, err)
			parsed, err := url.Parse(dsn)
			require.NoError(t, err)
			require.Equal(t, tt.want, parsed.Query().Get("sslmode"))
			require.Equal(t, "/cache/db", parsed.Path)
		})
	}
}

func TestPostgresDSNLeavesFullURLUnchanged(t *testing.T) {
	const dsn = "postgres://cache@db.example/cache?sslmode=disable&application_name=cache-server"

	got, err := postgresDSN(config.DBConfig{
		PostgresURL:     dsn,
		PostgresSSLMode: "verify-full",
	})

	require.NoError(t, err)
	require.Equal(t, dsn, got)
}

func TestMySQLDSNUsesConfiguredTLSMode(t *testing.T) {
	for _, tlsMode := range []string{"false", "true", "skip-verify", "preferred"} {
		t.Run(tlsMode, func(t *testing.T) {
			dsn := mysqlDSN(config.DBConfig{
				MySQLHost:     "db.example",
				MySQLPort:     "3306",
				MySQLDatabase: "cache",
				MySQLUser:     "cache",
				MySQLTLS:      tlsMode,
			})
			parsed, err := mysql.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, tlsMode, parsed.TLSConfig)
		})
	}
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

func sqliteIndexExists(ctx context.Context, t *testing.T, db *sql.DB, index string) bool {
	t.Helper()

	var name string
	err := db.QueryRowContext(
		ctx,
		`select name from sqlite_master where type = 'index' and name = ?`,
		index,
	).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return name == index
}

func sqliteQueryPlan(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()

	rows, err := db.QueryContext(ctx, query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(details, "\n")
}
