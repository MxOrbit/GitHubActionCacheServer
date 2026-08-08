package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"entgo.io/ent/dialect"
	entsqldialect "entgo.io/ent/dialect/sql"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/migrate"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"
)

const (
	DriverSQLite   = "sqlite"
	DriverPostgres = "postgres"
	DriverMySQL    = "mysql"

	// First 63 bits of SHA-256("github.com/MxOrbit/GitHubActionCacheServer:schema-migration").
	postgresMigrationLockKey      = int64(2389201325396535817)
	mysqlMigrationLockWaitSeconds = 10
	migrationUnlockTimeout        = 5 * time.Second
)

type openedDatabase struct {
	client *ent.Client
	sqlDB  *sql.DB
}

func openDatabase(ctx context.Context, cfg config.DBConfig) (*openedDatabase, error) {
	switch cfg.Driver {
	case DriverSQLite:
		return openSQLite(ctx, cfg.SQLitePath)
	case DriverPostgres:
		return openPostgres(ctx, cfg)
	case DriverMySQL:
		return openMySQL(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported DB_DRIVER %q", cfg.Driver)
	}
}

func OpenAndMigrate(ctx context.Context, cfg config.DBConfig) (*ent.Client, error) {
	opened, err := openDatabase(ctx, cfg)
	if err != nil {
		return nil, err
	}

	switch cfg.Driver {
	case DriverPostgres:
		err = migratePostgresWithLock(ctx, opened.sqlDB)
	case DriverMySQL:
		err = migrateMySQLWithLock(ctx, cfg, opened)
	case DriverSQLite:
		err = migrateSchema(ctx, opened.client)
		if err == nil {
			err = backfillStorageLocationRecency(ctx, opened.sqlDB, DriverSQLite)
		}
	default:
		err = fmt.Errorf("unsupported DB_DRIVER %q", cfg.Driver)
	}
	if err != nil {
		_ = opened.client.Close()
		return nil, err
	}

	return opened.client, nil
}

func migrateSchema(ctx context.Context, client *ent.Client) error {
	return client.Schema.Create(ctx, migrate.WithForeignKeys(true))
}

type sqlExecContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// backfillStorageLocationRecency materializes eviction recency for rows written
// before the recencyAt column existed, and repairs rows an older binary touched
// without maintaining recencyAt (recencyAt < lastDownloadedAt is unreachable for
// maintained rows). Idempotent. Must run under the same lock as the DDL.
func backfillStorageLocationRecency(ctx context.Context, exec sqlExecContext, driver string) error {
	query := `UPDATE storage_locations SET "recencyAt" = COALESCE("lastDownloadedAt", (SELECT MAX("updatedAt") FROM cache_entries WHERE cache_entries."locationId" = storage_locations."id"), 0) WHERE "deletionRequestedAt" IS NULL AND ("recencyAt" = 0 OR ("lastDownloadedAt" IS NOT NULL AND "recencyAt" < "lastDownloadedAt"))`
	if driver == DriverMySQL {
		query = strings.ReplaceAll(query, `"`, "`")
	}
	if _, err := exec.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("backfill storage location recency: %w", err)
	}
	return nil
}

func openSQLite(ctx context.Context, path string) (*openedDatabase, error) {
	if path == "" {
		return nil, fmt.Errorf("DB_SQLITE_PATH is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	sqlDB.SetMaxOpenConns(1)
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("enable sqlite foreign keys: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	return &openedDatabase{
		client: ent.NewClient(ent.Driver(entsqldialect.OpenDB(dialect.SQLite, sqlDB))),
		sqlDB:  sqlDB,
	}, nil
}

func openPostgres(ctx context.Context, cfg config.DBConfig) (*openedDatabase, error) {
	dsn, err := postgresDSN(cfg)
	if err != nil {
		return nil, err
	}

	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &openedDatabase{
		client: ent.NewClient(ent.Driver(entsqldialect.OpenDB(dialect.Postgres, sqlDB))),
		sqlDB:  sqlDB,
	}, nil
}

func openMySQL(ctx context.Context, cfg config.DBConfig) (*openedDatabase, error) {
	if cfg.MySQLDatabase == "" || cfg.MySQLHost == "" || cfg.MySQLUser == "" {
		return nil, fmt.Errorf("mysql requires DB_MYSQL_DATABASE, DB_MYSQL_HOST and DB_MYSQL_USER")
	}

	dsn := mysqlDSN(cfg)
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	return &openedDatabase{
		client: ent.NewClient(ent.Driver(entsqldialect.OpenDB(dialect.MySQL, sqlDB))),
		sqlDB:  sqlDB,
	}, nil
}

type postgresMigrationDriver struct {
	conn entsqldialect.Conn
}

func (d *postgresMigrationDriver) Exec(ctx context.Context, query string, args, value any) error {
	return d.conn.Exec(ctx, query, args, value)
}

func (d *postgresMigrationDriver) Query(ctx context.Context, query string, args, value any) error {
	return d.conn.Query(ctx, query, args, value)
}

func (d *postgresMigrationDriver) Tx(context.Context) (dialect.Tx, error) {
	return dialect.NopTx(d), nil
}

func (*postgresMigrationDriver) Close() error {
	return nil
}

func (*postgresMigrationDriver) Dialect() string {
	return dialect.Postgres
}

func migratePostgresWithLock(ctx context.Context, sqlDB *sql.DB) (retErr error) {
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin postgres schema migration transaction: %w", err)
	}
	defer func() {
		if tx == nil {
			return
		}
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			retErr = errors.Join(retErr, fmt.Errorf("rollback postgres schema migration: %w", err))
		}
	}()

	logger := zerolog.Ctx(ctx)
	waitStartedAt := time.Now()
	logger.Info().Str("driver", DriverPostgres).Msg("waiting for database schema migration lock")
	if _, err := tx.ExecContext(ctx, "select pg_advisory_xact_lock($1)", postgresMigrationLockKey); err != nil {
		return fmt.Errorf("acquire postgres schema migration lock: %w", err)
	}
	logger.Info().Str("driver", DriverPostgres).Dur("wait", time.Since(waitStartedAt)).Msg("database schema migration lock acquired")

	driver := &postgresMigrationDriver{
		conn: entsqldialect.Conn{ExecQuerier: tx},
	}
	if err := migrate.NewSchema(driver).Create(ctx, migrate.WithForeignKeys(true)); err != nil {
		return err
	}
	if err := backfillStorageLocationRecency(ctx, tx, DriverPostgres); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres schema migration: %w", err)
	}
	tx = nil
	return nil
}

type mysqlMigrationLock struct {
	db       *sql.DB
	conn     *sql.Conn
	lockName string
}

func migrateMySQLWithLock(ctx context.Context, cfg config.DBConfig, opened *openedDatabase) (retErr error) {
	lock, err := acquireMySQLMigrationLock(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, lock.release())
	}()
	if err := migrateSchema(ctx, opened.client); err != nil {
		return err
	}
	return backfillStorageLocationRecency(ctx, opened.sqlDB, DriverMySQL)
}

func acquireMySQLMigrationLock(ctx context.Context, cfg config.DBConfig) (*mysqlMigrationLock, error) {
	lockDB, err := sql.Open("mysql", mysqlDSN(cfg))
	if err != nil {
		return nil, fmt.Errorf("open mysql schema migration lock connection: %w", err)
	}
	lockDB.SetMaxOpenConns(1)
	lockDB.SetMaxIdleConns(1)

	conn, err := lockDB.Conn(ctx)
	if err != nil {
		_ = lockDB.Close()
		return nil, fmt.Errorf("reserve mysql schema migration lock connection: %w", err)
	}

	lockName := mysqlMigrationLockName(cfg.MySQLDatabase)
	lock := &mysqlMigrationLock{db: lockDB, conn: conn, lockName: lockName}
	logger := zerolog.Ctx(ctx)
	waitStartedAt := time.Now()
	logger.Info().Str("driver", DriverMySQL).Msg("waiting for database schema migration lock")
	for {
		var result sql.NullInt64
		if err := conn.QueryRowContext(
			ctx,
			"select get_lock(?, ?)",
			lockName,
			mysqlMigrationLockWaitSeconds,
		).Scan(&result); err != nil {
			_ = lock.close()
			return nil, fmt.Errorf("acquire mysql schema migration lock: %w", err)
		}
		acquired, err := mysqlLockAcquired(result)
		if err != nil {
			_ = lock.close()
			return nil, err
		}
		if acquired {
			logger.Info().Str("driver", DriverMySQL).Dur("wait", time.Since(waitStartedAt)).Msg("database schema migration lock acquired")
			return lock, nil
		}
	}
}

func mysqlLockAcquired(result sql.NullInt64) (bool, error) {
	if !result.Valid {
		return false, fmt.Errorf("acquire mysql schema migration lock: GET_LOCK returned NULL")
	}
	switch result.Int64 {
	case 1:
		return true, nil
	case 0:
		return false, nil
	default:
		return false, fmt.Errorf("acquire mysql schema migration lock: unexpected GET_LOCK result %d", result.Int64)
	}
}

func mysqlMigrationLockName(database string) string {
	digest := sha256.Sum256([]byte(database))
	return "gacs:schema-migration:" + hex.EncodeToString(digest[:20])
}

func (l *mysqlMigrationLock) release() error {
	ctx, cancel := context.WithTimeout(context.Background(), migrationUnlockTimeout)
	defer cancel()

	var errs []error
	var result sql.NullInt64
	if err := l.conn.QueryRowContext(ctx, "select release_lock(?)", l.lockName).Scan(&result); err != nil {
		errs = append(errs, fmt.Errorf("release mysql schema migration lock: %w", err))
	} else if !result.Valid || result.Int64 != 1 {
		errs = append(errs, fmt.Errorf("release mysql schema migration lock: unexpected RELEASE_LOCK result %v", result))
	}
	errs = append(errs, l.close())
	return errors.Join(errs...)
}

func (l *mysqlMigrationLock) close() error {
	return errors.Join(l.conn.Close(), l.db.Close())
}

func mysqlDSN(cfg config.DBConfig) string {
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = cfg.MySQLUser
	mysqlCfg.Passwd = cfg.MySQLPassword
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = net.JoinHostPort(cfg.MySQLHost, cfg.MySQLPort)
	mysqlCfg.DBName = cfg.MySQLDatabase
	mysqlCfg.ParseTime = true
	mysqlCfg.TLSConfig = cfg.MySQLTLS
	if mysqlCfg.TLSConfig == "" {
		mysqlCfg.TLSConfig = config.DefaultDBMySQLTLS
	}
	return mysqlCfg.FormatDSN()
}

func postgresDSN(cfg config.DBConfig) (string, error) {
	if cfg.PostgresURL != "" {
		return cfg.PostgresURL, nil
	}
	if cfg.PostgresDatabase == "" || cfg.PostgresHost == "" || cfg.PostgresUser == "" {
		return "", fmt.Errorf("postgres requires DB_POSTGRES_URL or DB_POSTGRES_DATABASE, DB_POSTGRES_HOST and DB_POSTGRES_USER")
	}
	sslMode := cfg.PostgresSSLMode
	if sslMode == "" {
		sslMode = config.DefaultDBPostgresSSLMode
	}
	values := url.Values{}
	values.Set("sslmode", sslMode)
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.PostgresUser, cfg.PostgresPassword),
		Host:     net.JoinHostPort(cfg.PostgresHost, cfg.PostgresPort),
		Path:     cfg.PostgresDatabase,
		RawQuery: values.Encode(),
	}
	return u.String(), nil
}
