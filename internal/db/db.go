package db

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/migrate"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const (
	DriverSQLite   = "sqlite"
	DriverPostgres = "postgres"
	DriverMySQL    = "mysql"
)

func Open(ctx context.Context, cfg config.DBConfig) (*ent.Client, error) {
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
	client, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := Migrate(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
	}

	return client, nil
}

func Migrate(ctx context.Context, client *ent.Client) error {
	return client.Schema.Create(ctx, migrate.WithForeignKeys(true))
}

func openSQLite(ctx context.Context, path string) (*ent.Client, error) {
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

	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.SQLite, sqlDB))), nil
}

func openPostgres(ctx context.Context, cfg config.DBConfig) (*ent.Client, error) {
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

	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, sqlDB))), nil
}

func openMySQL(ctx context.Context, cfg config.DBConfig) (*ent.Client, error) {
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

	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.MySQL, sqlDB))), nil
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
