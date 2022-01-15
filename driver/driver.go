package driver

import (
	"database/sql"

	"github.com/pkg/errors"

	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
)

type (
	Dialect string

	CommonOptions struct {
		MigrationsTable string
		Charset         string
	}

	driverConfig struct {
		sqlConn         *sql.Conn
		mysqlOptions    *MySQLOptions
		postgresOptions *PostgresOptions
		sqliteOptions   *SqliteOptions
	}

	Configurator func(cfg *driverConfig)

	MySQLOptions struct {
		CommonOptions
		LockKey string
		LockFor int // maybe refactor to duration
		NoLock  bool
	}

	PostgresOptions struct {
		CommonOptions
		LockKey int
		LockFor int
		NoLock  bool
	}

	SqliteOptions struct {
		CommonOptions
	}
)

const (
	MySQL    = Dialect("mysql")
	Postgres = Dialect("postgres")
	Sqlite   = Dialect("sqlite")

	DefaultMigrationsTable = "migrations"
)

type Driver struct {
	dialect Dialect
	db      database.DB
}

func WithSqlConnection(conn *sql.Conn) Configurator {
	return func(cfg *driverConfig) {
		cfg.sqlConn = conn
	}
}

func WithMySQLOptions(options MySQLOptions) Configurator {
	return func(cfg *driverConfig) {
		cfg.mysqlOptions = &options
	}
}

func WithPostgresOptions(options PostgresOptions) Configurator {
	return func(cfg *driverConfig) {
		cfg.postgresOptions = &options
	}
}

func NewDriver(dialect Dialect, configurators ...Configurator) (*Driver, error) {
	var cfg driverConfig

	for _, configurator := range configurators {
		configurator(&cfg)
	}

	if cfg.sqlConn == nil {
		return nil, errors.New("opened SQL connection is required")
	}

	var drv Driver
	var db database.DB
	var err error
	switch dialect {
	case MySQL:
		db, err = createMySql(cfg)
	case Postgres:
		db, err = createPostgres(cfg)
	case Sqlite:
		db, err = createSqlite(cfg)
	default:
		return nil, errors.Errorf("invalid dialect %s", dialect)
	}

	if err != nil {
		return nil, err
	}

	drv.db = db

	return &drv, nil
}

func createMySql(cfg driverConfig) (database.DB, error) {
	if cfg.mysqlOptions == nil {
		cfg.mysqlOptions = &MySQLOptions{
			CommonOptions: CommonOptions{
				MigrationsTable: DefaultMigrationsTable,
			},
			NoLock: true,
		}
	}

	return sqlgateway.NewMySQLGateway(
		cfg.sqlConn,
		cfg.mysqlOptions.MigrationsTable,
		cfg.mysqlOptions.LockKey,
		cfg.mysqlOptions.LockFor,
		cfg.mysqlOptions.NoLock,
		cfg.mysqlOptions.Charset,
	), nil
}

func createPostgres(cfg driverConfig) (database.DB, error) {
	if cfg.postgresOptions == nil {
		cfg.postgresOptions = &PostgresOptions{
			CommonOptions: CommonOptions{
				MigrationsTable: DefaultMigrationsTable,
			},
			NoLock: true,
		}
	}

	return sqlgateway.NewPostgresGateway(
		cfg.sqlConn,
		cfg.mysqlOptions.MigrationsTable,
		cfg.mysqlOptions.LockKey,
		cfg.mysqlOptions.LockFor,
		cfg.mysqlOptions.NoLock,
		cfg.mysqlOptions.Charset,
	), nil
}

func createSqlite(cfg driverConfig) (database.DB, error) {
	if cfg.sqliteOptions == nil {
		cfg.sqliteOptions = &SqliteOptions{
			CommonOptions: CommonOptions{
				MigrationsTable: DefaultMigrationsTable,
			},
		}
	}

	return sqlgateway.NewSqliteGateway(
		cfg.sqlConn,
		cfg.mysqlOptions.MigrationsTable,
		cfg.mysqlOptions.LockKey,
		cfg.mysqlOptions.LockFor,
		cfg.mysqlOptions.NoLock,
		cfg.mysqlOptions.Charset,
	), nil
}
