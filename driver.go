package tern

import (
	"context"
	"database/sql"
	"time"

	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
)

type (
	Dialect string

	CommonOptions struct {
		MigrationsTable string
		Charset         string
		Timeout         time.Duration
	}

	driverConfig struct {
		sqlConn         *sql.Conn
		mysqlOptions    *MySQLOptions
		postgresOptions *PostgresOptions
		sqliteOptions   *SqliteOptions
	}

	Configurator interface {
		configure(sql *sql.Conn) (database.Effector, Dialect, error)
	}

	MySQLOptions struct {
		CommonOptions
		LockKey string
		LockFor int // maybe refactor to duration
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
	MySQLDialect    = Dialect("mysql")
	PostgresDialect = Dialect("postgres")
	SqliteDialect   = Dialect("sqlite")

	DefaultMigrationsTable = "migrations"
)

type Driver struct {
	dialect  Dialect
	effector database.Effector
}

func NewDefaultMySQLOptions() MySQLOptions {
	return MySQLOptions{
		CommonOptions: CommonOptions{
			MigrationsTable: DefaultMigrationsTable,
			Charset: "utf8",
		},
		LockKey: "",
		LockFor: 0,
	}
}

func NewSQLDriver(db *sql.DB, configurator Configurator) (*Driver, CloserFunc, error) {
	conn, err := db.Conn(context.TODO())
	if err != nil {
		return nil, nil, err // TODO: wrap
	}

	var drv Driver

	effector, dialect, err := configurator.configure(conn)
	if err != nil {
		return nil, nil, err
	}

	drv.effector = effector
	drv.dialect = dialect

	return &drv, func() error { return conn.Close() }, nil
}

func (mysql MySQLOptions) configure(conn *sql.Conn) (database.Effector, Dialect, error) {
	if mysql.MigrationsTable == "" {
		mysql.MigrationsTable = DefaultMigrationsTable
	}

	if mysql.Charset == "" {
		mysql.Charset = "utf8"
	}

	return sqlgateway.NewMySQLGateway(
		conn,
		mysql.MigrationsTable,
		mysql.LockKey,
		mysql.LockFor,
		mysql.LockFor == 0 || mysql.LockKey == "",
		mysql.Charset,
	), MySQLDialect, nil
}

func createPostgres(cfg driverConfig) (database.Effector, error) {
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
		cfg.postgresOptions.MigrationsTable,
		cfg.postgresOptions.LockKey,
		cfg.postgresOptions.LockFor,
		cfg.postgresOptions.NoLock,
		cfg.postgresOptions.Charset,
	), nil
}

func createSqlite(cfg driverConfig) (database.Effector, error) {
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
	), nil
}
