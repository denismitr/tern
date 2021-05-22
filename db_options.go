package tern

import (
	"database/sql"
	database2 "github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/internal/database/sqlgateway"
	"time"
)

type (
	MySQLOptionFunc func(*sqlgateway.MySQLOptions, *sqlgateway.ConnectOptions)
	SqliteOptionFunc func(*sqlgateway.SqliteOptions, *sqlgateway.ConnectOptions)
)

func UseMySQL(db *sql.DB, options ...MySQLOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		mysqlOpts := &sqlgateway.MySQLOptions{
			LockFor: sqlgateway.MysqlDefaultLockSeconds,
			LockKey: sqlgateway.MysqlDefaultLockKey,
			CommonOptions: database2.CommonOptions{
				MigrationsTable: database2.DefaultMigrationsTable,
			},
		}

		connectOpts := sqlgateway.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(mysqlOpts, connectOpts)
		}

		connector := sqlgateway.MakeRetryingConnector(db, connectOpts)
		gateway, closer, err := sqlgateway.NewMySQLGateway(connector, mysqlOpts)
		if err != nil {
			return err
		}

		m.closerFns = append(m.closerFns, CloserFunc(closer))
		m.gateway = gateway

		return nil
	}
}

func UseSqlite(db *sql.DB, options ...SqliteOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		sqliteOpts := &sqlgateway.SqliteOptions{
			CommonOptions: database2.CommonOptions{
				MigrationsTable: database2.DefaultMigrationsTable,
			},
		}

		connectOpts := sqlgateway.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(sqliteOpts, connectOpts)
		}

		connector := sqlgateway.MakeRetryingConnector(db, connectOpts)
		gateway, closer, err := sqlgateway.NewSqliteGateway(connector, sqliteOpts)
		if err != nil {
			return err
		}

		m.gateway = gateway
		m.closerFns = append(m.closerFns, CloserFunc(closer))

		return nil
	}
}

func WithSqliteMigrationTable(migrationTable string) SqliteOptionFunc {
	return func(mysqlOpts *sqlgateway.SqliteOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.MigrationsTable = migrationTable
	}
}

func WithSqliteMaxConnectionAttempts(attempts int) SqliteOptionFunc {
	return func(mysqlOpts *sqlgateway.SqliteOptions, connectOpts *sqlgateway.ConnectOptions) {
		connectOpts.MaxAttempts = attempts
	}
}

func WithSqliteConnectionTimeout(timeout time.Duration) SqliteOptionFunc {
	return func(mysqlOpts *sqlgateway.SqliteOptions, connectOpts *sqlgateway.ConnectOptions) {
		connectOpts.MaxTimeout = timeout
	}
}

func WithMySQLNoLock() MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.NoLock = true
	}
}

func WithMySQLLockKey(key string) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.LockKey = key
	}
}

func WithMySQLMigrationTable(migrationTable string) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.MigrationsTable = migrationTable
	}
}

func WithMySQLLockFor(lockFor int) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.LockFor = lockFor
	}
}

func WithMySQLConnectionTimeout(timeout time.Duration) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		connectOpts.MaxTimeout = timeout
	}
}

func WithMySQLMaxConnectionAttempts(attempts int) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		connectOpts.MaxAttempts = attempts
	}
}

type action struct {
	steps int
	keys  []string
}

func WithSteps(steps int) ActionConfigurator {
	return func(a *action) {
		a.steps = steps
	}
}

func WithKeys(keys ...string) ActionConfigurator {
	return func(a *action) {
		a.keys = keys
	}
}
