package tern

import (
	"database/sql"
	"github.com/denismitr/tern/database"
	"time"
)

type (
	MySQLOptionFunc func(*database.MySQLOptions, *database.ConnectOptions)
	SqliteOptionFunc func(*database.SqliteOptions, *database.ConnectOptions)
)

func UseMySQL(db *sql.DB, options ...MySQLOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		mysqlOpts := &database.MySQLOptions{
			LockFor: database.MysqlDefaultLockSeconds,
			LockKey: database.MysqlDefaultLockKey,
			CommonOptions: database.CommonOptions{
				MigrationsTable: database.DefaultMigrationsTable,
			},
		}

		connectOpts := database.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(mysqlOpts, connectOpts)
		}

		connector := database.MakeRetryingConnector(db, connectOpts)
		gateway, err := database.NewMySQLGateway(connector, mysqlOpts)
		if err != nil {
			return err
		}

		m.gateway = gateway

		return nil
	}
}

func UseSqlite(db *sql.DB, options ...SqliteOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		sqliteOpts := &database.SqliteOptions{
			CommonOptions: database.CommonOptions{
				MigrationsTable: database.DefaultMigrationsTable,
			},
		}

		connectOpts := database.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(sqliteOpts, connectOpts)
		}

		connector := database.MakeRetryingConnector(db, connectOpts)
		gateway, err := database.NewSqliteGateway(connector, sqliteOpts)
		if err != nil {
			return err
		}

		m.gateway = gateway

		return nil
	}
}

func WithSqliteMigrationTable(migrationTable string) SqliteOptionFunc {
	return func(mysqlOpts *database.SqliteOptions, connectOpts *database.ConnectOptions) {
		mysqlOpts.MigrationsTable = migrationTable
	}
}

func WithSqliteMaxConnectionAttempts(attempts int) SqliteOptionFunc {
	return func(mysqlOpts *database.SqliteOptions, connectOpts *database.ConnectOptions) {
		connectOpts.MaxAttempts = attempts
	}
}

func WithSqliteConnectionTimeout(timeout time.Duration) SqliteOptionFunc {
	return func(mysqlOpts *database.SqliteOptions, connectOpts *database.ConnectOptions) {
		connectOpts.MaxTimeout = timeout
	}
}

func WithMySQLNoLock() MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
		mysqlOpts.NoLock = true
	}
}

func WithMySQLLockKey(key string) MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
		mysqlOpts.LockKey = key
	}
}

func WithMySQLMigrationTable(migrationTable string) MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
		mysqlOpts.MigrationsTable = migrationTable
	}
}

func WithMySQLLockFor(lockFor int) MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
		mysqlOpts.LockFor = lockFor
	}
}

func WithMySQLConnectionTimeout(timeout time.Duration) MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
		connectOpts.MaxTimeout = timeout
	}
}

func WithMySQLMaxConnectionAttempts(attempts int) MySQLOptionFunc {
	return func(mysqlOpts *database.MySQLOptions, connectOpts *database.ConnectOptions) {
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
