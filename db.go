package tern

import (
	"database/sql"
	"github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/internal/database/sqlgateway"
	"time"
)

type (
	MySQLOptionFunc  func(*sqlgateway.MySQLOptions, *sqlgateway.ConnectOptions)
	SqliteOptionFunc func(*sqlgateway.SqliteOptions, *sqlgateway.ConnectOptions)
)

func UseMySQL(db *sql.DB, options ...MySQLOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		mysqlOpts := &sqlgateway.MySQLOptions{
			LockFor: sqlgateway.MysqlDefaultLockSeconds,
			LockKey: sqlgateway.MysqlDefaultLockKey,
			CommonOptions: database.CommonOptions{
				MigrationsTable:  database.DefaultMigrationsTable,
				MigratedAtColumn: database.MigratedAtColumn,
			},
		}

		connectOpts := sqlgateway.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(mysqlOpts, connectOpts)
		}

		connector := sqlgateway.MakeRetryingConnector(db, connectOpts)
		gateway, closer := sqlgateway.NewMySQLGateway(connector, mysqlOpts)

		m.closerFns = append(m.closerFns, CloserFunc(closer))
		m.gateway = gateway

		return nil
	}
}

func UseSqlite(db *sql.DB, options ...SqliteOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		sqliteOpts := &sqlgateway.SqliteOptions{
			CommonOptions: database.CommonOptions{
				MigrationsTable:  database.DefaultMigrationsTable,
				MigratedAtColumn: database.MigratedAtColumn,
			},
		}

		connectOpts := sqlgateway.NewDefaultConnectOptions()

		for _, oFunc := range options {
			oFunc(sqliteOpts, connectOpts)
		}

		connector := sqlgateway.MakeRetryingConnector(db, connectOpts)
		gateway, closer := sqlgateway.NewSqliteGateway(connector, sqliteOpts)

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

func WithSqliteMigratedAtColumn(column string) SqliteOptionFunc {
	return func(mysqlOpts *sqlgateway.SqliteOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.MigratedAtColumn = column
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

func WithMySQLMigratedAtColumn(column string) MySQLOptionFunc {
	return func(mysqlOpts *sqlgateway.MySQLOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.MigratedAtColumn = column
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
