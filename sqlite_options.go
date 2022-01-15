package tern

import (
	"database/sql"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"time"
)

type SqliteOptionFunc func(*sqlgateway.SqliteOptions, *sqlgateway.ConnectOptions)

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

		m.db = gateway
		m.closerFns = append(m.closerFns, CloserFunc(closer))

		return nil
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

func WithSqliteMigrationTable(migrationTable string) SqliteOptionFunc {
	return func(mysqlOpts *sqlgateway.SqliteOptions, connectOpts *sqlgateway.ConnectOptions) {
		mysqlOpts.MigrationsTable = migrationTable
	}
}
