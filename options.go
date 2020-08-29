package tern

import (
	"database/sql"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/migration"
	"github.com/denismitr/tern/source"
	"time"
)

type OptionFunc func(*Migrator) error
type MySQLOptionFunc func(*database.MySQLOptions, *database.ConnectOptions)
type ActionConfigurator func(a *action)

func UseLocalFolderSource(folder string) OptionFunc {
	return func(m *Migrator) error {
		conv, err := source.NewLocalFSSource(folder)
		if err != nil {
			return err
		}

		m.converter = conv
		return nil
	}
}

func UseInMemorySource(migrations ...*migration.Migration) OptionFunc {
	return func(m *Migrator) error {
		conv := source.NewInMemorySource(migrations...)

		m.converter = conv
		return nil
	}
}

func UseMySQL(db *sql.DB, options ...MySQLOptionFunc) OptionFunc {
	return func(m *Migrator) error {
		mysqlOpts := &database.MySQLOptions{
			LockFor: database.MysqlDefaultLockSeconds,
			LockKey: database.MysqlDefaultLockKey,
			CommonOptions: database.CommonOptions{
				MigrationsTable: database.DefaultMigrationsTable,
			},
		}

		connectOpts := &database.ConnectOptions{
			MaxAttempts: 10,
			MaxTimeout:  30 * time.Second,
			Step:        1 * time.Second,
		}

		for _, oFunc := range options {
			oFunc(mysqlOpts, connectOpts)
		}

		connector := database.MakeRetryingConnector(connectOpts)
		gateway, err := database.NewMySQLGateway(db, connector, mysqlOpts)
		if err != nil {
			return err
		}

		m.gateway = gateway

		return nil
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
