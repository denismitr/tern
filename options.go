package tern

import (
	"database/sql"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/migration"
	"github.com/denismitr/tern/source"
)

type OptionFunc func(m *Migrator, driver string, db *sql.DB) error

func UseLocalFolderSource(folder string) OptionFunc {
	return func(m *Migrator, _ string, _ *sql.DB) error {
		conv, err := source.NewLocalFSSource(folder)
		if err != nil {
			return err
		}

		m.converter = conv
		return nil
	}
}

func UseInMemorySource(migrations ...*migration.Migration) OptionFunc {
	return func(m *Migrator, _ string, _ *sql.DB) error {
		conv := source.NewInMemorySource(migrations...)

		m.converter = conv
		return nil
	}
}

func WithMysqlConfig(migrationsTable, lockKey string, lockFor int, connectOptions *database.ConnectOptions) OptionFunc {
	return func(m *Migrator, driver string, db *sql.DB) error {
		connector := database.MakeRetryingConnector(connectOptions)
		gateway, err := database.NewMysqlGateway(db, connector, migrationsTable, lockKey, lockFor)
		if err != nil {
			return err
		}

		m.gateway = gateway
		return nil
	}
}

func WithConnectOptions(connectOptions *database.ConnectOptions) OptionFunc {
	return func(m *Migrator, driver string, db *sql.DB) error {
		m.connectOptions = connectOptions
		return nil
	}
}

type action struct {
	steps int
	keys []string
}

type ActionConfigurator func (a *action)

func WithSteps(steps int) ActionConfigurator {
	return func (a *action) {
		a.steps = steps
	}
}

func WithKeys(keys ...string) ActionConfigurator {
	return func (a *action) {
		a.keys = keys
	}
}
