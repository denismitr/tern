package tern

import (
	"github.com/jmoiron/sqlx"
	"github.com/denismitr/tern/converter"
	"github.com/denismitr/tern/database"
)

type OptionFunc func(m *Migrator, db *sqlx.DB) error

func UseLocalFolderSource(folder string) OptionFunc {
	return func(m *Migrator, _ *sqlx.DB) error {
		conv, err := converter.NewLocalFSConverter(folder)
		if err != nil {
			return err
		}

		m.converter = conv
		return nil
	}
}

func WithMysqlConfig(migrationsTable, lockKey string, lockFor int) OptionFunc {
	return func(m *Migrator, db *sqlx.DB) error {
		gateway, err := database.NewMysqlGateway(db, migrationsTable, lockKey, lockFor)
		if err != nil {
			return err
		}

		m.gateway = gateway
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
