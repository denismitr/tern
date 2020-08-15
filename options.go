package tern

import "github.com/jmoiron/sqlx"

type OptionFunc func(m *Migrator, db *sqlx.DB)

func UseLocalFolder(folder string) OptionFunc {
	return func(m *Migrator, _ *sqlx.DB) {
		m.converter = localFSConverter{folder: folder}
	}
}

func WithMysqlConfig(migrationsTable, lockKey string, lockFor int) OptionFunc {
	return func(m *Migrator, db *sqlx.DB) {
		m.ex = &mysqlGateway{
			db: db,
			migrationsTable: migrationsTable,
			lockFor: lockFor,
			lockKey: lockKey,
		}
	}
}

type action struct {
	steps int
}

type ActionConfigurator func (a *action)

func WithSteps(steps int) ActionConfigurator {
	return func (a *action) {
		a.steps = steps
	}
}
