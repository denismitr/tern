package tern

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const DefaultMigrationsTable = "migrations"
const DefaultMigrationsFolder = "./migrations"

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")

type Migrator struct {
	ex        gateway
	converter converter
}

func NewMigrator(db *sqlx.DB, opts ...OptionFunc) (*Migrator, error) {
	ex, err := createExecutor(db, DefaultMigrationsTable)
	if err != nil {
		return nil, err
	}

	m := &Migrator{
		ex:        ex,
		converter: localFSConverter{folder: DefaultMigrationsFolder},
	}

	for _, oFunc := range opts {
		oFunc(m)
	}

	return m, nil
}

func (m *Migrator) Up(ctx context.Context, cfs ...ActionConfigurator) ([]string, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.converter.Convert(ctx, filter{})
	if err != nil {
		return nil, err
	}

	p := plan{steps: act.steps}
	if migrated, err := m.ex.up(ctx, migrations, p); err != nil {
		return nil, err
	} else {
		return migrated.Keys(), nil
	}
}

func (m *Migrator) Down(ctx context.Context, cfs ...ActionConfigurator) error {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.converter.Convert(ctx, filter{})
	if err != nil {
		return err
	}

	if err := m.ex.down(ctx, migrations, plan{steps: act.steps}); err != nil {
		return err
	}

	return nil
}

func createExecutor(db *sqlx.DB, tableName string) (gateway, error) {
	driver := db.DriverName()

	switch driver {
	case "mysql":
		return newMysqlGateway(db, tableName)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}
