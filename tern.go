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
	m := new(Migrator)

	for _, oFunc := range opts {
		oFunc(m, db)
	}

	if m.converter == nil {
		m.converter = localFSConverter{folder: DefaultMigrationsFolder}
	}

	if m.ex == nil {
		ex, err := createExecutor(db, DefaultMigrationsTable)
		if err != nil {
			return nil, err
		}
		m.ex = ex
	}

	return m, nil
}

func (m *Migrator) Up(ctx context.Context, cfs ...ActionConfigurator) ([]string, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	if err := m.ex.lock(ctx); err != nil {
		return nil, errors.Wrap(err, "migrations up lock failed")
	}

	defer func() {
		if err := m.ex.unlock(ctx); err != nil {
			panic(err) // fixme
		}
	}()

	migrations, err := m.converter.Convert(ctx, filter{})
	if err != nil {
		return nil, err
	}

	p := plan{steps: act.steps}
	migrated, err := m.ex.up(ctx, migrations, p);
	if err != nil {
		return nil, err
	}

	return migrated.Keys(), nil
}

func (m *Migrator) Down(ctx context.Context, cfs ...ActionConfigurator) error {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	if err := m.ex.lock(ctx); err != nil {
		return errors.Wrap(err, "down migrations lock failed")
	}

	defer func() {
		if err := m.ex.unlock(ctx); err != nil {
			panic(err) // fixme
		}
	}()

	migrations, err := m.converter.Convert(ctx, filter{})
	if err != nil {
		return err
	}

	if err := m.ex.down(ctx, migrations, plan{steps: act.steps}); err != nil {
		return err
	}

	return nil
}
