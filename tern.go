package tern

import (
	"context"
	"github.com/denismitr/tern/converter"
	"github.com/denismitr/tern/database"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Migrator struct {
	gateway   database.Gateway
	converter converter.Converter
}

func NewMigrator(db *sqlx.DB, opts ...OptionFunc) (*Migrator, error) {
	m := new(Migrator)

	for _, oFunc := range opts {
		oFunc(m, db)
	}

	// Default converter implementation
	if m.converter == nil {
		localFsConverter, err := converter.NewLocalFSConverter(converter.DefaultMigrationsFolder)
		if err != nil {
			return nil, err
		}
		m.converter = localFsConverter
	}

	if m.gateway == nil {
		gateway, err := database.Create(db, database.DefaultMigrationsTable)
		if err != nil {
			return nil, err
		}
		m.gateway = gateway
	}

	return m, nil
}

func (m *Migrator) Up(ctx context.Context, cfs ...ActionConfigurator) ([]string, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	// fixme: make internal
	if err := m.gateway.Lock(ctx); err != nil {
		return nil, errors.Wrap(err, "migrations up lock failed")
	}

	defer func() {
		if err := m.gateway.Unlock(ctx); err != nil {
			panic(err) // fixme
		}
	}()

	migrations, err := m.converter.Convert(ctx, converter.Filter{})
	if err != nil {
		return nil, err
	}

	p := database.Plan{Steps: act.steps}
	migrated, err := m.gateway.Up(ctx, migrations, p)
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

	if err := m.gateway.Lock(ctx); err != nil {
		return errors.Wrap(err, "down migrations lock failed")
	}

	defer func() {
		if err := m.gateway.Unlock(ctx); err != nil {
			panic(err) // fixme
		}
	}()

	migrations, err := m.converter.Convert(ctx, converter.Filter{})
	if err != nil {
		return err
	}

	if err := m.gateway.Down(ctx, migrations, database.Plan{Steps: act.steps}); err != nil {
		return err
	}

	return nil
}
