package tern

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/source"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
)

var ErrGatewayNotInitialized = errors.New("database gateway has not been initialized")

type Migrator struct {
	gateway   database.Gateway
	converter source.Selector
}

// NewMigrator creates a migrator using the sql.DB and option callbacks
// to customize the newly created configurator, when no custom options
// are required a number of defaults will be applied
func NewMigrator(driver string, db *sql.DB, opts ...OptionFunc) (*Migrator, error) {
	m := new(Migrator)

	for _, oFunc := range opts {
		if err := oFunc(m, driver, db); err != nil {
			return nil, err
		}
	}

	// Default converter implementation
	if m.converter == nil {
		localFsConverter, err := source.NewLocalFSSource(source.DefaultMigrationsFolder)
		if err != nil {
			return nil, err
		}
		m.converter = localFsConverter
	}

	// Default gateway implementation
	if m.gateway == nil {
		gateway, err := database.CreateGateway(driver, db, database.DefaultMigrationsTable)
		if err != nil {
			return nil, err
		}
		m.gateway = gateway
	}

	return m, nil
}

// Migrate the migrations using action configurator callbacks to customize
// the process of migration
func (m *Migrator) Migrate(ctx context.Context, cfs ...ActionConfigurator) ([]string, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.converter.Select(ctx, source.Filter{Keys: act.keys})
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

// Rollback the migrations using action configurator callbacks
// to customize the rollback process
func (m *Migrator) Rollback(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.converter.Select(ctx, source.Filter{})
	if err != nil {
		return nil, err
	}

	executed, err := m.gateway.Down(ctx, migrations, database.Plan{Steps: act.steps});
	if err != nil {
		return nil, err
	}

	return executed, nil
}

// Close the migrator
func (m *Migrator) Close() error {
	if m.gateway == nil {
		return ErrGatewayNotInitialized
	}

	return m.gateway.Close()
}

// Refresh first rollbacks the migrations and then migrates them again
// uses the action configurator callbacks to customize the process
func (m *Migrator) Refresh(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, migration.Migrations, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.converter.Select(ctx, source.Filter{})
	if err != nil {
		return nil, nil, err
	}

	rolledBack, migrated, err := m.gateway.Refresh(ctx, migrations, database.Plan{});
	if err != nil {
		return nil, nil, err
	}

	return rolledBack, migrated, nil
}