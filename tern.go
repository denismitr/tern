package tern

import (
	"context"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/internal/logger"
	"github.com/denismitr/tern/internal/source"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
)

var ErrGatewayNotInitialized = errors.New("database gateway has not been initialized")

type CloserFunc func() error

type Migrator struct {
	lg             logger.Logger
	gateway        database.Gateway
	selector       source.Selector
	connectOptions *database.ConnectOptions
}

// NewMigrator creates a migrator using the sql.DB and option callbacks
// to customize the newly created configurator, when no custom options
// are required a number of defaults will be applied
func NewMigrator(opts ...OptionFunc) (*Migrator, CloserFunc, error) {
	m := new(Migrator)
	m.lg = &logger.NullLogger{}

	for _, oFunc := range opts {
		if err := oFunc(m); err != nil {
			return nil, nil, err
		}
	}

	if m.gateway == nil {
		return nil, nil, ErrGatewayNotInitialized
	}

	// Default selector implementation
	if m.selector == nil {
		localFsConverter, err := source.NewLocalFSSource(
			source.DefaultMigrationsFolder,
			m.lg,
			migration.TimestampFormat,
		)

		if gatewayErr := m.gateway.Close(); gatewayErr != nil {
			return nil, nil, errors.Wrap(err, gatewayErr.Error())
		}

		if err != nil {
			return nil, nil, err
		}

		m.selector = localFsConverter
	}

	m.gateway.SetLogger(m.lg)

	return m, m.close, nil
}

// Migrate the migrations using action configurator callbacks to customize
// the process of migration
func (m *Migrator) Migrate(ctx context.Context, cfs ...ActionConfigurator) ([]string, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.selector.Select(ctx, source.Filter{Keys: act.keys})
	if err != nil {
		m.lg.Error(err)
		return nil, err
	}

	p := database.Plan{Steps: act.steps}
	migrated, err := m.gateway.Migrate(ctx, migrations, p)
	if err != nil {
		if !errors.Is(err, database.ErrNothingToMigrate) {
			m.lg.Error(err)
		}

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

	migrations, err := m.selector.Select(ctx, source.Filter{Keys: act.keys})
	if err != nil {
		m.lg.Error(err)
		return nil, errors.Wrap(err, "could not rollback migrations")
	}

	executed, err := m.gateway.Rollback(ctx, migrations, database.Plan{Steps: act.steps})
	if err != nil {
		m.lg.Error(err)
		return nil, errors.Wrap(err, "could not rollback migrations")
	}

	return executed, nil
}

// Close the migrator
func (m *Migrator) close() error {
	if m.gateway == nil {
		return ErrGatewayNotInitialized
	}

	if err := m.gateway.Close(); err != nil {
		m.lg.Error(err)
	}

	return nil
}

// Refresh first rollbacks the migrations and then migrates them again
// uses the action configurator callbacks to customize the process
func (m *Migrator) Refresh(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, migration.Migrations, error) {
	act := new(action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.selector.Select(ctx, source.Filter{Keys: act.keys})
	if err != nil {
		m.lg.Error(err)
		return nil, nil, err
	}

	rolledBack, migrated, err := m.gateway.Refresh(ctx, migrations, database.Plan{Steps: act.steps})
	if err != nil {
		m.lg.Error(err)
		return nil, nil, err
	}

	return rolledBack, migrated, nil
}

// Source - returns migrator selector if it implements the full source.Source interface
func (m *Migrator) Source() source.Source {
	if s, ok := m.selector.(source.Source); ok {
		return s
	}

	return nil
}
