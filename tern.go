package tern

import (
	"context"
	"github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/internal/database/sqlgateway"
	"github.com/denismitr/tern/v2/internal/logger"
	"github.com/denismitr/tern/v2/internal/source"
	"github.com/denismitr/tern/v2/migration"
	"github.com/pkg/errors"
)

var ErrGatewayNotInitialized = errors.New("database gateway has not been initialized")

type CloserFunc func() error

var ErrNothingToMigrate = errors.New("nothing to migrate")
var ErrNothingToRollback = errors.New("nothing to rollback")
var ErrNothingToMigrateOrRollback = errors.New("nothing to migrate or rollback")

type Migrator struct {
	lg             logger.Logger
	gateway        database.Gateway
	selector       source.Selector
	connectOptions *sqlgateway.ConnectOptions
	closerFns      []CloserFunc
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

		if err != nil {
			return nil, nil, err
		}

		m.selector = localFsConverter
	}

	m.gateway.SetLogger(m.lg)

	closer := func() error {
		for _, fn := range m.closerFns {
			if err := fn(); err != nil {
				return err // fixme
			}
		}

		return nil
	}

	return m, closer, nil
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

	if err := m.gateway.Connect(); err != nil {
		return nil, err
	}

	p := database.Plan{Steps: act.steps}
	migrated, err := m.gateway.Migrate(ctx, migrations, p)
	if err != nil {
		if !errors.Is(err, database.ErrNothingToMigrate) {
			return nil, ErrNothingToMigrate
		}

		m.lg.Error(err)

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

	if err := m.gateway.Connect(); err != nil {
		return nil, err
	}

	executed, err := m.gateway.Rollback(ctx, migrations, database.Plan{Steps: act.steps})
	if err != nil {
		if errors.Is(err, database.ErrNothingToRollback) {
			return nil, ErrNothingToRollback
		}

		m.lg.Error(err)
		return nil, errors.Wrap(err, "could not rollback migrations")
	}

	return executed, nil
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

	if err := m.gateway.Connect(); err != nil {
		return nil, nil, err
	}

	rolledBack, migrated, err := m.gateway.Refresh(ctx, migrations, database.Plan{Steps: act.steps})
	if err != nil {
		if errors.Is(err, database.ErrNothingToMigrateOrRollback) {
			return nil, nil, ErrNothingToMigrateOrRollback
		}

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

// dbGateway - return database gateway for internal testing usage
func (m *Migrator) dbGateway() database.Gateway {
	if err := m.gateway.Connect(); err != nil {
		panic(err)
	}

	return m.gateway
}
