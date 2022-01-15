package tern

import (
	"context"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/denismitr/tern/v3/internal/source"
	"github.com/denismitr/tern/v3/migration"
	"github.com/pkg/errors"
)

var ErrGatewayNotInitialized = errors.New("database gateway has not been initialized")
var ErrNothingToMigrateOrRollback = errors.New("nothing to migrate or rollback")

type CloserFunc func() error

type Migrator struct {
	lg        logger.Logger
	db        database.DB
	selector  source.Selector
	closerFns []CloserFunc
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

	if m.db == nil {
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

	m.db.SetLogger(m.lg)

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

// Migrate the migrations using Action configurator callbacks to customize
// the process of migration
func (m *Migrator) Migrate(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, error) {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.selector.Select(ctx, source.Filter{Versions: act.versions})
	if err != nil {
		m.lg.Error(err)
		return nil, err
	}

	if connErr := m.db.Connect(); connErr != nil {
		return nil, connErr
	}

	p := database.Plan{Steps: act.steps, Versions: act.versions}
	migrated, err := m.db.Migrate(ctx, migrations, p)
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return nil, ErrNothingToMigrateOrRollback
		}

		m.lg.Error(err)

		return migrated, err
	}

	return migrated, nil
}

// Rollback the migrations using Action configurator callbacks
// to customize the rollback process
func (m *Migrator) Rollback(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, error) {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.selector.Select(ctx, source.Filter{Versions: act.versions})
	if err != nil {
		m.lg.Error(err)
		return nil, errors.Wrap(err, "could not rollback migrations")
	}

	if connErr := m.db.Connect(); connErr != nil {
		return nil, connErr
	}

	rolledBack, err := m.db.Rollback(ctx, migrations, database.Plan{Steps: act.steps, Versions: act.versions})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return nil, ErrNothingToMigrateOrRollback
		}

		m.lg.Error(err)

		return rolledBack, errors.Wrap(err, "could not rollback migrations")
	}

	return rolledBack, nil
}

// Refresh first rollbacks the migrations and then migrates them again
// uses the Action configurator callbacks to customize the process
func (m *Migrator) Refresh(ctx context.Context, cfs ...ActionConfigurator) (migration.Migrations, migration.Migrations, error) {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	migrations, err := m.selector.Select(ctx, source.Filter{Versions: act.versions})
	if err != nil {
		m.lg.Error(err)
		return nil, nil, err
	}

	if connErr := m.db.Connect(); connErr != nil {
		return nil, nil, connErr
	}

	rolledBack, migrated, err := m.db.Refresh(ctx, migrations, database.Plan{Steps: act.steps, Versions: act.versions})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
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
func (m *Migrator) dbGateway() database.DB {
	if err := m.db.Connect(); err != nil {
		panic(err)
	}

	return m.db
}
