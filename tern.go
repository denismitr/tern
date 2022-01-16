package tern

import (
	"context"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/denismitr/tern/v3/internal/source"
	"github.com/pkg/errors"
)

var ErrGatewayNotInitialized = errors.New("database gateway has not been initialized")
var ErrNothingToMigrateOrRollback = errors.New("nothing to migrate or rollback")

type CloserFunc func() error

type Migrator struct {
	lg        logger.Logger
	driver    *Driver
	selector  source.Selector
	closerFns []CloserFunc
}

// NewMigrator creates a migrator using the *tern.Driver and option callbacks
// to customize the newly created configurator, when no custom options
// are required a number of defaults will be applied
func NewMigrator(driver *Driver, opts ...OptionFunc) (*Migrator, error) {
	m := new(Migrator)
	m.lg = &logger.NullLogger{}

	for _, oFunc := range opts {
		if err := oFunc(m); err != nil {
			return nil, err
		}
	}

	if m.driver == nil {
		return nil, ErrGatewayNotInitialized // TODO: rename
	}

	return m, nil
}

// Migrate the given migrations using Action configurator callbacks to customize
// the process of migration
func (m *Migrator) Migrate(ctx context.Context, migrations Migrations, cfs ...ActionConfigurator) error {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	databaseMigrations := make(database.Migrations, len(migrations))
	for i := range migrations {
		databaseMigrations[i] = database.Migration{
			Migrate: migrations[i].Migrate,
			Rollback: migrations[i].Rollback,
			Version: database.Version{
				Name: migrations[i].Version.Name,
				Batch: database.Batch(migrations[i].Version.Batch),
				Order: database.Order(migrations[i].Version.Order),
			},
		}
	}

	p := database.Plan{Steps: act.steps, Versions: act.versions}
	_, err := m.driver.effector.Migrate(ctx, databaseMigrations, p)
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			m.lg.Debugf("Nothing to migrate")
			return nil
		}

		m.lg.Error(err)

		return err
	}

	return nil
}

// Rollback the migrations using Action configurator callbacks
// to customize the rollback process
func (m *Migrator) Rollback(ctx context.Context, migrations Migrations, cfs ...ActionConfigurator) error {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	databaseMigrations := make(database.Migrations, len(migrations))
	for i := range migrations {
		databaseMigrations[i] = database.Migration{
			Migrate: migrations[i].Migrate,
			Rollback: migrations[i].Rollback,
			Version: database.Version{
				Name: migrations[i].Version.Name,
				Batch: database.Batch(migrations[i].Version.Batch),
				Order: database.Order(migrations[i].Version.Order),
			},
		}
	}

	_, err := m.driver.effector.Rollback(ctx, databaseMigrations, database.Plan{Steps: act.steps, Versions: act.versions})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			m.lg.Debugf("Nothing to rollback")
			return nil
		}

		m.lg.Error(err)

		return errors.Wrap(err, "could not rollback migrations")
	}

	return nil
}

// Refresh first rollbacks the migrations and then migrates them again
// uses the Action configurator callbacks to customize the process
func (m *Migrator) Refresh(
	ctx context.Context,
	migrations Migrations,
	cfs ...ActionConfigurator,
) error {
	act := new(Action)
	for _, f := range cfs {
		f(act)
	}

	databaseMigrations := make(database.Migrations, len(migrations))
	for i := range migrations {
		databaseMigrations[i] = database.Migration{
			Migrate: migrations[i].Migrate,
			Rollback: migrations[i].Rollback,
			Version: database.Version{
				Name: migrations[i].Version.Name,
				Batch: database.Batch(migrations[i].Version.Batch),
				Order: database.Order(migrations[i].Version.Order),
			},
		}
	}

	_, _, err := m.driver.effector.Refresh(ctx, databaseMigrations, database.Plan{Steps: act.steps, Versions: act.versions})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			m.lg.Debugf("Nothing to migrate or rollback")
			return nil
		}

		m.lg.Error(err)
		return err
	}

	return nil
}
