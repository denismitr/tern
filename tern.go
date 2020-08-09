package tern

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")

type Migrator struct {
	ex   executor
	conv converter
}

func NewMigrator(db *sqlx.DB, opts ...OptionFunc) (*Migrator, error) {
	ex, err := createExecutor(db, "migrations")
	if err != nil {
		return nil, err
	}

	m := &Migrator{
		ex: ex,
		conv: localFSConverter{folder: "./migrations"},
	}

	for _, oFunc := range opts {
		oFunc(m)
	}

	return m, nil
}

func (m *Migrator) Up(ctx context.Context) error {
	migrations, err := m.conv.ReadAll(ctx)
	if err != nil {
		return err
	}

	if err := m.ex.up(ctx, migrations); err != nil {
		return err
	}

	return nil
}

func createExecutor(db *sqlx.DB, tableName string) (executor, error) {
	driver := db.DriverName()

	switch driver {
	case "mysql":
		return newMysqlExecutor(db, tableName)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}
