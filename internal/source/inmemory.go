package source

import (
	"context"
	"github.com/denismitr/tern/v3/migration"
	"github.com/pkg/errors"
)

var ErrNoMigrations = errors.New("no migrations")

type InMemorySource struct {
	migrations migration.Migrations
}

func (c *InMemorySource) Select(ctx context.Context, f Filter) (migration.Migrations, error) {
	if c.migrations == nil {
		return nil, ErrNoMigrations
	}

	return c.migrations, nil
}

func NewInMemorySource(factories ...migration.Factory) (*InMemorySource, error) {
	m, err := migration.NewMigrations(factories...)
	if err != nil {
		return nil, err
	}

	return &InMemorySource{
		migrations: m,
	}, nil
}
