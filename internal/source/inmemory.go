package source

import (
	"context"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
)

var ErrNoMigrations = errors.New("no migrations")

type InMemoryConverter struct {
	migrations migration.Migrations
}

func (c *InMemoryConverter) Select(ctx context.Context, f Filter) (migration.Migrations, error) {
	if c.migrations == nil {
		return nil, ErrNoMigrations
	}

	return c.migrations, nil
}

func NewInMemorySource(migrations ...*migration.Migration) *InMemoryConverter {
	return &InMemoryConverter{
		migrations: migrations,
	}
}
