package converter

import (
	"context"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
)

var ErrNoMigrations = errors.New("no migrations")

type InMemoryConverter struct {
	migrations migration.Migrations
}

func (c *InMemoryConverter) Convert(ctx context.Context, f Filter) (migration.Migrations, error) {
	if c.migrations == nil {
		return nil, ErrNoMigrations
	}

	return c.migrations, nil
}

func NewInMemoryConverter(migrations ...*migration.Migration) *InMemoryConverter  {
	return &InMemoryConverter{
		migrations: migrations,
	}
}
