package database

import (
	"context"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/denismitr/tern/v3/migration"
	"github.com/pkg/errors"
)

var ErrNoChangesRequired = errors.New("no changes to the database required")
var ErrMigrationVersionNotSpecified = errors.New("migration version not specified")

var MigratedAtColumn = "migrated_at"

const (
	DefaultMigrationsTable = "migrations"

	OperationRollback = "rollback"
	OperationMigrate  = "migrate"
	OperationRefresh  = "refresh"
)

type CommonOptions struct {
	MigrationsTable  string
	MigratedAtColumn string
}

type Plan struct {
	Steps    int
	Versions []migration.Version
}

type versionController interface {
	WriteVersions(ctx context.Context, migrations migration.Migrations) error
	ReadVersions(ctx context.Context) ([]migration.Version, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type DB interface {
	SetLogger(logger.Logger)
	Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Refresh(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, migration.Migrations, error)
	Connect() error

	versionController
}

type ConnCloser func() error

func ScheduleForRollback(
	migrations migration.Migrations,
	migratedVersions []migration.Version,
	p Plan,
) migration.Migrations {
	var scheduled migration.Migrations

	for i := len(migrations) - 1; i >= 0; i-- {
		if len(p.Versions) > 0 && !migration.InVersions(migrations[i].Version, p.Versions) {
			continue
		}

		if migration.InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			scheduled = append(scheduled, migrations[i])
		}
	}

	return scheduled
}

func ScheduleForMigration(
	migrations migration.Migrations,
	migratedVersions []migration.Version,
	p Plan,
) migration.Migrations {
	var scheduled migration.Migrations

	for i := range migrations {
		if !migration.InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			if len(p.Versions) == 0 || migration.InVersions(migrations[i].Version, p.Versions) {
				scheduled = append(scheduled, migrations[i])
			}
		}
	}

	return scheduled
}

func ScheduleForRefresh(
	migrations migration.Migrations,
	migratedVersions []migration.Version,
	p Plan,
) migration.Migrations {
	var scheduled migration.Migrations
	for i := len(migrations) - 1; i >= 0; i-- {
		if len(p.Versions) > 0 && !migration.InVersions(migrations[i].Version, p.Versions) {
			continue
		}

		if migration.InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			scheduled = append(scheduled, migrations[i])
		}
	}
	return scheduled
}
