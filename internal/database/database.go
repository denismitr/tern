package database

import (
	"context"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/pkg/errors"
)

var ErrNoChangesRequired = errors.New("no changes to the database required")
var ErrMigrationVersionNotSpecified = errors.New("migration version not specified")
var ErrMigrationIsMalformed = errors.New("migration is malformed")

const (
	DefaultMigrationsTable = "migrations"

	OperationRollback = "rollback"
	OperationMigrate  = "migrate"
	OperationRefresh  = "refresh"
)

const (
	ASC  = "ASC"
	DESC = "DESC"
)

type ReadVersionsFilter struct {
	Limit    int
	Sort     string
	MinBatch *uint
	MaxBatch *uint
}

type CommonOptions struct {
	MigrationsTable string
}

type Plan struct {
	Steps    int
	Versions []Version
}

type versionController interface {
	WriteVersions(ctx context.Context, migrations Migrations) error
	ReadVersions(ctx context.Context) ([]Version, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type Effector interface {
	SetLogger(logger.Logger)
	Migrate(ctx context.Context, migrations Migrations, p Plan) (Migrations, error)
	Rollback(ctx context.Context, migrations Migrations, p Plan) (Migrations, error)
	Refresh(ctx context.Context, migrations Migrations, p Plan) (Migrations, Migrations, error)

	versionController
}

type ConnCloser func() error

func ScheduleForRollback(
	migrations Migrations,
	migratedVersions []Version,
	p Plan,
) Migrations {
	var scheduled Migrations

	for i := len(migrations) - 1; i >= 0; i-- {
		if len(p.Versions) > 0 && !InVersions(migrations[i].Version, p.Versions) {
			continue
		}

		if InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			scheduled = append(scheduled, migrations[i])
		}
	}

	return scheduled
}

func ScheduleForMigration(
	migrations Migrations,
	migratedVersions []Version,
	p Plan,
) Migrations {
	var scheduled Migrations

	for i := range migrations {
		if !InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			if len(p.Versions) == 0 || InVersions(migrations[i].Version, p.Versions) {
				scheduled = append(scheduled, migrations[i])
			}
		}
	}

	return scheduled
}

func ScheduleForRefresh(
	migrations Migrations,
	migratedVersions []Version,
	p Plan,
) Migrations {
	var scheduled Migrations
	for i := len(migrations) - 1; i >= 0; i-- {
		if len(p.Versions) > 0 && !InVersions(migrations[i].Version, p.Versions) {
			continue
		}

		if InVersions(migrations[i].Version, migratedVersions) {
			if p.Steps != 0 && len(scheduled) >= p.Steps {
				break
			}

			scheduled = append(scheduled, migrations[i])
		}
	}
	return scheduled
}
