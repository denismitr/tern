package database

import (
	"context"
	"github.com/denismitr/tern/v2/internal/logger"
	"github.com/denismitr/tern/v2/migration"
	"github.com/pkg/errors"
)

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")
var ErrNothingToMigrate = errors.New("nothing to migrate")
var ErrNothingToRollback = errors.New("nothing to rollback")
var ErrNothingToMigrateOrRollback = errors.New("nothing to migrate or rollback")
var ErrMigrationVersionNotSpecified = errors.New("migration version not specified")

var MigratedAtColumn = "migrated_at"

const (
	DefaultMigrationsTable = "migrations"

	OperationRollback = "rollback"
	OperationMigrate  = "migrate"
	OperationRefresh  = "refresh"
)

type CommonOptions struct {
	MigrationsTable   string
	MigratedAtColumn  string
}

type Plan struct {
	Steps int
}

type versionController interface {
	WriteVersions(ctx context.Context, migrations migration.Migrations) error
	ReadVersions(ctx context.Context) ([]migration.Version, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type Gateway interface {
	SetLogger(logger.Logger)
	Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Refresh(ctx context.Context, migrations migration.Migrations, plan Plan) (migration.Migrations, migration.Migrations, error)
	Connect() error

	versionController
}

type ConnCloser func() error
