package database

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/migration"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"io"
)

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")
var ErrNothingToMigrate = errors.New("nothing to migrate")
var ErrMigrationVersionNotSpecified = errors.New("migration version not specified")

const (
	operationRollback = "rollback"
	operationMigrate  = "migrate"
	operationRefresh  = "refresh"
)

type CommonOptions struct {
	MigrationsTable string
}

type migrateFunc func(ctx context.Context, ex ctxExecutor, migration *migration.Migration, insertQuery string) error
type rollbackFunc func(ctx context.Context, ex ctxExecutor, migration *migration.Migration, removeVersionQuery string) error

type handlers struct {
	migrate         migrateFunc
	rollback        rollbackFunc
}

type ctxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type Plan struct {
	Steps int
}

type ServiceGateway interface {
	io.Closer

	WriteVersions(ctx context.Context, migrations migration.Migrations) error
	ReadVersions(ctx context.Context) ([]migration.Version, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type Gateway interface {
	io.Closer

	Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Refresh(ctx context.Context, migrations migration.Migrations, plan Plan) (migration.Migrations, migration.Migrations, error)
}

// CreateServiceGateway - creates gateway with service functionality
// such as listing all tables in database and reading migration versions
func CreateServiceGateway(driver string, db *sql.DB, migrationsTable string) (ServiceGateway, error) {
	connector := MakeRetryingConnector(NewDefaultConnectOptions())

	switch driver {
	case "mysql":
		return NewMySQLGateway(db, connector,
			&MySQLOptions{
				CommonOptions: CommonOptions{
					MigrationsTable: migrationsTable,
				},
				LockFor: MysqlDefaultLockSeconds,
				LockKey: MysqlDefaultLockKey,
			})
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

func migrate(ctx context.Context, tx ctxExecutor, migration *migration.Migration, insertQuery string) error {
	if migration.Version.Timestamp == "" {
		return ErrMigrationVersionNotSpecified
	}

	if _, err := tx.ExecContext(ctx, migration.MigrateScripts()); err != nil {
		return errors.Wrapf(err, "could not run migration [%s]", migration.Key)
	}

	if _, err := tx.ExecContext(ctx, insertQuery, migration.Version.Timestamp, migration.Name); err != nil {
		return errors.Wrapf(
			err,
			"could not insert migration version [%s]",
			migration.Version.Timestamp,
		)
	}

	return nil
}

func rollback(ctx context.Context, ex ctxExecutor, migration *migration.Migration, removeVersionQuery string) error {
	if migration.Version.Timestamp == "" {
		return ErrMigrationVersionNotSpecified
	}

	if _, err := ex.ExecContext(ctx, migration.RollbackScripts()); err != nil {
		return errors.Wrapf(err, "could not rollback migration [%s]", migration.Key)
	}

	if _, err := ex.ExecContext(ctx, removeVersionQuery, migration.Version.Timestamp); err != nil {
		return errors.Wrapf(
			err,
			"could not remove migration version [%s]",
			migration.Version.Timestamp,
		)
	}

	return nil
}
