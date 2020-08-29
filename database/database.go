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

const (
	operationRollback = "rollback"
	operationMigrate  = "migrate"
	operationRefresh  = "refresh"
)

type migrateFunc func(ctx context.Context, tx *sql.Tx, migration *migration.Migration, insertQuery string) error
type rollbackFunc func(ctx context.Context, tx *sql.Tx, migration *migration.Migration, removeVersionQuery string) error

type handlers struct {
	migrate         migrateFunc
	rollback        rollbackFunc
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

// CreateGateway for basic migration functionality
func CreateGateway(driver string, db *sql.DB, migrationsTable string, connectOptions *ConnectOptions) (Gateway, error) {
	connector := MakeRetryingConnector(connectOptions)

	switch driver {
	case "mysql":
		return NewMySQLGateway(db, connector, migrationsTable, MysqlDefaultLockKey, MysqlDefaultLockSeconds)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

// CreateServiceGateway - creates gateway with service functionality
// such as listing all tables in database and reading migration versions
func CreateServiceGateway(driver string, db *sql.DB, migrationsTable string) (ServiceGateway, error) {
	connector := MakeRetryingConnector(NewDefaultConnectOptions())

	switch driver {
	case "mysql":
		return NewMySQLGateway(db, connector, migrationsTable, MysqlDefaultLockKey, MysqlDefaultLockSeconds)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

func migrate(ctx context.Context, tx *sql.Tx, migration *migration.Migration, insertQuery string) error {
	if _, err := tx.ExecContext(ctx, migration.MigrateScripts()); err != nil {
		return errors.Wrapf(err, "could not execute migrate migration [%s]", migration.Key)
	}

	if _, err := tx.ExecContext(ctx, insertQuery, migration.Version.Timestamp, migration.Name); err != nil {
		return errors.Wrapf(
			err,
			"could not insert migration version with key [%s] with query %s",
			migration.Key,
			insertQuery,
		)
	}

	return nil
}

func rollback(ctx context.Context, tx *sql.Tx, migration *migration.Migration, removeVersionQuery string) error {
	if _, err := tx.ExecContext(ctx, migration.RollbackScripts()); err != nil {
		return errors.Wrapf(err, "could not execute rollback migration %s", migration.Key)
	}

	if _, err := tx.ExecContext(ctx, removeVersionQuery, migration.Version.Timestamp); err != nil {
		return errors.Wrapf(
			err,
			"could not remove migration version [%s] with query [%s]",
			migration.Version.Timestamp,
			removeVersionQuery,
		)
	}

	return nil
}
