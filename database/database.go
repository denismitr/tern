package database

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/migration"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"io"
)

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")

type ServiceGateway interface {
	io.Closer

	WriteVersions(ctx context.Context, migrations migration.Migrations) error
	ReadVersions(ctx context.Context) ([]string, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type Gateway interface {
	io.Closer

	Up(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Down(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
}

func CreateGateway(db *sqlx.DB, migrationsTable string) (Gateway, error) {
	driver := db.DriverName()

	switch driver {
	case "mysql":
		return NewMysqlGateway(db, migrationsTable, MysqlDefaultLockKey, MysqlDefaultLockSeconds)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

func CreateServiceGateway(db *sqlx.DB, migrationsTable string) (ServiceGateway, error) {
	driver := db.DriverName()

	switch driver {
	case "mysql":
		return NewMysqlGateway(db, migrationsTable, MysqlDefaultLockKey, MysqlDefaultLockSeconds)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

func up(ctx context.Context, tx *sql.Tx, migration migration.Migration, insertQuery string) error {
	if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, insertQuery, migration.Version, migration.Name); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return err
		}
	}

	return nil
}

func down(ctx context.Context, tx *sql.Tx, migration migration.Migration, removeVersionQuery string) error {
	if _, err := tx.ExecContext(ctx, migration.Down); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, removeVersionQuery, migration.Version); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return err
		}
	}

	return nil
}
