package database

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

func Create(db *sqlx.DB, migrationsTable string) (Gateway, error) {
	driver := db.DriverName()

	switch driver {
	case "mysql":
		return NewMysqlGateway(db, migrationsTable, MysqlDefaultLockKey, MysqlDefaultLockSeconds)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}
