package tern

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const mysqlCreateMigrationsSchema = `
CREATE TABLE IF NOT EXISTS %s (
	version VARCHAR(13) PRIMARY KEY,
	name VARCHAR(255),
	created_at TIMESTAMP default CURRENT_TIMESTAMP
) ENGINE=INNODB;	
`

const mysqlDropMigrationsSchema = `DROP TABLE IF EXISTS %s;`

type gateway interface {
	up(ctx context.Context, migrations Migrations) error
	down(ctx context.Context, migrations Migrations) error
	readVersions(context.Context) ([]string, error)
	dropMigrationsTable(context.Context) error
}

type mysqlGateway struct {
	db *sqlx.DB
	migrationsTable string
}

func (e *mysqlGateway) down(ctx context.Context, migrations Migrations) error {
	tx, err := e.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	versions, err := readVersions(tx, e.migrationsTable);
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	deleteVersionQuery := e.createDeleteVersionQuery()

	for i := range migrations {
		if inVersions(migrations[i].Version, versions) {
			if _, err := tx.ExecContext(ctx, migrations[i].Down); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return errors.Wrap(err, rollbackErr.Error())
				}
			}

			if _, err := tx.ExecContext(ctx, deleteVersionQuery, migrations[i].Version); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return errors.Wrap(err, rollbackErr.Error())
				}
			}
		}
	}

	return tx.Commit()
}

func (e *mysqlGateway) readVersions(ctx context.Context) ([]string, error) {
	tx, err := e.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := readVersions(tx, e.migrationsTable)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return result, err
	}

	return result, nil
}

func newMysqlGateway(db *sqlx.DB, tableName string) (*mysqlGateway, error) {
	return &mysqlGateway{db: db, migrationsTable: tableName}, nil
}

func (e *mysqlGateway) up(ctx context.Context, migrations Migrations) error {
	if err := e.createMigrationsTable(ctx); err != nil {
		return err
	}

	tx, err := e.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	versions, err := readVersions(tx, e.migrationsTable);
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	insertVersionQuery := e.createInsertVersionQuery()

	for i := range migrations {
		if !inVersions(migrations[i].Version, versions) {
			if _, err := tx.ExecContext(ctx, migrations[i].Up); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return errors.Wrap(err, rollbackErr.Error())
				}
			}

			if _, err := tx.ExecContext(ctx, insertVersionQuery, migrations[i].Version, migrations[i].Name); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return errors.Wrap(err, rollbackErr.Error())
				}
			}
		}
	}

	return tx.Commit()
}

func (e *mysqlGateway) createInsertVersionQuery() string {
	return fmt.Sprintf("INSERT INTO %s (version, name) VALUE (?, ?)", e.migrationsTable)
}

func (e *mysqlGateway) createDeleteVersionQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version = ?;", e.migrationsTable)
}

func (e *mysqlGateway) createMigrationsTable(ctx context.Context) error {
	if _, err := e.db.ExecContext(ctx, fmt.Sprintf(mysqlCreateMigrationsSchema, e.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (e *mysqlGateway) dropMigrationsTable(ctx context.Context) error {
	if _, err := e.db.ExecContext(ctx, fmt.Sprintf(mysqlDropMigrationsSchema, e.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func readVersions(tx *sqlx.Tx, migrationsTable string) ([]string, error) {
	rows, err := tx.Query(fmt.Sprintf("SELECT version FROM %s", migrationsTable))
	if err != nil {
		return nil, err
	}

	var result []string

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, version)
	}

	return result, nil
}

func inVersions(version string, versions []string) bool {
	for _, v := range versions {
		if v == version {
			return true
		}
	}

	return false
}