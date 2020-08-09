package tern

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type mysqlExecutor struct {
	db *sqlx.DB
	migrationsTable string
}

const mysqlMigrationsSchema = `
CREATE TABLE IF NOT EXISTS %s (
	version VARCHAR(13) PRIMARY KEY,
	name VARCHAR(255),
	created_at TIMESTAMP default CURRENT_TIMESTAMP
) ENGINE=INNODB;	
`

func newMysqlExecutor(db *sqlx.DB, tableName string) (*mysqlExecutor, error) {
	return &mysqlExecutor{db: db, migrationsTable: tableName}, nil
}

func (e *mysqlExecutor) Up(ctx context.Context) error {
	if err := e.createServiceTable(ctx); err != nil {
		return err
	}

	return nil
}

func (e *mysqlExecutor) createServiceTable(ctx context.Context) error {
	if _, err := e.db.ExecContext(ctx, fmt.Sprintf(mysqlMigrationsSchema, e.migrationsTable)); err != nil {
		return err
	}

	return nil
}

type executor interface {
	Up(ctx context.Context) error
	createServiceTable(ctx context.Context) error
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