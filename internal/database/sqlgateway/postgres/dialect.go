package postgres

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/pkg/errors"
)

type Dialect struct {
	migrationsTable, charset string
}

var _ sqlgateway.Dialect = (*Dialect)(nil)

func NewDialect(migrationsTable, charset string) *Dialect {
	return &Dialect{migrationsTable: migrationsTable, charset: charset}
}

func (d Dialect) InitQuery() string {
	const createSQL = `
		CREATE TABLE IF NOT EXISTS %s (
			order bigint PRIMARY KEY,
			batch bigint,
			name VARCHAR(120),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, d.migrationsTable, d.charset)
}

func (d Dialect) InsertQuery(m database.Migration) (string, []interface{}, error) {
	// TODO: optimize with bytes.Buffer

	const insertSQL = `
		INSERT INTO %s (order, batch, name, migrated_at) VALUES ($1, $2, $3, $4);	
	`

	if m.Version.Order <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version order must be greater than 0")
	}

	if m.Version.Batch <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version batch must be greater than 0")
	}

	if m.Version.Name == "" {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version name must be specified")
	}

	if m.Version.MigratedAt.IsZero() {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version migrated_at must be specified")
	}

	args := []interface{}{
		m.Version.Order,
		m.Version.Batch,
		m.Version.Name,
		m.Version.MigratedAt,
	}

	return fmt.Sprintf(insertSQL, d.migrationsTable), args, nil
}

func (d Dialect) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT order, batch, name, migrated_at FROM %s"

	if f.MinBatch != nil {
		if *f.MinBatch <= 1 {
			return "", errors.Wrapf(database.ErrMigrationIsMalformed, "version min batch should be greater than 1")
		}

		readSQL += fmt.Sprintf(" WHERE batch >= %d", f.MinBatch)
		if f.MaxBatch != nil {
			if *f.MaxBatch <= 1 || *f.MaxBatch < *f.MinBatch {
				return "", errors.Wrapf(
					database.ErrMigrationIsMalformed,
					"version max batch should be greater than 1 and greater than min batch if specified",
				)
			}

			readSQL += fmt.Sprintf(" AND batch <= %d", f.MaxBatch)
		}
	}

	if f.MaxBatch != nil {
		if *f.MaxBatch <= 1 {
			return "", errors.Wrapf(database.ErrMigrationIsMalformed, "max batch should be greater than 1")
		}
	}

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY version DESC"
	} else {
		readSQL += " ORDER BY version ASC"
	}

	return fmt.Sprintf(readSQL, d.migrationsTable), nil
}

func (d Dialect) RemoveQuery(m database.Migration) (string, []interface{}, error) {
	if m.Version.Order <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version order must be greater than 0")
	}
	const removeSQL = "DELETE FROM %s WHERE order = $1;"
	return fmt.Sprintf(removeSQL, d.migrationsTable), []interface{}{m.Version.Order}, nil
}

func (d Dialect) DropQuery() string {
	const dropSQL = `
		DROP TABLE IF EXISTS %s;
	`
	return fmt.Sprintf(dropSQL, d.migrationsTable)
}

func (d Dialect) ShowTablesQuery() string {
	return "SELECT schemaname as table_name FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
}
