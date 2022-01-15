package postgres

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/denismitr/tern/v3/migration"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type StateManager struct {
	migrationsTable, migratedAtColumn, charset string
}

var _ sqlgateway.StateManager = (*StateManager)(nil)

func NewPostgresStateManager(migrationsTable, migratedAtColumn, charset string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn, charset: charset}
}

func (s StateManager) InitQuery() string {
	const createSQL = `
		CREATE TABLE IF NOT EXISTS %s (
			version bigint PRIMARY KEY,
			batch bigint,
			name VARCHAR(120),
			%s TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, s.migrationsTable, s.migratedAtColumn, s.charset)
}

func (s StateManager) InsertQuery(m *migration.Migration) (string, []interface{}, error) {
	// TODO: optimize with bytes.Buffer

	const insertSQL = `
		INSERT INTO %s (version, batch, name) VALUES ($1, $2, $3);	
	`

	if m.Version <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version must be greater than 0")
	}

	if m.Batch <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "batch must be greater than 0")
	}

	if m.Name == "" {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "name must be specified")
	}

	return fmt.Sprintf(insertSQL, s.migrationsTable), []interface{}{m.Version, m.Version, m.Name}, nil
}

func (s StateManager) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT version, batch %s FROM %s"

	if f.MinBatch != nil {
		if *f.MinBatch <= 1 {
			return "", errors.Wrapf(database.ErrMigrationIsMalformed, "min batch should be greater than 1")
		}

		readSQL += fmt.Sprintf(" WHERE batch >= %d", f.MinBatch)
		if f.MaxBatch != nil {
			if *f.MaxBatch <= 1 || *f.MaxBatch < *f.MinBatch {
				return "", errors.Wrapf(
					database.ErrMigrationIsMalformed,
					"max batch should be greater than 1 and greater than min batch if specified",
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

	return fmt.Sprintf(readSQL, s.migratedAtColumn, s.migrationsTable), nil
}

func (s StateManager) RemoveQuery(m *migration.Migration) (string, []interface{}, error) {
	if m.Version <= 1 {
		return "", nil, errors.Wrapf(database.ErrMigrationIsMalformed, "version must be greater than 0")
	}
	const removeSQL = "DELETE FROM %s WHERE `version` = $1;"
	return fmt.Sprintf(removeSQL, s.migrationsTable), []interface{}{m.Version}, nil
}

func (s StateManager) DropQuery() string {
	const dropSQL = `
		DROP TABLE IF EXISTS %s;
	`
	return fmt.Sprintf(dropSQL, s.migrationsTable)
}

func (s StateManager) ShowTablesQuery() string {
	return "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
}
