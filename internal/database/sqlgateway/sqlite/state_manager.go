package sqlite

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/denismitr/tern/v3/migration"
)

type StateManager struct {
	migrationsTable string
}

type Options struct {
	database.CommonOptions
}

func NewStateManager(migrationsTable string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable}
}

var _ sqlgateway.StateManager = (*StateManager)(nil)

func (s StateManager) InitQuery() string {
	const sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			order BIGINT PRIMARY KEY,
            batch BIGINT,
			name VARCHAR(255),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`

	return fmt.Sprintf(sqliteCreateMigrationsSchema, s.migrationsTable)
}

func (s StateManager) InsertQuery(m *migration.Migration) (string, []interface{}, error) {
	const sqliteInsertVersionQuery = "INSERT INTO %s (order, batch, name, migrated_at) VALUES (?, ?, ?, ?);"
	q := fmt.Sprintf(sqliteInsertVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version.Order, m.Version.Batch, m.Version.Name, m.Version.MigratedAt}, nil
}

func (s StateManager) RemoveQuery(m *migration.Migration) (string, []interface{}, error) {
	const sqliteDeleteVersionQuery = "DELETE FROM %s WHERE order = ?;"
	q := fmt.Sprintf(sqliteDeleteVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version}, nil
}

func (s StateManager) DropQuery() string {
	const sqliteDropMigrationsQuery = "DROP TABLE IF EXISTS %s;"
	q := fmt.Sprintf(sqliteDropMigrationsQuery, s.migrationsTable)
	return q
}

func (s StateManager) ShowTablesQuery() string {
	return "SELECT name as table_name FROM sqlite_master WHERE type='table' ORDER BY name;"
}

func (s StateManager) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT order, batch, name, migrated_at FROM %s"

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY order DESC"
	} else {
		readSQL += " ORDER BY order ASC"
	}

	return fmt.Sprintf(readSQL, s.migrationsTable), nil
}
