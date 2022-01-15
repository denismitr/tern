package sqlite

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/denismitr/tern/v3/migration"
)

type StateManager struct {
	migrationsTable, migratedAtColumn string
}

func NewStateManager(migrationsTable string, migratedAtColumn string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn}
}

var _ sqlgateway.StateManager = (*StateManager)(nil)

func (s StateManager) InitQuery() string {
	const sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			%s TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`

	return fmt.Sprintf(sqliteCreateMigrationsSchema, s.migrationsTable, s.migratedAtColumn)
}

func (s StateManager) InsertQuery(m *migration.Migration) (string, []interface{}, error) {
	const sqliteInsertVersionQuery = "INSERT INTO %s (version, batch, name) VALUES (?, ?);"
	q := fmt.Sprintf(sqliteInsertVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version, m.Batch, m.Name}, nil
}

func (s StateManager) RemoveQuery(m *migration.Migration) (string, []interface{}, error) {
	const sqliteDeleteVersionQuery = "DELETE FROM %s WHERE version = ?;"
	q := fmt.Sprintf(sqliteDeleteVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version}, nil
}

func (s StateManager) DropQuery() string {
	const sqliteDropMigrationsQuery = "DROP TABLE IF EXISTS %s;"
	q := fmt.Sprintf(sqliteDropMigrationsQuery, s.migrationsTable)
	return q
}

func (s StateManager) ShowTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
}

func (s StateManager) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT `version`, `%s` FROM %s"

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY `version` DESC"
	} else {
		readSQL += " ORDER BY `version` ASC"
	}

	return fmt.Sprintf(readSQL, s.migratedAtColumn, s.migrationsTable), nil
}

func newSqliteSchemaV1(migrationsTable, migratedAtColumn string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn}
}

type Options struct {
	database.CommonOptions
}
