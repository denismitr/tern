package sqlgateway

import (
	"fmt"
	"github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/migration"
)

type sqliteSchemaV1 struct {
	migrationsTable, migratedAtColumn string
}

func (s sqliteSchemaV1) initQuery() string {
	const sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			%s TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`

	return fmt.Sprintf(sqliteCreateMigrationsSchema, s.migrationsTable, s.migratedAtColumn)
}

func (s sqliteSchemaV1) insertQuery(m *migration.Migration) (string, []interface{}) {
	const sqliteInsertVersionQuery   = "INSERT INTO %s (version, name) VALUES (?, ?);"
	q := fmt.Sprintf(sqliteInsertVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version.Value, m.Name}
}

func (s sqliteSchemaV1) removeQuery(m *migration.Migration) (string, []interface{}) {
	const sqliteDeleteVersionQuery = "DELETE FROM %s WHERE version = ?;"
	q := fmt.Sprintf(sqliteDeleteVersionQuery, s.migrationsTable)
	return q, []interface{}{m.Version.Value}
}

func (s sqliteSchemaV1) dropQuery() string {
	const sqliteDropMigrationsQuery = "DROP TABLE IF EXISTS %s;"
	q := fmt.Sprintf(sqliteDropMigrationsQuery, s.migrationsTable)
	return q
}

func (s sqliteSchemaV1) showTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
}

func (s sqliteSchemaV1) readVersionsQuery(f readVersionsFilter) string {
	var readSQL = "SELECT `version`, `%s` FROM %s"

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == DESC {
		readSQL += " ORDER BY `version` DESC"
	} else {
		readSQL += " ORDER BY `version` ASC"
	}

	return fmt.Sprintf(readSQL, s.migratedAtColumn, s.migrationsTable)
}

var _ schema = (*sqliteSchemaV1)(nil)

func newSqliteSchemaV1(migrationsTable, migratedAtColumn string) *sqliteSchemaV1 {
	return &sqliteSchemaV1{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn}
}

type SqliteOptions struct {
	database.CommonOptions
}
