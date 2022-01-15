package sqlgateway

import (
	"fmt"
	"github.com/denismitr/tern/v3/migration"
)

type mysqlSchemaV1 struct {
	migrationsTable, migratedAtColumn, charset string
}

var _ schema = (*mysqlSchemaV1)(nil)

func newMysqlSchemaV1(migrationsTable, migratedAtColumn, charset string) *mysqlSchemaV1 {
	return &mysqlSchemaV1{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn, charset: charset}
}

func (s mysqlSchemaV1) initQuery() string {
	const createSQL = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(14) PRIMARY KEY,
			name VARCHAR(120),
			%s TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, s.migrationsTable, s.migratedAtColumn, s.charset)
}

func (s mysqlSchemaV1) insertQuery(m *migration.Migration) (string, []interface{}) {
	const insertSQL = `
		INSERT INTO %s (version, name) VALUES (?, ?);	
	`
	v := m.Version.Value
	n := m.Name
	return fmt.Sprintf(insertSQL, s.migrationsTable), []interface{}{v, n}
}

func (s mysqlSchemaV1) readVersionsQuery(f readVersionsFilter) string {
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

func (s mysqlSchemaV1) removeQuery(m *migration.Migration) (string, []interface{}) {
	const removeSQL = "DELETE FROM %s WHERE `version` = ?;"
	v := m.Version.Value
	return fmt.Sprintf(removeSQL, s.migrationsTable), []interface{}{v}
}

func (s mysqlSchemaV1) dropQuery() string {
	const dropSQL = `
		DROP TABLE IF EXISTS %s;
	`
	return fmt.Sprintf(dropSQL, s.migrationsTable)
}

func (s mysqlSchemaV1) showTablesQuery() string {
	return "SHOW TABLES;"
}
