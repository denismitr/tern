package sqlite

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
)

type Dialect struct {
	migrationsTable string
}

type Options struct {
	database.CommonOptions
}

func NewDialect(migrationsTable string) *Dialect {
	return &Dialect{migrationsTable: migrationsTable}
}

var _ sqlgateway.Dialect = (*Dialect)(nil)

func (d Dialect) InitQuery() string {
	const sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			order BIGINT PRIMARY KEY,
            batch BIGINT,
			name VARCHAR(255),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`

	return fmt.Sprintf(sqliteCreateMigrationsSchema, d.migrationsTable)
}

func (d Dialect) InsertQuery(m database.Migration) (string, []interface{}, error) {
	const sqliteInsertVersionQuery = "INSERT INTO %s (order, batch, name, migrated_at) VALUES (?, ?, ?, ?);"
	q := fmt.Sprintf(sqliteInsertVersionQuery, d.migrationsTable)
	return q, []interface{}{m.Version.Order, m.Version.Batch, m.Version.Name, m.Version.MigratedAt}, nil
}

func (d Dialect) RemoveQuery(m database.Migration) (string, []interface{}, error) {
	const sqliteDeleteVersionQuery = "DELETE FROM %s WHERE order = ?;"
	q := fmt.Sprintf(sqliteDeleteVersionQuery, d.migrationsTable)
	return q, []interface{}{m.Version}, nil
}

func (d Dialect) DropQuery() string {
	const sqliteDropMigrationsQuery = "DROP TABLE IF EXISTS %s;"
	q := fmt.Sprintf(sqliteDropMigrationsQuery, d.migrationsTable)
	return q
}

func (d Dialect) ShowTablesQuery() string {
	return "SELECT name as table_name FROM sqlite_master WHERE type='table' ORDER BY name;"
}

func (d Dialect) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT order, batch, name, migrated_at FROM %s"

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY order DESC"
	} else {
		readSQL += " ORDER BY order ASC"
	}

	return fmt.Sprintf(readSQL, d.migrationsTable), nil
}
