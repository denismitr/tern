package mysql

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
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
			id BIGINT PRIMARY KEY,
			batch BIGINT,
			name VARCHAR(120),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, d.migrationsTable, d.charset)
}

func (d Dialect) InsertQuery(m database.Migration) (string, []interface{}, error) {
	const insertSQL = "INSERT INTO %s (`id`, `batch`, `name`, `migrated_at`) VALUES (?, ?, ?, ?);"

	// TODO: validation
	return fmt.Sprintf(insertSQL, d.migrationsTable), []interface{}{
		m.Version,
		m.Version.Batch,
		m.Version.Name,
		m.Version.MigratedAt,
	}, nil
}

func (d Dialect) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT `id`, `name`, `batch`, `migrated_at` FROM %s"

	// TODO: optimize with bytes.Buffer

	// TODO: validation

	// TODO: minBatch & maxBatch

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY `id` DESC"
	} else {
		readSQL += " ORDER BY `id` ASC"
	}

	return fmt.Sprintf(readSQL, d.migrationsTable), nil
}

func (d Dialect) RemoveQuery(m database.Migration) (string, []interface{}, error) {
	const removeSQL = "DELETE FROM %s WHERE `id` = ?;"
	// TODO: validation
	return fmt.Sprintf(removeSQL, d.migrationsTable), []interface{}{m.Version}, nil
}

func (d Dialect) DropQuery() string {
	const dropSQL = `
		DROP TABLE IF EXISTS %s;
	`
	return fmt.Sprintf(dropSQL, d.migrationsTable)
}

func (d Dialect) ShowTablesQuery() string {
	return "SHOW TABLES;"
}
