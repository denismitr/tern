package mysql

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/denismitr/tern/v3/migration"
	_ "github.com/go-sql-driver/mysql"
)

type StateManager struct {
	migrationsTable, migratedAtColumn, charset string
}

var _ sqlgateway.StateManager = (*StateManager)(nil)

func NewStateManager(migrationsTable, migratedAtColumn, charset string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable, migratedAtColumn: migratedAtColumn, charset: charset}
}

func (s StateManager) InitQuery() string {
	const createSQL = `
		CREATE TABLE IF NOT EXISTS %s (
			version BIGINT PRIMARY KEY,
			batch BIGINT,
			name VARCHAR(120),
			%s TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, s.migrationsTable, s.migratedAtColumn, s.charset)
}

func (s StateManager) InsertQuery(m *migration.Migration) (string, []interface{}, error) {
	const insertSQL = "INSERT INTO %s (`version`, `batch`, `name`) VALUES (?, ?);"

	// TODO: validation
	return fmt.Sprintf(insertSQL, s.migrationsTable), []interface{}{m.Version, m.Batch, m.Name}, nil
}

func (s StateManager) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT `version`, `%s` FROM %s"

	// TODO: optimize with bytes.Buffer

	// TODO: validation

	// TODO: minBatch & maxBatch

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

func (s StateManager) RemoveQuery(m *migration.Migration) (string, []interface{}, error) {
	const removeSQL = "DELETE FROM %s WHERE `version` = ?;"
	// TODO: validation
	return fmt.Sprintf(removeSQL, s.migrationsTable), []interface{}{m.Version}, nil
}

func (s StateManager) DropQuery() string {
	const dropSQL = `
		DROP TABLE IF EXISTS %s;
	`
	return fmt.Sprintf(dropSQL, s.migrationsTable)
}

func (s StateManager) ShowTablesQuery() string {
	return "SHOW TABLES;"
}