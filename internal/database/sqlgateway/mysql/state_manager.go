package mysql

import (
	"fmt"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/denismitr/tern/v3/migration"
	_ "github.com/go-sql-driver/mysql"
)

type StateManager struct {
	migrationsTable, charset string
}

var _ sqlgateway.StateManager = (*StateManager)(nil)

func NewStateManager(migrationsTable, charset string) *StateManager {
	return &StateManager{migrationsTable: migrationsTable, charset: charset}
}

func (s StateManager) InitQuery() string {
	const createSQL = `
		CREATE TABLE IF NOT EXISTS %s (
			order BIGINT PRIMARY KEY,
			batch BIGINT,
			name VARCHAR(120),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=InnoDB CHARACTER SET=%s
	`

	return fmt.Sprintf(createSQL, s.migrationsTable, s.charset)
}

func (s StateManager) InsertQuery(m *migration.Migration) (string, []interface{}, error) {
	const insertSQL = "INSERT INTO %s (`order`, `batch`, `name`, `migrated_at`) VALUES (?, ?, ?, ?);"

	// TODO: validation
	return fmt.Sprintf(insertSQL, s.migrationsTable), []interface{}{
		m.Version,
		m.Version.Batch,
		m.Version.Name,
		m.Version.MigratedAt,
	}, nil
}

func (s StateManager) ReadVersionsQuery(f database.ReadVersionsFilter) (string, error) {
	var readSQL = "SELECT `order`, `name`, `batch`, `migrated_at` FROM %s"

	// TODO: optimize with bytes.Buffer

	// TODO: validation

	// TODO: minBatch & maxBatch

	if f.Limit != 0 {
		readSQL += fmt.Sprintf(" LIMIT %d", f.Limit)
	}

	if f.Sort == database.DESC {
		readSQL += " ORDER BY `order` DESC"
	} else {
		readSQL += " ORDER BY `order` ASC"
	}

	return fmt.Sprintf(readSQL, s.migrationsTable), nil
}

func (s StateManager) RemoveQuery(m *migration.Migration) (string, []interface{}, error) {
	const removeSQL = "DELETE FROM %s WHERE `order` = ?;"
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