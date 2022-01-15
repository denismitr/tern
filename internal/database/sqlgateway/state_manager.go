package sqlgateway

import (
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/migration"
)

const (
	ASC  = "ASC"
	DESC = "DESC"
)

type ReadVersionsFilter struct {
	Limit int
	Sort  string
}

type StateManager interface {
	InitQuery() string
	InsertQuery(m *migration.Migration) (string, []interface{}, error)
	RemoveQuery(m *migration.Migration) (string, []interface{}, error)
	DropQuery() string
	ShowTablesQuery() string
	ReadVersionsQuery(f database.ReadVersionsFilter) (string, error)
}
