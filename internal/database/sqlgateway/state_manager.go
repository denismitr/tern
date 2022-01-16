package sqlgateway

import (
	"github.com/denismitr/tern/v3/internal/database"
)

const (
	ASC  = "ASC"
	DESC = "DESC"
)

type ReadVersionsFilter struct {
	Limit int
	Sort  string
}

type Dialect interface {
	InitQuery() string
	InsertQuery(m database.Migration) (string, []interface{}, error)
	RemoveQuery(m database.Migration) (string, []interface{}, error)
	DropQuery() string
	ShowTablesQuery() string
	ReadVersionsQuery(f database.ReadVersionsFilter) (string, error)
}
