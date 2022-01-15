package sqlgateway

import (
	"github.com/denismitr/tern/v3/migration"
)

const (
	ASC  = "ASC"
	DESC = "DESC"
)

type readVersionsFilter struct {
	Limit int
	Sort  string
}

type schema interface {
	initQuery() string
	insertQuery(m *migration.Migration) (string, []interface{})
	removeQuery(m *migration.Migration) (string, []interface{})
	dropQuery() string
	showTablesQuery() string
	readVersionsQuery(f readVersionsFilter) string
}
