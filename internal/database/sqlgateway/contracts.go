package sqlgateway

import (
	"context"
	"database/sql"
)

type ctxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
