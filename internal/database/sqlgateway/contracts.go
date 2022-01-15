package sqlgateway

import (
	"context"
	"database/sql"
)

type CtxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
