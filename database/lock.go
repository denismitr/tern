package database

import (
	"context"
	"database/sql"
)

type locker interface {
	lock(ctx context.Context, conn *sql.Conn) error
	unlock(ctx context.Context, conn *sql.Conn) error
}
