package database

import (
	"context"
	"database/sql"
)

type Locker interface {
	Lock(ctx context.Context, conn *sql.Conn) error
	Unlock(ctx context.Context, conn *sql.Conn) error
}

type NullLocker struct {}

func (NullLocker) Lock(context.Context, *sql.Conn) error {
	return nil
}

func (NullLocker) Unlock(context.Context, *sql.Conn) error {
	return nil
}
