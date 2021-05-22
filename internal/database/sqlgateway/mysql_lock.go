package sqlgateway

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
)

type mySQLLocker struct {
	lockKey string
	lockFor int
	noLock  bool
}

func (g *mySQLLocker) Lock(ctx context.Context, conn *sql.Conn) error {
	if g.noLock {
		return nil
	}

	if _, err := conn.ExecContext(ctx, "SELECT GET_LOCK(?, ?)", g.lockKey, g.lockFor); err != nil {
		return errors.Wrapf(err, "could not obtain [%s] exclusive MySQL DB Lock for [%d] seconds", g.lockKey, g.lockFor)
	}

	return nil
}

func (g *mySQLLocker) Unlock(ctx context.Context, conn *sql.Conn) error {
	if g.noLock {
		return nil
	}

	if _, err := conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", g.lockKey); err != nil {
		return errors.Wrapf(err, "could not release [%s] exclusive MySQL DB Lock", g.lockKey)
	}

	return nil
}
