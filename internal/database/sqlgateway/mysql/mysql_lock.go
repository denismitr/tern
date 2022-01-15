package mysql

import (
	"context"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/pkg/errors"
)

const DefaultLockKey = "tern_migrations"
const DefaultLockSeconds = 3

type Options struct {
	database.CommonOptions
	LockKey string
	LockFor int // maybe refactor to duration
	NoLock  bool
}

type Locker struct {
	lockKey   string
	lockFor   int
	noLock    bool
}

func NewLocker(lockKey string, lockFor int, noLock bool) *Locker {
	return &Locker{lockKey: lockKey, lockFor: lockFor, noLock: noLock}
}

func (l *Locker) Lock(ctx context.Context, ex sqlgateway.CtxExecutor) error {
	if l.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, "SELECT GET_LOCK(?, ?)", l.lockKey, l.lockFor); err != nil {
		return errors.Wrapf(err, "could not obtain [%s] exclusive MySQL DB lock for [%d] seconds", l.lockKey, l.lockFor)
	}

	return nil
}

func (l *Locker) Unlock(ctx context.Context, ex sqlgateway.CtxExecutor) error {
	if l.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", l.lockKey); err != nil {
		return errors.Wrapf(err, "could not release [%s] exclusive MySQL DB lock", l.lockKey)
	}

	return nil
}
