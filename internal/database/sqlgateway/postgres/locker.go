package postgres

import (
	"context"
	"fmt"

	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/pkg/errors"
)

const DefaultLockKey = 99887766
const DefaultLockSeconds = 3

type Options struct {
	database.CommonOptions
	LockKey int
	LockFor int // maybe refactor to duration
	NoLock  bool
}

type Locker struct {
	lockKey int
	lockFor int
	noLock  bool
}

func NewLocker(lockKey int, lockFor int, noLock bool) *Locker {
	return &Locker{lockKey: lockKey, lockFor: lockFor, noLock: noLock}
}

func (l *Locker) Lock(ctx context.Context, ex sqlgateway.CtxExecutor) error {
	if l.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, fmt.Sprintf("SELECT pg_advisory_lock(%d)", l.lockKey)); err != nil {
		return errors.Wrapf(err, "could not obtain [%s] exclusive MySQL DB lock for [%d] seconds", l.lockKey, l.lockFor)
	}

	return nil
}

func (l *Locker) Unlock(ctx context.Context, ex sqlgateway.CtxExecutor) error {
	if l.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, "SELECT pg_advisory_unlock(%d)", l.lockKey); err != nil {
		return errors.Wrapf(err, "could not release [%s] exclusive MySQL DB lock", l.lockKey)
	}

	return nil
}
