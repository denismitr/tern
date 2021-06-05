package sqlgateway

import (
	"context"
	"github.com/pkg/errors"
)

type mySQLLocker struct {
	lockKey   string
	lockFor   int
	noLock    bool
}

func newMySQLLocker(lockKey string, lockFor int, noLock bool) *mySQLLocker {
	return &mySQLLocker{lockKey: lockKey, lockFor: lockFor, noLock: noLock}
}

func (msl *mySQLLocker) lock(ctx context.Context, ex Executor) error {
	if msl.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, "SELECT GET_LOCK(?, ?)", msl.lockKey, msl.lockFor); err != nil {
		return errors.Wrapf(err, "could not obtain [%s] exclusive MySQL DB lock for [%d] seconds", msl.lockKey, msl.lockFor)
	}

	return nil
}

func (msl *mySQLLocker) unlock(ctx context.Context, ex Executor) error {
	if msl.noLock {
		return nil
	}

	if _, err := ex.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", msl.lockKey); err != nil {
		return errors.Wrapf(err, "could not release [%s] exclusive MySQL DB lock", msl.lockKey)
	}

	return nil
}
