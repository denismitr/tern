package sqlgateway

import (
	"context"
)

type locker interface {
	lock(context.Context, ctxExecutor) error
	unlock(context.Context, ctxExecutor) error
}

type nullLocker struct {}

func (nullLocker) lock(context.Context, ctxExecutor) error {
	return nil
}

func (nullLocker) unlock(context.Context, ctxExecutor) error {
	return nil
}
