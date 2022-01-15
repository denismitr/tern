package sqlgateway

import (
	"context"
)

type Locker interface {
	Lock(context.Context, CtxExecutor) error
	Unlock(context.Context, CtxExecutor) error
}

type nullLocker struct {}

func (nullLocker) Lock(context.Context, CtxExecutor) error {
	return nil
}

func (nullLocker) Unlock(context.Context, CtxExecutor) error {
	return nil
}
