package sqlgateway

import (
	"context"
)

type locker interface {
	lock(context.Context, Executor) error
	unlock(context.Context, Executor) error
}

type nullLocker struct {}

func (nullLocker) lock(context.Context, Executor) error {
	return nil
}

func (nullLocker) unlock(context.Context, Executor) error {
	return nil
}
