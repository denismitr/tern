package retry

import (
	"context"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var ErrTooManyAttempts = errors.New("too many retry attempts")

type Callable func(attempt int) error

type retryError struct {
	error
	attempt int
}

func Error(err error, attempt int) error {
	if err == nil {
		return nil
	}
	return &retryError{error: err, attempt: attempt}
}

type Attempts interface {
	Next() (time.Duration, bool)
	Current() int
}

func Start(ctx context.Context, a Attempts, cb Callable) error {
	for {
		err := cb(a.Current())
		if err == nil {
			return nil
		}

		// callable encountered an unrecoverable error
		if _, ok := err.(*retryError); !ok {
			return errors.Wrapf(err, "retry %d failed", a.Current())
		}

		next, stop := a.Next()
		if stop {
			return ErrTooManyAttempts
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(next):
			continue
		}
	}
}

func Incremental(ctx context.Context, step time.Duration, maxRetries int, cb Callable) error {
	return Start(ctx, IncrementalAttempts(step, maxRetries), cb)
}

type incrementalAttempts struct {
	sync.RWMutex
	prev time.Duration
	step time.Duration
	max int
	curr int
}

func (a *incrementalAttempts) Next() (time.Duration, bool) {
	a.Lock()
	defer a.Unlock()

	a.curr++
	if a.curr > a.max {
		return 0, true
	}

	next := a.prev + a.step
	a.prev = next

	return next, false
}

func (a *incrementalAttempts) Current() int {
	a.RLock()
	defer a.RUnlock()
	return a.curr
}

func IncrementalAttempts(step time.Duration, max int) Attempts {
	return &incrementalAttempts{
		prev: 0,
		step: step,
		max: max,
		curr: 1,
	}
}
