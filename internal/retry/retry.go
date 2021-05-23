package retry

import (
	"context"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var ErrTooManyAttempts = errors.New("too many retry attempts")
var ErrContextEnded = errors.New("context ended")

type Callable func(attempt int) (interface{}, error)

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

func start(ctx context.Context, a Attempts, cb Callable) (interface{}, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrap(ErrContextEnded, err.Error())
		}

		result, err := cb(a.Current())
		if err == nil {
			return result, nil
		}

		// если не ошибка типа retryError
		// нужно закончить дальнейшиие попытки
		if _, ok := err.(*retryError); !ok {
			return nil, errors.Wrapf(err, "retry %d failed", a.Current())
		}

		next, stop := a.Next()
		if stop {
			return nil, ErrTooManyAttempts
		}

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ErrContextEnded, ctx.Err().Error())
		case <-time.After(next):
			continue
		}
	}
}

func Incremental(ctx context.Context, step time.Duration, maxRetries int, cb Callable) (interface{}, error) {
	return start(ctx, IncrementalAttempts(step, maxRetries), cb)
}

type incrementalAttempts struct {
	sync.RWMutex
	prev time.Duration
	step time.Duration
	max  int
	curr int
}

func (a *incrementalAttempts) Next() (time.Duration, bool) {
	a.Lock()
	defer a.Unlock()

	a.curr++
	if a.curr > a.max {
		return 0, true
	}

	// если пред. шаг был 0.5с а шаг 0.25c
	// то след шаг будет через 0.5c + 0.25c = 0.75с
	// еще след шаг будет занимать уже 1c и тд
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
		max:  max,
		curr: 1,
	}
}
