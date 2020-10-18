package database

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/internal/retry"
	"github.com/pkg/errors"
	"time"
)

const (
	DefaultConnectionAttemts     = 100
	DefaultConnectionTimeout     = 60 * time.Second
	DefaultConnectionAttemptStep = 2 * time.Second
)

type ConnectOptions struct {
	MaxAttempts int
	MaxTimeout  time.Duration
	Step        time.Duration
}

func NewDefaultConnectOptions() *ConnectOptions {
	return &ConnectOptions{
		MaxAttempts: DefaultConnectionAttemts,
		MaxTimeout:  DefaultConnectionTimeout,
		Step:        DefaultConnectionAttemptStep,
	}
}

type connector interface {
	connect(ctx context.Context) (*sql.Conn, error)
	timeout() time.Duration
}

type RetryingConnector struct {
	options *ConnectOptions
	db *sql.DB
}

func (c RetryingConnector) timeout() time.Duration {
	return c.options.MaxTimeout
}

func MakeRetryingConnector(db *sql.DB, options *ConnectOptions) RetryingConnector {
	return RetryingConnector{db: db, options: options}
}

func (c RetryingConnector) connect(ctx context.Context) (*sql.Conn, error) {
	var conn *sql.Conn
	if err := retry.Incremental(ctx, 2*time.Second, c.options.MaxAttempts, func(attempt int) (err error) {
		conn, err = c.db.Conn(ctx)
		if err != nil {
			return retry.Error(errors.Wrap(err, "could not establish DB connection"), attempt)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return conn, nil
}
