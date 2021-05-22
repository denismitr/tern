package sqlgateway

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/internal/retry"
	"github.com/pkg/errors"
	"time"
)

const (
	DefaultConnectionAttempts    = 100
	DefaultConnectionTimeout     = 60 * time.Second
	DefaultConnectionAttemptStep = 2 * time.Second
)

type ConnectOptions struct {
	MaxAttempts int
	MaxTimeout  time.Duration
	RetryStep   time.Duration
}

func NewDefaultConnectOptions() *ConnectOptions {
	return &ConnectOptions{
		MaxAttempts: DefaultConnectionAttempts,
		MaxTimeout:  DefaultConnectionTimeout,
		RetryStep:   DefaultConnectionAttemptStep,
	}
}

type SQLConnector interface {
	Connect(ctx context.Context) (*sql.Conn, database.ConnCloser, error)
	Timeout() time.Duration
}

type RetryingConnector struct {
	options *ConnectOptions
	db *sql.DB
}

func (c RetryingConnector) Timeout() time.Duration {
	return c.options.MaxTimeout
}

func MakeRetryingConnector(db *sql.DB, options *ConnectOptions) RetryingConnector {
	return RetryingConnector{db: db, options: options}
}

func (c RetryingConnector) Connect(ctx context.Context) (*sql.Conn, database.ConnCloser, error) {
	var conn *sql.Conn
	if err := retry.Incremental(ctx, 2*time.Second, c.options.MaxAttempts, func(attempt int) (err error) {
		conn, err = c.db.Conn(ctx)
		if err != nil {
			return retry.Error(errors.Wrap(err, "could not establish DB connection"), attempt)
		}

		if err := conn.PingContext(ctx); err != nil {
			return errors.Wrap(err, "db ping failed")
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}

	return conn, conn.Close, nil
}
