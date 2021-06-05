package sqlgateway

import (
	"context"
	"database/sql"
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
	Connect(ctx context.Context) (*sql.Conn, error)
	Timeout() time.Duration
	Close() error
}

type RetryingConnector struct {
	options *ConnectOptions
	db *sql.DB
	conn *sql.Conn
}

func (c RetryingConnector) Timeout() time.Duration {
	return c.options.MaxTimeout
}

func MakeRetryingConnector(db *sql.DB, options *ConnectOptions) *RetryingConnector {
	return &RetryingConnector{db: db, options: options}
}

func (c *RetryingConnector) Connect(ctx context.Context) (*sql.Conn, error) {
	if c.conn != nil {
		return c.conn, nil
	}

	result, err := retry.Incremental(ctx, 2*time.Second, c.options.MaxAttempts, func(attempt int) (interface{}, error) {
		conn, err := c.db.Conn(ctx)
		if err != nil {
			return nil, retry.Error(errors.Wrap(err, "could not establish DB connection"), attempt)
		}

		if err := conn.PingContext(ctx); err != nil {
			return nil, errors.Wrap(err, "db ping failed")
		}

		return conn, nil
	})

	if err != nil {
		return nil, err
	}

	conn, ok := result.(*sql.Conn)
	if !ok {
		panic("how could result not be an instance of *sql.Conn")
	}

	c.conn = conn

	return conn, nil
}

func (c *RetryingConnector) Close() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return errors.Wrap(err, "retrying connector could not close the connection")
		}
	}

	return nil
}
