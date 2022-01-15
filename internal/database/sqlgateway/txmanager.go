package sqlgateway

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"strings"
)

var ErrTxDeadlock = errors.New("transaction deadlock occurred")

// TxConfig - configures tx
type TxConfig struct {
	Iso      sql.IsolationLevel
	ReadOnly bool
}

type TxConfigFunc func(*TxConfig)

// ISO - isolation level type
type ISO int

const (
	Serializable ISO = iota
	RepeatableRead
	ReadCommitted
)

// Isolation tx config function
func Isolation(iso ISO) TxConfigFunc {
	return func(txCfg *TxConfig) {
		switch iso {
		case Serializable:
			txCfg.Iso = sql.LevelSerializable
		case RepeatableRead:
			txCfg.Iso = sql.LevelRepeatableRead
		case ReadCommitted:
			txCfg.Iso = sql.LevelReadCommitted
		}
	}
}

type Preparer interface {
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
}

type Tx interface {
	Preparer
}

type DBPinger interface {
	Ping(ctx context.Context) error
}

type TxCallback func(context.Context, Tx) (interface{}, error)

type TxManager interface {
	ReadOnly(context.Context, TxCallback, ...TxConfigFunc) (interface{}, error)
	ReadWrite(context.Context, TxCallback, ...TxConfigFunc) (interface{}, error)
	ReadWithoutIsolation(ctx context.Context, cb TxCallback) (interface{}, error)
	DBPinger
}

type SqlxTxManager struct {
	db *sqlx.DB
}

func NewTxManager(db *sqlx.DB) *SqlxTxManager {
	return &SqlxTxManager{db: db}
}

func (txm *SqlxTxManager) Ping(ctx context.Context) error {
	return Ping(ctx, txm.db)
}

func (txm *SqlxTxManager) ReadOnly(
	ctx context.Context,
	cb TxCallback,
	cfn ...TxConfigFunc,
) (interface{}, error) {
	txCfg := TxConfig{
		Iso:      sql.LevelRepeatableRead,
		ReadOnly: true,
	}

	for _, fn := range cfn {
		fn(&txCfg)
	}

	return txm.isolate(ctx, cb, txCfg)
}

func (txm *SqlxTxManager) ReadWrite(
	ctx context.Context,
	cb TxCallback,
	cfn ...TxConfigFunc,
) (interface{}, error) {
	txCfg := TxConfig{
		Iso:      sql.LevelSerializable,
		ReadOnly: false,
	}

	for _, fn := range cfn {
		fn(&txCfg)
	}

	return txm.isolate(ctx, cb, txCfg)
}

func (txm *SqlxTxManager) ReadWithoutIsolation(
	ctx context.Context,
	cb TxCallback,
) (interface{}, error) {
	result, err := cb(ctx, txm.db)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (txm *SqlxTxManager) isolate(
	ctx context.Context,
	cb TxCallback,
	txCfg TxConfig,
) (interface{}, error) {
	txx, err := txm.db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: txCfg.ReadOnly, Isolation: txCfg.Iso})
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"could not start transaction. read-only: %v, isolation: %d",
			txCfg.ReadOnly, txCfg.Iso,
		)
	}

	result, err := cb(ctx, txx)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
			err = errors.Wrapf(
				ErrTxDeadlock,
				"read-only: %v, isolation: %d, on callback: %s",
				txCfg.ReadOnly, txCfg.Iso, err.Error(),
			)
		}

		if rbErr := txx.Rollback(); rbErr != nil {
			return nil, errors.Wrap(err, " : ROLLBACK : "+rbErr.Error())
		}

		return nil, err
	}

	if err := txx.Commit(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
			return nil, errors.Wrapf(
				ErrTxDeadlock,
				"read-only: %v, isolation: %d, on commit: %s",
				txCfg.ReadOnly, txCfg.Iso, err.Error(),
			)
		}

		return nil, errors.Wrapf(
			err,
			"could not commit transaction. read-only: %v, isolation: %d",
			txCfg.ReadOnly, txCfg.Iso,
		)
	}

	return result, nil
}
