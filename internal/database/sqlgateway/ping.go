package sqlgateway

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"time"
)

func Ping(ctx context.Context, db *sqlx.DB) error {
	// Проверяем есть ли связь с БД
	for attempts := 1; ; attempts++ {
		if err := db.Ping(); err == nil {
			break
		}

		time.Sleep(time.Duration(attempts) * 100 * time.Millisecond)

		// Убеждемся, что контекс не истек или не был отменен
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "could not Ping DB")
		}
	}

	// Убеждемся, что контекс не истек или не был отменен
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "could not Ping DB")
	}

	var result int
	if err := db.QueryRowxContext(ctx, "select 1").Scan(&result); err != nil {
		return errors.Wrap(err, "could not ping DB")
	}

	return nil
}