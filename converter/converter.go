package converter

import (
	"context"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"unicode"
)

var ErrInvalidTimestamp = errors.New("invalid timestamp in migration filename")
var ErrNotAMigrationFile = errors.New("not a migration file")
var ErrTooManyFilesForKey = errors.New("too many files for single mysqlDefaultLockKey")

type Filter struct {
	Keys  []string
}

type Converter interface {
	Convert(ctx context.Context, f Filter) (migration.Migrations, error)
}

func filterMigrations(m migration.Migrations, f Filter) migration.Migrations {
	return m
}

func ucFirst(s string) string {
	r := []rune(s)

	if len(r) == 0 {
		return ""
	}

	f := string(unicode.ToUpper(r[0]))

	return f + string(r[1:])
}

func inStringSlice(key string, keys []string) bool {
	if keys == nil {
		return false
	}

	for i := range keys {
		if keys[i] == key {
			return true
		}
	}
	return false
}
