package source

import (
	"context"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/migration"
	"github.com/pkg/errors"
	"strings"
	"unicode"
)

var ErrInvalidTimestamp = errors.New("invalid timestamp in migration filename")
var ErrNotAMigrationFile = errors.New("not a migration file")
var ErrTooManyFilesForKey = errors.New("too many files for single mysqlDefaultLockKey")

type Filter struct {
	Versions []database.Order
}

type Selector interface {
	Select(ctx context.Context, f Filter) (migration.Migrations, error)
}

type Source interface {
	Selector

	IsValid() bool
	AlreadyExists(dt, name string) bool
	Create(dt, name string, withRollback bool) (*migration.Migration, error)
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

func keyContainsOfVersions(key string, versions []string) bool {
	if versions == nil {
		return false
	}

	segments := strings.Split(key, "_")
	if len(segments) < 2 {
		return false
	}

	for i := range versions {
		if segments[0] == versions[i] {
			return true
		}
	}

	return false
}
