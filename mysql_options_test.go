package tern

import (
	"database/sql"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUseMySQL(t *testing.T) {
	t.Parallel()

	t.Run("default mysql options", func(t *testing.T) {
		m := Migrator{}
		checkerRuns := 0
		checker := func(mysqlOpts *sqlgateway.MySQLOptions, cOpts *sqlgateway.ConnectOptions) {
			assert.Equal(t, "migrations", mysqlOpts.MigrationsTable)
			assert.Equal(t, "tern_migrations", mysqlOpts.LockKey)
			assert.Equal(t, 3, mysqlOpts.LockFor)
			assert.False(t, mysqlOpts.NoLock)
			checkerRuns++
		}

		optionsFn := UseMySQL(&sql.DB{}, checker)

		err := optionsFn(&m)
		require.NoError(t, err)
		require.Equal(t, 1, checkerRuns)
	})

	t.Run("default mysql options no lock", func(t *testing.T) {
		m := Migrator{}

		checkerRuns := 0
		checker := func(mysqlOpts *sqlgateway.MySQLOptions, cOpts *sqlgateway.ConnectOptions) {
			assert.Equal(t, "migrations", mysqlOpts.MigrationsTable)
			assert.Equal(t, "tern_migrations", mysqlOpts.LockKey)
			assert.Equal(t, 3, mysqlOpts.LockFor)
			assert.True(t, mysqlOpts.NoLock)
			checkerRuns++
		}

		optionsFn := UseMySQL(&sql.DB{}, WithMySQLNoLock(), checker)

		err := optionsFn(&m)
		require.NoError(t, err)
		require.Equal(t, 1, checkerRuns)
	})

	t.Run("custom mysql options", func(t *testing.T) {
		m := Migrator{}

		checkerRuns := 0
		checker := func(mysqlOpts *sqlgateway.MySQLOptions, cOpts *sqlgateway.ConnectOptions) {
			assert.Equal(t, "versions", mysqlOpts.MigrationsTable)
			assert.Equal(t, "created_at", mysqlOpts.MigratedAtColumn)
			assert.Equal(t, "foo", mysqlOpts.LockKey)
			assert.Equal(t, 5, mysqlOpts.LockFor)
			assert.False(t, mysqlOpts.NoLock, "lock expected")
			checkerRuns++
		}

		optionsFn := UseMySQL(
			&sql.DB{},
			WithMySQLMigrationTable("versions"),
			WithMySQLMigratedAtColumn("created_at"),
			WithMySQLLockFor(5),
			WithMySQLLockKey("foo"),
			checker)

		err := optionsFn(&m)
		require.NoError(t, err)
		require.Equal(t, 1, checkerRuns)
	})
}
