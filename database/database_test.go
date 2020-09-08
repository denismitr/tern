package database

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

type ctxExecutorMock struct {
	calls      int
	queries    []string
	args       map[string][]interface{}
	migrateErr error
	versionErr error
}

func newCtxExecutorMock() *ctxExecutorMock {
	return &ctxExecutorMock{
		args: make(map[string][]interface{}),
	}
}

func newCtxExecutorMockWithErrors(migrateErr, versionErr error) *ctxExecutorMock {
	return &ctxExecutorMock{
		args:       make(map[string][]interface{}),
		migrateErr: migrateErr,
		versionErr: versionErr,
	}
}

func (ex *ctxExecutorMock) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ex.calls++
	ex.queries = append(ex.queries, query)
	ex.args[query] = args

	if ex.migrateErr != nil && ex.calls == 1 {
		return nil, ex.migrateErr
	}

	if ex.versionErr != nil && ex.calls == 2 {
		return nil, ex.versionErr
	}

	return nil, nil
}

//func Test_MigrateAndRollback_Funcs(t *testing.T) {
//	versionDeletionQuery := fmt.Sprintf(mysqlDeleteVersionQuery, DefaultMigrationsTable)
//	versionInsertionQuery := fmt.Sprintf(mysqlInsertVersionQuery, DefaultMigrationsTable)
//
//	lg := &logger.NullLogger{}
//
//	t.Run("it will process valid migration with success", func(t *testing.T) {
//		ex := newCtxExecutorMock()
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//
//		if err := migrate(ctx, ex, lg, m, versionInsertionQuery); err != nil {
//			t.Fatal(err)
//		}
//
//		assert.Equal(t, 2, ex.calls)
//		assert.Len(t, ex.queries, 2)
//		assert.Equal(t, m.Migrate[0], ex.queries[0])
//		assert.Equal(t, versionInsertionQuery, ex.queries[1])
//
//		assert.Len(t, ex.args, 2)
//		assert.Equal(t, []interface{}(nil), ex.args[m.Migrate[0]])
//		assert.Equal(t, []interface{}{"1596897167", "Create foo table"}, ex.args[versionInsertionQuery])
//	})
//
//	t.Run("it will return error if migration fails", func(t *testing.T) {
//		ex := newCtxExecutorMockWithErrors(errors.New("FOO migration failed"), nil)
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//
//		if err := migrate(ctx, ex, lg, m, versionInsertionQuery); err == nil {
//			t.Fatal(err)
//		} else {
//			assert.Equal(t, "could not run migration [1596897167_create_foo_table]: FOO migration failed", err.Error())
//		}
//	})
//
//	t.Run("it will return error if version insertion fails", func(t *testing.T) {
//		ex := newCtxExecutorMockWithErrors(nil, errors.New("unique constraint violation"))
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//
//		if err := migrate(ctx, ex, lg, m, versionInsertionQuery); err == nil {
//			t.Fatal(err)
//		} else {
//			assert.Equal(t, "could not insert migration version [1596897167]: unique constraint violation", err.Error())
//		}
//	})
//
//	t.Run("it will run valid rollback with success", func(t *testing.T) {
//		ex := newCtxExecutorMock()
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//		if err := rollback(ctx, ex, lg, m, versionDeletionQuery); err != nil {
//			t.Fatal(err)
//		}
//
//		assert.Equal(t, 2, ex.calls)
//		assert.Len(t, ex.queries, 2)
//		assert.Equal(t, m.Rollback[0], ex.queries[0])
//		assert.Equal(t, versionDeletionQuery, ex.queries[1])
//
//		assert.Len(t, ex.args, 2)
//		assert.Equal(t, []interface{}(nil), ex.args[m.Rollback[0]])
//		assert.Equal(t, []interface{}{"1596897167"}, ex.args[versionDeletionQuery])
//	})
//
//	t.Run("it will return error if rollback fails", func(t *testing.T) {
//		ex := newCtxExecutorMockWithErrors(errors.New("FOO rollback failed"), nil)
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//
//		if err := rollback(ctx, ex, lg, m, versionDeletionQuery); err == nil {
//			t.Fatal(err)
//		} else {
//			assert.Equal(t, "could not rollback migration [1596897167_create_foo_table]: FOO rollback failed", err.Error())
//		}
//	})
//
//	t.Run("it will return error if version removal fails", func(t *testing.T) {
//		ex := newCtxExecutorMockWithErrors(nil, errors.New("version not found"))
//
//		m := migration.New(
//			"1596897167",
//			"Create foo table",
//			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
//			[]string{"DROP TABLE IF EXISTS foo;"},
//		)
//
//		ctx := context.Background()
//
//		if err := rollback(ctx, ex, lg, m, versionDeletionQuery); err == nil {
//			t.Fatal(err)
//		} else {
//			assert.Equal(t, "could not remove migration version [1596897167]: version not found", err.Error())
//		}
//	})
//}
