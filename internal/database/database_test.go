package database

import (
	"github.com/denismitr/tern/v3/migration"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSchedule(t *testing.T) {
	t.Parallel()

	vf1 := migration.Timestamp("1596897167")
	mf1 := migration.New(
		vf1,
		"Create foo table",
		[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY);"},
		[]string{"DROP TABLE IF EXISTS foo;"},
	)

	vf2 := migration.Timestamp("1596899255")
	mf2 := migration.New(
		vf2,
		"Create bar table",
		[]string{"CREATE TABLE IF NOT EXISTS bar (uid binary(16) PRIMARY KEY);"},
		[]string{"DROP TABLE IF EXISTS bar;"},
	)

	vf3 := migration.Timestamp("1596899399")
	mf3 := migration.New(
		vf3,
		"Create baz table",
		[]string{"CREATE TABLE IF NOT EXISTS baz (uid binary(16) PRIMARY KEY);"},
		[]string{"DROP TABLE IF EXISTS baz;"},
	)

	m1, err := mf1()
	require.NoError(t, err)

	m2, err := mf2()
	require.NoError(t, err)

	m3, err := mf3()
	require.NoError(t, err)

	v1, err := vf1()
	require.NoError(t, err)

	v2, err := vf2()
	require.NoError(t, err)

	v3, err := vf3()
	require.NoError(t, err)

	t.Run("it will schedule only 1 migration for rollback if steps are limited to one", func(t *testing.T) {
		scheduled := ScheduleForRollback(migration.Migrations{m1, m2, m3}, []migration.Order{v1, v2, v3}, Plan{Steps: 1})
		require.Len(t, scheduled, 1)
		assert.Equal(t, v3.Value, scheduled[0].Version.Value)
		assert.Equal(t, "Create baz table", scheduled[0].Name)
	})

	t.Run("it will schedule 1 specific migration for rollback if steps are limited to one and versions in plan", func(t *testing.T) {
		scheduled := ScheduleForRollback(
			migration.Migrations{m1, m2, m3},
			[]migration.Order{v1, v2, v3},
			Plan{Steps: 1, Versions: []migration.Order{v2}},
		)
		require.Len(t, scheduled, 1)
		assert.Equal(t, v2.Value, scheduled[0].Version.Value)
		assert.Equal(t, "Create bar table", scheduled[0].Name)
	})

	t.Run("it will schedule all migrations for rollback if specific plan not specified", func(t *testing.T) {
		scheduled := ScheduleForRollback(migration.Migrations{m1, m2, m3}, []migration.Order{v1, v2, v3}, Plan{})
		require.Len(t, scheduled, 3)

		assert.Equal(t, v3.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create baz table", scheduled[0].Name)

		assert.Equal(t, v2.Value, scheduled[1].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[1].Version.Format)
		assert.Equal(t, "Create bar table", scheduled[1].Name)

		assert.Equal(t, v1.Value, scheduled[2].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[2].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[2].Name)
	})

	t.Run("it will schedule everything for migration if nothing was migrated", func(t *testing.T) {
		scheduled := ScheduleForMigration(migration.Migrations{m1, m2, m3}, []migration.Order{}, Plan{})
		require.Len(t, scheduled, 3)

		assert.Equal(t, v3.Value, scheduled[2].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[2].Version.Format)
		assert.Equal(t, "Create baz table", scheduled[2].Name)

		assert.Equal(t, v2.Value, scheduled[1].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[1].Version.Format)
		assert.Equal(t, "Create bar table", scheduled[1].Name)

		assert.Equal(t, v1.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[0].Name)
	})

	t.Run("it will schedule only 2 migrations for migration if plan steps are limited to 2", func(t *testing.T) {
		scheduled := ScheduleForMigration(migration.Migrations{m1, m2, m3}, []migration.Order{}, Plan{Steps: 2})
		require.Len(t, scheduled, 2)

		assert.Equal(t, v2.Value, scheduled[1].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[1].Version.Format)
		assert.Equal(t, "Create bar table", scheduled[1].Name)

		assert.Equal(t, v1.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[0].Name)
	})

	t.Run("it will schedule only 2 migrations for migration if 2 are specified in plan versions", func(t *testing.T) {
		scheduled := ScheduleForMigration(
			migration.Migrations{m1, m2, m3},
			[]migration.Order{},
			Plan{Versions: []migration.Order{v1, v3}},
		)

		require.Len(t, scheduled, 2)

		assert.Equal(t, v1.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[0].Name)

		assert.Equal(t, v3.Value, scheduled[1].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[1].Version.Format)
		assert.Equal(t, "Create baz table", scheduled[1].Name)
	})

	t.Run("it will schedule only 1 migration for migration if 2 are specified in plan versions and 1 in steps", func(t *testing.T) {
		scheduled := ScheduleForMigration(
			migration.Migrations{m1, m2, m3},
			[]migration.Order{},
			Plan{Versions: []migration.Order{v1, v3}, Steps: 1},
		)

		require.Len(t, scheduled, 1)

		assert.Equal(t, v1.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[0].Name)
	})

	t.Run("it will schedule only 1 migration for refresh if steps are limited to one", func(t *testing.T) {
		scheduled := ScheduleForRefresh(migration.Migrations{m1, m2, m3}, []migration.Order{v1, v2, v3}, Plan{Steps: 1})
		require.Len(t, scheduled, 1)
		assert.Equal(t, v3.Value, scheduled[0].Version.Value)
		assert.Equal(t, "Create baz table", scheduled[0].Name)
	})

	t.Run("it will schedule 1 specific migration for refresh if steps are limited to one and versions in plan", func(t *testing.T) {
		scheduled := ScheduleForRefresh(
			migration.Migrations{m1, m2, m3},
			[]migration.Order{v1, v2, v3},
			Plan{Steps: 1, Versions: []migration.Order{v2}},
		)
		require.Len(t, scheduled, 1)
		assert.Equal(t, v2.Value, scheduled[0].Version.Value)
		assert.Equal(t, "Create bar table", scheduled[0].Name)
	})

	t.Run("it will schedule all migrations for refresh if specific plan not specified", func(t *testing.T) {
		scheduled := ScheduleForRefresh(migration.Migrations{m1, m2, m3}, []migration.Order{v1, v2, v3}, Plan{})
		require.Len(t, scheduled, 3)

		assert.Equal(t, v3.Value, scheduled[0].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[0].Version.Format)
		assert.Equal(t, "Create baz table", scheduled[0].Name)

		assert.Equal(t, v2.Value, scheduled[1].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[1].Version.Format)
		assert.Equal(t, "Create bar table", scheduled[1].Name)

		assert.Equal(t, v1.Value, scheduled[2].Version.Value)
		assert.Equal(t, migration.TimestampFormat, scheduled[2].Version.Format)
		assert.Equal(t, "Create foo table", scheduled[2].Name)
	})
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
//		require.Len(t, ex.queries, 2)
//		assert.Equal(t, m.Migrate[0], ex.queries[0])
//		assert.Equal(t, versionInsertionQuery, ex.queries[1])
//
//		require.Len(t, ex.args, 2)
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
//		require.Len(t, ex.queries, 2)
//		assert.Equal(t, m.Rollback[0], ex.queries[0])
//		assert.Equal(t, versionDeletionQuery, ex.queries[1])
//
//		require.Len(t, ex.args, 2)
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
