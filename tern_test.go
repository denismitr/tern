package tern

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const testConnection = "tern:secret@(127.0.0.1:33066)/tern_db?parseTime=true"

func Test_MigratorCanBeInstantiated(t *testing.T) {
	db, err := sqlx.Open("mysql", testConnection)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	m, err := NewMigrator(db)
	assert.NoError(t, err)
	assert.NotNil(t, m)
}

func Test_Tern_WithMySQL(t *testing.T) {
	db, err := sqlx.Open("mysql", testConnection)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	t.Run("it_can_migrate_up_and_down_everything_from_a_custom_folder", func(t *testing.T) {
		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 3)

		gateway, err := newMysqlGateway(db, DefaultMigrationsTable, mysqlDefaultLockKey, mysqlDefaultLockSeconds)
		if err != nil {
			t.Fatal(err)
		}

		versions, err := gateway.readVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, versions, 3)

		assert.Equal(t, "1596897167", versions[0])
		assert.Equal(t, "1596897188", versions[1])
		assert.Equal(t, "1597897177", versions[2])

		// expect 4 tables to exist now in the DB
		// migrations table and 3 tables created by scripts
		tables, err := gateway.showTables(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, tables, 4)
		assert.Equal(t, []string{"bar", "baz", "foo", "migrations"}, tables)

		// DO: lets bring it down
		if err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		}

		// expect migrations table to be clean
		versionsAfterDown, err := gateway.readVersions(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, versionsAfterDown, 0)

		if err := gateway.dropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it_will_skip_migrations_that_are_already_in_migrations_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		gateway, err := newMysqlGateway(db, DefaultMigrationsTable, mysqlDefaultLockKey, mysqlDefaultLockSeconds)
		if err != nil {
			t.Fatal(err)
		}

		// given we already have a migrations table
		if err := gateway.createMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}

		// given we have already migrated these 2 migrations
		existingMigrations := []string{"1596897167_create_foo_table", "1596897188_create_bar_table"}
		if err := gateway.writeVersions(ctx, existingMigrations); err != nil {
			t.Fatal(err)
		}

		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 1)

		versions, err := gateway.readVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 3 versions in migrations table
		assert.Len(t, versions, 3)
		assert.Equal(t, "1596897167", versions[0])
		assert.Equal(t, "1596897188", versions[1])
		assert.Equal(t, "1597897177", versions[2])

		// expect 2 tables to exist now in the DB
		// the migrations table and baz table created by the only script that ran
		tables, err := gateway.showTables(ctx)
		assert.NoError(t, err)

		assert.Len(t, tables, 2)
		assert.Equal(t, []string{"baz", "migrations"}, tables)

		// DO: execute down migrations to rollback all of them
		if err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		}

		versionsAfterDown, err := gateway.readVersions(ctx)
		assert.NoError(t, err)

		// expect no versions in migrations table
		assert.Len(t, versionsAfterDown, 0)

		// DO: clean up
		if err := gateway.dropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it_will_run_no_migrations_if_all_available_versions_are_in_migrations_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		gateway, err := newMysqlGateway(db, DefaultMigrationsTable, mysqlDefaultLockKey, mysqlDefaultLockSeconds)
		if err != nil {
			t.Fatal(err)
		}

		// given we already have a migrations table
		if err := gateway.createMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}

		// given we have already migrated these 2 migrations
		existingMigrations := []string{
			"1596897167_create_foo_table",
			"1596897188_create_bar_table",
			"1597897177_create_baz_table",
		}

		if err := gateway.writeVersions(ctx, existingMigrations); err != nil {
			t.Fatal(err)
		}

		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 0)

		versions, err := gateway.readVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 3 versions in migrations table
		assert.Len(t, versions, 3)
		assert.Equal(t, "1596897167", versions[0])
		assert.Equal(t, "1596897188", versions[1])
		assert.Equal(t, "1597897177", versions[2])

		// expect 2 tables to exist now in the DB
		// the migrations table and baz table created by the only script that ran
		tables, err := gateway.showTables(ctx)
		assert.NoError(t, err)

		assert.Len(t, tables, 1)
		assert.Equal(t, []string{"migrations"}, tables)

		// DO: execute down migrations to rollback all of them
		if err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		}

		versionsAfterDown, err := gateway.readVersions(ctx)
		assert.NoError(t, err)

		// expect no versions in migrations table
		assert.Len(t, versionsAfterDown, 0)

		// DO: clean up
		if err := gateway.dropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("run_single_migration_when_step_is_one", func(t *testing.T) {
		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		keys, err := m.Up(ctx, WithSteps(1))
		assert.NoError(t, err)
		assert.Len(t, keys, 1)

		gateway, err := newMysqlGateway(db, DefaultMigrationsTable, mysqlDefaultLockKey, mysqlDefaultLockSeconds)
		if err != nil {
			t.Fatal(err)
		}

		versions, err := gateway.readVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 3 versions in migrations table
		assert.Len(t, versions, 1)
		assert.Equal(t, "1596897167", versions[0])

		// expect 2 tables to exist now in the DB
		// the migrations table and baz table created by the only script that ran
		tables, err := gateway.showTables(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, tables, 2)
		assert.Equal(t, []string{"foo", DefaultMigrationsTable}, tables)

		// DO: execute down migrations to rollback all of them
		if err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		}

		// DO: clean up
		if err := gateway.dropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})
}


