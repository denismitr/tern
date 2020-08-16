package tern

import (
	"context"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/migration"
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
		defer m.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 3)

		gateway, err := database.CreateServiceGateway(db, database.DefaultMigrationsTable)
		if err != nil {
			t.Fatal(err)
		}

		defer gateway.Close()

		versions, err := gateway.ReadVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, versions, 3)

		assert.Equal(t, "1596897167", versions[0])
		assert.Equal(t, "1596897188", versions[1])
		assert.Equal(t, "1597897177", versions[2])

		// expect 4 tables to exist now in the DB
		// migrations table and 3 tables created by scripts
		tables, err := gateway.ShowTables(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, tables, 4)
		assert.Equal(t, []string{"bar", "baz", "foo", "migrations"}, tables)

		// DO: lets bring it down
		if executed ,err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		} else {
			assert.Len(t, executed, 3)
		}

		// expect migrations table to be clean
		versionsAfterDown, err := gateway.ReadVersions(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, versionsAfterDown, 0)

		if err := gateway.DropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it_will_skip_migrations_that_are_already_in_migrations_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		gateway, err := database.CreateServiceGateway(db, database.DefaultMigrationsTable)
		if err != nil {
			t.Fatal(err)
		}

		// given we already have a migrations table
		if err := gateway.CreateMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}

		// given we have already migrated these 2 migrations
		existingMigrations := migration.Migrations(
			[]migration.Migration{
				{Key: "1596897167_create_foo_table", Name: "CreateGateway foo table", Version: "1596897167"},
				{Key: "1596897188_create_bar_table", Name: "CreateGateway bar table", Version: "1596897188"},
			},
		)

		if err := gateway.WriteVersions(ctx, existingMigrations); err != nil {
			t.Fatal(err)
		}

		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)
		defer m.Close()

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 1)

		versions, err := gateway.ReadVersions(ctx)
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
		tables, err := gateway.ShowTables(ctx)
		assert.NoError(t, err)

		assert.Len(t, tables, 2)
		assert.Equal(t, []string{"baz", "migrations"}, tables)

		// DO: execute down migrations to rollback all of them
		if executed, err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		} else {
			assert.Len(t, executed, 3)
		}

		versionsAfterDown, err := gateway.ReadVersions(ctx)
		assert.NoError(t, err)

		// expect no versions in migrations table
		assert.Len(t, versionsAfterDown, 0)

		// DO: clean up
		if err := gateway.DropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it_will_run_no_migrations_if_all_available_versions_are_in_migrations_table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		gateway, err := database.CreateServiceGateway(db, database.DefaultMigrationsTable)
		if err != nil {
			t.Fatal(err)
		}

		defer gateway.Close()

		// given we already have a migrations table
		if err := gateway.CreateMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}

		// given we have already migrated these 2 migrations
		existingMigrations := migration.Migrations(
			[]migration.Migration{
				{Key: "1596897167_create_foo_table", Name: "CreateGateway foo table", Version: "1596897167"},
				{Key: "1596897188_create_bar_table", Name: "CreateGateway bar table", Version: "1596897188"},
				{Key: "1597897177_create_bar_table", Name: "CreateGateway baz table", Version: "1597897177"},
			},
		)

		if err := gateway.WriteVersions(ctx, existingMigrations); err != nil {
			t.Fatal(err)
		}

		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)
		defer m.Close()

		keys, err := m.Up(ctx)
		assert.NoError(t, err)
		assert.Len(t, keys, 0)

		versions, err := gateway.ReadVersions(ctx)
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
		tables, err := gateway.ShowTables(ctx)
		assert.NoError(t, err)

		assert.Len(t, tables, 1)
		assert.Equal(t, []string{"migrations"}, tables)

		// DO: execute down migrations to rollback all of them
		if executed, err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		} else {
			assert.Len(t, executed, 3)
		}

		versionsAfterDown, err := gateway.ReadVersions(ctx)
		assert.NoError(t, err)

		// expect no versions in migrations table
		assert.Len(t, versionsAfterDown, 0)

		// DO: clean up
		if err := gateway.DropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("run_single_migration_when_step_is_one", func(t *testing.T) {
		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		defer m.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		keys, err := m.Up(ctx, WithSteps(1))
		assert.NoError(t, err)
		assert.Len(t, keys, 1)

		gateway, err := database.CreateServiceGateway(db, database.DefaultMigrationsTable)
		if err != nil {
			t.Fatal(err)
		}

		defer gateway.Close()

		versions, err := gateway.ReadVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 3 versions in migrations table
		assert.Len(t, versions, 1)
		assert.Equal(t, "1596897167", versions[0])

		// expect 2 tables to exist now in the DB
		// the migrations table and baz table created by the only script that ran
		tables, err := gateway.ShowTables(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, tables, 2)
		assert.Equal(t, []string{"foo", database.DefaultMigrationsTable}, tables)

		// DO: execute down migrations to rollback all of them
		if executed, err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		} else {
			assert.Len(t, executed, 1)
		}

		// DO: clean up
		if err := gateway.DropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it_can_migrate_a_single_file", func(t *testing.T) {
		m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
		assert.NoError(t, err)

		defer m.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		defer cancel()

		keys, err := m.Up(ctx, WithKeys("1596897188_create_bar_table"))
		assert.NoError(t, err)
		assert.Len(t, keys, 1)

		gateway, err := database.CreateServiceGateway(db, database.DefaultMigrationsTable)
		if err != nil {
			t.Fatal(err)
		}

		defer gateway.Close()

		versions, err := gateway.ReadVersions(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 3 versions in migrations table
		assert.Len(t, versions, 1)
		assert.Equal(t, "1596897188", versions[0])

		tables, err := gateway.ShowTables(ctx)
		if err != nil {
			assert.NoError(t, err)
		}

		assert.Len(t, tables, 2)
		assert.Equal(t, []string{"bar", database.DefaultMigrationsTable}, tables)

		// DO: execute down migrations to rollback all of them
		if executed, err := m.Down(ctx); err != nil {
			assert.NoError(t, err)
		} else {
			assert.Len(t, executed, 1)
		}

		// DO: clean up
		if err := gateway.DropMigrationsTable(ctx); err != nil {
			t.Fatal(err)
		}
	})
}


