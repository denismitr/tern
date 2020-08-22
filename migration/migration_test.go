package migration

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func Test_MigrationCanAssambleScriptsInOne(t *testing.T) {
	tt := []struct{
	    name string
	    migrate []string
	    migrateScripts string
	    rollback []string
	    rollbackScripts string
	}{
	    {
	        name: "single scripts with no trailing semicolon",
	        migrate: []string{"CREATE foo"},
	        migrateScripts: "CREATE foo;",
	        rollback: []string{"DROP foo"},
	        rollbackScripts: "DROP foo;",
	    },
		{
			name: "two scripts with one with trailing semicolon",
			migrate: []string{"CREATE TABLE foo;", "INSERT INTO foo (name) VALUES (?)"},
			migrateScripts: "CREATE TABLE foo;\nINSERT INTO foo (name) VALUES (?);",
			rollback: []string{"DROP TABLE foo"},
			rollbackScripts: "DROP TABLE foo;",
		},
		{
			name: "three scripts with no trailing semicolons",
			migrate: []string{"CREATE TABLE foo", "INSERT INTO foo (name) VALUES (?)", "CREATE TABLE IF NOT EXISTS baz"},
			migrateScripts: "CREATE TABLE foo;\nINSERT INTO foo (name) VALUES (?);\nCREATE TABLE IF NOT EXISTS baz;",
			rollback: []string{"DELETE FROM foo where 1", "DROP TABLE foo", "DROP TABLE baz"},
			rollbackScripts: "DELETE FROM foo where 1;\nDROP TABLE foo;\nDROP TABLE baz;",
		},
	}

	for _, tc := range tt {
	    t.Run(tc.name, func(t *testing.T) {
			m := Migration{
				Migrate:  tc.migrate,
				Rollback: tc.rollback,
			}

			assert.Equal(t, tc.migrateScripts, m.MigrateScripts())
			assert.Equal(t, tc.rollbackScripts, m.RollbackScripts())
	    })
	}
}

func Test_MigrationsCanBeSortedByVersion(t *testing.T) {
	m1 := &Migration{
		Version:  Version{Timestamp: "1596897167"},
		Name:     "Foo migration",
		Migrate:  []string{"CREATE foo"},
		Rollback: []string{"DROP foo"},
	}

	m2 := &Migration{
		Version:  Version{Timestamp: "1586897167"},
		Name:     "Bar migration",
		Migrate:  []string{"CREATE bar"},
		Rollback: []string{"DROP bar"},
	}

	m3 := &Migration{
		Version:  Version{Timestamp: "1597897167"},
		Name:     "Baz migration",
		Migrate:  []string{"CREATE baz"},
		Rollback: []string{"DROP baz"},
	}

	m4 := &Migration{
		Version:  Version{Timestamp: "1577897167"},
		Name:     "FooBaz migration",
		Migrate:  []string{"CREATE foo_baz"},
		Rollback: []string{"DROP foo_baz"},
	}

	var migrations = Migrations{m1, m2, m3, m4}

	sort.Sort(migrations)

	assert.Equal(t, migrations[0].Name, m4.Name)
	assert.Equal(t, migrations[1].Name, m2.Name)
	assert.Equal(t, migrations[2].Name, m1.Name)
	assert.Equal(t, migrations[3].Name, m3.Name)
}
