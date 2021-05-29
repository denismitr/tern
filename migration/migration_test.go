package migration

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func Test_MigrationCanAssembleScriptsInOne(t *testing.T) {
	t.Parallel()

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
		tc := tc
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
		Version:  Version{Value: "1596897167"},
		Name:     "Foo migration",
		Migrate:  []string{"CREATE foo"},
		Rollback: []string{"DROP foo"},
	}

	m2 := &Migration{
		Version:  Version{Value: "1586897167"},
		Name:     "Bar migration",
		Migrate:  []string{"CREATE bar"},
		Rollback: []string{"DROP bar"},
	}

	m3 := &Migration{
		Version:  Version{Value: "1597897167"},
		Name:     "Baz migration",
		Migrate:  []string{"CREATE baz"},
		Rollback: []string{"DROP baz"},
	}

	m4 := &Migration{
		Version:  Version{Value: "1577897167"},
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

func Test_GenerateVersion_InTimestampFormat(t *testing.T) {
	cf := func() time.Time {
		t, _ := time.Parse(time.RFC850, "Monday, 02-Jan-19 17:18:19 UTC")
		return t
	}

	versionTs := GenerateVersion(cf, TimestampFormat)
	assert.Equal(t, versionTs.Format, TimestampFormat)
	assert.Equal(t, "1546449499", versionTs.Value)

	versionDt := GenerateVersion(cf, DatetimeFormat)
	assert.Equal(t, versionDt.Format, DatetimeFormat)
	assert.Equal(t, "20190102171819", versionDt.Value)
}

func TestInVersions(t *testing.T) {
	tt := []struct{
		name string
		version Version
		versions []Version
		expected bool
	}{
		{
			name: "one version and one match in timestamp format",
			version: Version{
				Format: TimestampFormat,
				Value: "1546449499",
			},
			versions: []Version{
				{Value: "1546449499", Format: TimestampFormat},
			},
			expected: true,
		},
		{
			name: "two versions and no match in timestamp format",
			version: Version{
				Format: TimestampFormat,
				Value: "1546449499",
			},
			versions: []Version{
				{Value: "1546449498", Format: TimestampFormat},
				{Value: "1546446497", Format: TimestampFormat},
			},
			expected: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result := InVersions(tc.version, tc.versions)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTimestampVersion(t *testing.T) {
	t.Parallel()

	validInputs := []struct{
		t string
	}{
		{"154644949"},
		{"1546449498"},
		{"15464494912"},
		{"154644949125"},
	}

	for _, tc := range validInputs {
		t.Run(tc.t, func(t *testing.T) {
			result, err := Timestamp(tc.t)()
			require.NoError(t, err)
			assert.Equal(t, tc.t, result.Value)
			assert.Equal(t, TimestampFormat, result.Format)
			assert.True(t, result.MigratedAt.IsZero())
		})
	}

	invalidInputs := []struct{
		t string
	}{
		{""},
		{"foo"},
		{"1543barbaz"},
		{"15434566"},
		{"15464494945678"},
	}

	for _, tc := range invalidInputs {
		t.Run(tc.t, func(t *testing.T) {
			{
				result, err := Timestamp(tc.t)()
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidVersionFormat))
				assert.Equal(t, "", result.Value)
			}
		})
	}
}

func TestDateTimeVersion(t *testing.T) {
	t.Parallel()

	validInputs := []struct{
		year int
		month int
		day int
		hour int
		minute int
		second int
		exp string
	}{
		{
			year: 2019,
			month: 1,
			day: 30,
			hour: 10,
			minute: 5,
			second: 59,
			exp: "20190130100559",
		},
	}

	for _, tc := range validInputs {
		t.Run(tc.exp, func(t *testing.T) {
			result, err := DateTime(tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second)()
			require.NoError(t, err)
			assert.Equal(t, tc.exp, result.Value)
			assert.Equal(t, DatetimeFormat, result.Format)
			assert.True(t, result.MigratedAt.IsZero())
		})
	}
}

func TestNumberFormat(t *testing.T) {
	t.Parallel()

	validInputs := []struct{
		n uint
		exp string
	}{
		{n: 1, exp: "00000000000001"},
		{n: 9, exp: "00000000000009"},
		{n: 879, exp: "00000000000879"},
		{n: 123_456_789_000_00, exp: "12345678900000"},
	}

	for _, tc := range validInputs {
		t.Run(tc.exp, func(t *testing.T) {
			v, err := Number(tc.n)()
			require.NoError(t, err)
			assert.Equal(t, tc.exp, v.Value)
			assert.Equal(t, NumberFormat, v.Format)
			assert.True(t, v.MigratedAt.IsZero())
		})
	}

	invalidInputs := []struct{
		n uint
	}{
		{n: 123_456_789_000_000},
	}

	for _, tc := range invalidInputs {
		t.Run(fmt.Sprintf("%d", tc.n), func(t *testing.T) {
			_, err := Number(tc.n)()
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidVersionFormat))
		})
	}
}

func TestNew(t *testing.T) {
	t.Run("from number", func(t *testing.T) {
		f := New(Number(1), "foo", []string{"SELECT 1"}, []string{})
		m, err := f()

		require.NoError(t, err)
		assert.Equal(t, "foo", m.Name)
		assert.Equal(t, "00000000000001_foo", m.Key)
		assert.Equal(t, "00000000000001", m.Version.Value)
		assert.Equal(t, NumberFormat, m.Version.Format)
	})

}