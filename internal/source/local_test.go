package source

import (
	"context"
	"fmt"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/denismitr/tern/v3/migration"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const defaultMysqlStubs = "./stubs"

func Test_SingleMigrationCanBeReadFromLocalFile(t *testing.T) {
	folder, err := filepath.Abs(defaultMysqlStubs)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
	assert.NoError(t, err)

	key := "1596897167_create_foo_table"

	m, err := c.readOne(key)

	assert.NoError(t, err)
	assert.Equal(t, "1596897167", m.Version.Value)
	assert.Equal(t, "Create foo table", m.Name)
	assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"}, m.Migrate)
	assert.Equal(t, []string{"DROP TABLE IF EXISTS foo;"}, m.Rollback)
}

func Test_ConvertLocalFolder(t *testing.T) {
	folder, err := filepath.Abs(defaultMysqlStubs)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
	assert.NoError(t, err)

	t.Run("all migrations can be read from local folder", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		migrations, err := c.Select(ctx, Filter{})

		assert.NoError(t, err)
		assert.Len(t, migrations, 3)

		assert.Equal(t, "Create foo table", migrations[0].Name)
		assert.Equal(t, "1596897167", migrations[0].Version.Value)
		assert.Equal(t, "1596897167_create_foo_table", migrations[0].Key)
		assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"}, migrations[0].Migrate)
		assert.Equal(t, []string{"DROP TABLE IF EXISTS foo;"}, migrations[0].Rollback)

		assert.Equal(t, "Create bar table", migrations[1].Name)
		assert.Equal(t, "1596897188", migrations[1].Version.Value)
		assert.Equal(t, "1596897188_create_bar_table", migrations[1].Key)
		assert.Equal(t, []string{"CREATE TABLE bar (uid binary(16) PRIMARY KEY) ENGINE=INNODB;"}, migrations[1].Migrate)
		assert.Equal(t, []string{"DROP TABLE IF EXISTS bar;"}, migrations[1].Rollback)

		assert.Equal(t, "Create baz table", migrations[2].Name)
		assert.Equal(t, "1597897177", migrations[2].Version.Value)
		assert.Equal(t, "1597897177_create_baz_table", migrations[2].Key)
		assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS baz (uid binary(16) PRIMARY KEY, name varchar(10), length INT NOT NULL) ENGINE=INNODB;"}, migrations[2].Migrate)
		assert.Equal(t, []string{"DROP TABLE IF EXISTS baz;"}, migrations[2].Rollback)
	})

	t.Run("specified migrations can be read from local folder", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		migrations, err := c.Select(
			ctx,
			Filter{Versions: []migration.Order{{Value: "1596897188"}, {Value: "1597897177"}}},
		)

		require.NoError(t, err)
		require.Len(t, migrations, 2)

		assert.Equal(t, "Create bar table", migrations[0].Name)
		assert.Equal(t, "1596897188", migrations[0].Version.Value)
		assert.Equal(t, "1596897188_create_bar_table", migrations[0].Key)
		assert.Equal(t, []string{"CREATE TABLE bar (uid binary(16) PRIMARY KEY) ENGINE=INNODB;"}, migrations[0].Migrate)
		assert.Equal(t, []string{"DROP TABLE IF EXISTS bar;"}, migrations[0].Rollback)

		assert.Equal(t, "Create baz table", migrations[1].Name)
		assert.Equal(t, "1597897177", migrations[1].Version.Value)
		assert.Equal(t, "1597897177_create_baz_table", migrations[1].Key)
		assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS baz (uid binary(16) PRIMARY KEY, name varchar(10), length INT NOT NULL) ENGINE=INNODB;"}, migrations[1].Migrate)
		assert.Equal(t, []string{"DROP TABLE IF EXISTS baz;"}, migrations[1].Rollback)
	})
}

func Test_VersionCanBeExtractedFromKey(t *testing.T) {
	t.Parallel()

	valid := []struct {
		in  string
		out string
	}{
		{in: "1596897167_create_foo_table", out: "1596897167"},
		{in: "1496897167_create_foo_table", out: "1496897167"},
		{in: "1496897167", out: "1496897167"},
		{in: "315360001", out: "315360001"},
		{in: "14968971672", out: "14968971672"},
	}

	invalid := []struct {
		in  string
		err error
	}{
		{in: "M1596897167_create_foo_table", err: ErrInvalidTimestamp},
		{in: "15968V97167_create_foo_table", err: ErrInvalidTimestamp},
		{in: "_foo", err: ErrInvalidTimestamp},
		{in: "125A3656456566_foo", err: ErrInvalidTimestamp},
	}

	folder, err := filepath.Abs(defaultMysqlStubs)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
	assert.NoError(t, err)

	for _, tc := range valid {
		tc := tc

		t.Run(fmt.Sprintf("valid-timestanps-%s", tc.in), func(t *testing.T) {
			out, err := c.extractVersionFromKey(tc.in)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, out.Value)
		})
	}

	for _, tc := range invalid {
		tc := tc

		t.Run(fmt.Sprintf("invalid-timestanps-%s", tc.in), func(t *testing.T) {
			out, err := c.extractVersionFromKey(tc.in)
			assert.Error(t, err)
			assert.True(t, errors.Is(tc.err, err))
			assert.Equal(t, "", out.Value)
		})
	}
}

func Test_MigrationNameCanBeExtractedFromKey(t *testing.T) {
	t.Parallel()

	tt := []struct {
		in  string
		out string
	}{
		{in: "1596897167_create_foo_table", out: "Create foo table"},
		{in: "1496897167_create_the_bar_2_table", out: "Create the bar 2 table"},
		{in: "1496897167_create_the_bar-2_table", out: "Create the bar-2 table"},
		{in: "1496897167_delete_some_field", out: "Delete some field"},
		{in: "3153600022_initial", out: "Initial"},
		{in: "14968971672", out: ""},
	}

	folder, err := filepath.Abs(defaultMysqlStubs)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
	assert.NoError(t, err)

	for _, tc := range tt {
		tc := tc
		t.Run(fmt.Sprintf("valid-timestanps-%s", tc.in), func(t *testing.T) {
			out := c.extractNameFromKey(tc.in)
			assert.Equal(t, tc.out, out)
		})
	}
}

func Test_ConvertPathToKey(t *testing.T) {
	t.Parallel()

	valid := []struct {
		in  string
		out string
	}{
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_bar_table.migrate.sql", out: "1596897188_create_bar_table"},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.rollback.sql", out: "1596897188_create_foo_table"},
		{in: "1596897188_create_foo_table.rollback.sql", out: "1596897188_create_foo_table"},
		{in: "./1596897188_create_foo_table.rollback.sql", out: "1596897188_create_foo_table"},
	}

	invalid := []struct {
		in  string
		err error
	}{
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_bar_table.sql", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.rollback", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.foo", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.", err: ErrNotAMigrationFile},
		{in: ".1596897188_create_foo_table.migrate.sql", err: ErrNotAMigrationFile},
	}

	for _, tc := range valid {
		tc := tc

		t.Run(tc.in, func(t *testing.T) {
			out, err := convertLocalFilePathToKey(tc.in)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, out)
		})
	}

	for _, tc := range invalid {
		tc := tc

		t.Run(tc.in, func(t *testing.T) {
			out, err := convertLocalFilePathToKey(tc.in)
			assert.Error(t, err)
			assert.True(t, errors.Is(err, tc.err))
			assert.Equal(t, "", out)
		})
	}
}

func TestLocalFileSource_Create(t *testing.T) {
	t.Run("create timestamp format with rollback", func(t *testing.T) {
		folder, err := filepath.Abs("/tmp")
		if err != nil {
			t.Fatal(err)
		}

		c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		m, err := c.Create("1596897188", "foo_bar", true)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		if m == nil {
			t.Fatal("how can migration be nil?")
		}

		require.FileExists(t, "/tmp/1596897188_foo_bar.migrate.sql")
		require.FileExists(t, "/tmp/1596897188_foo_bar.rollback.sql")

		if err := os.Remove("/tmp/1596897188_foo_bar.migrate.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}

		if err := os.Remove("/tmp/1596897188_foo_bar.rollback.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}
	})

	t.Run("create datetime format with rollback", func(t *testing.T) {
		folder, err := filepath.Abs("/tmp")
		if err != nil {
			t.Fatal(err)
		}

		c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.DatetimeFormat)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		m, err := c.Create("20210618163457", "fooBar", true)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		if m == nil {
			t.Fatal("how can migration be nil?")
		}

		require.FileExists(t, "/tmp/20210618163457_fooBar.migrate.sql")
		require.FileExists(t, "/tmp/20210618163457_fooBar.rollback.sql")

		if err := os.Remove("/tmp/20210618163457_fooBar.migrate.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}

		if err := os.Remove("/tmp/20210618163457_fooBar.rollback.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}
	})

	t.Run("create timestamp format without rollback", func(t *testing.T) {
		folder, err := filepath.Abs("/tmp")
		if err != nil {
			t.Fatal(err)
		}

		c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.TimestampFormat)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		m, err := c.Create("1596897188", "foo_bar", false)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		if m == nil {
			t.Fatal("how can migration be nil?")
		}

		assert.Equal(t, "foo_bar", m.Name)
		assert.Equal(t, "1596897188", m.Version.Value)
		assert.Equal(t, migration.TimestampFormat, m.Version.Format)

		require.FileExists(t, "/tmp/1596897188_foo_bar.migrate.sql")
		assert.NoFileExists(t, "/tmp/1596897188_foo_bar.rollback.sql")

		if err := os.Remove("/tmp/1596897188_foo_bar.migrate.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}
	})

	t.Run("create datetime format without rollback", func(t *testing.T) {
		folder, err := filepath.Abs("/tmp")
		if err != nil {
			t.Fatal(err)
		}

		c, err := NewLocalFSSource(folder, &logger.NullLogger{}, migration.DatetimeFormat)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		m, err := c.Create("20210618163457", "fooBar", false)
		if err != nil {
			t.Fatalf("unexpected error %+v", err)
		}

		if m == nil {
			t.Fatal("how can migration be nil?")
		}

		require.FileExists(t, "/tmp/20210618163457_fooBar.migrate.sql")
		require.NoFileExists(t, "/tmp/20210618163457_fooBar.rollback.sql")

		if err := os.Remove("/tmp/20210618163457_fooBar.migrate.sql"); err != nil {
			t.Errorf("unexpected error %+v", err)
		}
	})
}
