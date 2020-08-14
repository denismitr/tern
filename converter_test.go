package tern

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
	"time"
)

const defaultMysqlStubs = "./stubs/valid/mysql"

func Test_SingleMigrationCanBeReadFromLocalFile(t *testing.T) {
	folder, err := filepath.Abs(defaultMysqlStubs)
	if err != nil {
		t.Fatal(err)
	}

	c := localFSConverter{folder: folder}
	key := "1596897167_create_foo_table"


	m, err := c.readOne(key)

	assert.NoError(t, err)
	assert.Equal(t, "1596897167", m.Version)
	assert.Equal(t, "Create foo table", m.Name)
	assert.Equal(t, "CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;", m.Up)
	assert.Equal(t, "DROP TABLE IF EXISTS foo;", m.Down)
}

func Test_ConvertLocalFolder(t *testing.T) {
	t.Run("all migrations can be read from local folder", func(t *testing.T) {
		folder, err := filepath.Abs(defaultMysqlStubs)
		if err != nil {
			t.Fatal(err)
		}

		c := localFSConverter{folder: folder}

		ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
		defer cancel()

		migrations, err := c.Convert(ctx, filter{})

		assert.NoError(t, err)
		assert.Len(t, migrations, 3)

		assert.Equal(t, "Create foo table", migrations[0].Name)
		assert.Equal(t, "1596897167", migrations[0].Version)
		assert.Equal(t, "1596897167_create_foo_table", migrations[0].Key)
		assert.Equal(t, "CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;", migrations[0].Up)
		assert.Equal(t, "DROP TABLE IF EXISTS foo;", migrations[0].Down)

		assert.Equal(t, "Create bar table", migrations[1].Name)
		assert.Equal(t, "1596897188", migrations[1].Version)
		assert.Equal(t, "1596897188_create_bar_table", migrations[1].Key)
		assert.Equal(t, "CREATE TABLE bar (uid binary(16) PRIMARY KEY) ENGINE=INNODB;", migrations[1].Up)
		assert.Equal(t, "DROP TABLE IF EXISTS bar;", migrations[1].Down)

		assert.Equal(t, "Create baz table", migrations[2].Name)
		assert.Equal(t, "1597897177", migrations[2].Version)
		assert.Equal(t, "1597897177_create_baz_table", migrations[2].Key)
		assert.Equal(t, "CREATE TABLE IF NOT EXISTS baz (uid binary(16) PRIMARY KEY, name varchar(10), length INT NOT NULL) ENGINE=INNODB;", migrations[2].Up)
		assert.Equal(t, "DROP TABLE IF EXISTS baz;", migrations[2].Down)
	})

	t.Run("specified migrations can be read from local folder", func(t *testing.T) {
		folder, err := filepath.Abs(defaultMysqlStubs)
		if err != nil {
			t.Fatal(err)
		}

		c := localFSConverter{folder: folder}

		ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
		defer cancel()

		migrations, err := c.Convert(ctx, filter{keys: []string{"1596897188_create_bar_table", "1597897177_create_baz_table"}})

		assert.NoError(t, err)
		assert.Len(t, migrations, 2)

		assert.Equal(t, "Create bar table", migrations[0].Name)
		assert.Equal(t, "1596897188", migrations[0].Version)
		assert.Equal(t, "1596897188_create_bar_table", migrations[0].Key)
		assert.Equal(t, "CREATE TABLE bar (uid binary(16) PRIMARY KEY) ENGINE=INNODB;", migrations[0].Up)
		assert.Equal(t, "DROP TABLE IF EXISTS bar;", migrations[0].Down)

		assert.Equal(t, "Create baz table", migrations[1].Name)
		assert.Equal(t, "1597897177", migrations[1].Version)
		assert.Equal(t, "1597897177_create_baz_table", migrations[1].Key)
		assert.Equal(t, "CREATE TABLE IF NOT EXISTS baz (uid binary(16) PRIMARY KEY, name varchar(10), length INT NOT NULL) ENGINE=INNODB;", migrations[1].Up)
		assert.Equal(t, "DROP TABLE IF EXISTS baz;", migrations[1].Down)
	})
}

func Test_VersionCanBeExtractedFromKey(t *testing.T) {
	valid := []struct {
		in  string
		out string
	}{
		{in: "1596897167_create_foo_table", out: "1596897167"},
		{in: "1496897167_create_foo_table", out: "1496897167"},
		{in: "1496897167", out: "1496897167"},
		{in: "31536000", out: "31536000"},
		{in: "14968971672", out: "14968971672"},
	}

	invalid := []struct {
		in string
		err error
	}{
		{in: "M1596897167_create_foo_table", err:  ErrInvalidTimestamp},
		{in: "15968V97167_create_foo_table", err:  ErrInvalidTimestamp},
		{in: "_foo", err:  ErrInvalidTimestamp},
		{in: "1253656456566_foo", err:  ErrInvalidTimestamp},
	}

	for _, tc := range valid {
		t.Run(fmt.Sprintf("valid-timestanps-%s", tc.in), func(t *testing.T) {
			out, err := extractVersionFromKey(tc.in, versionRegexp)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, out)
		})
	}

	for _, tc := range invalid {
		t.Run(fmt.Sprintf("invalid-timestanps-%s", tc.in), func(t *testing.T) {
			out, err := extractVersionFromKey(tc.in, versionRegexp)
			assert.Error(t, err)
			assert.True(t, errors.Is(tc.err, err))
			assert.Equal(t, "", out)
		})
	}
}

func Test_MigrationNameCanBeExtractedFromKey(t *testing.T) {
	tt := []struct {
		in  string
		out string
	}{
		{in: "1596897167_create_foo_table", out: "Create foo table"},
		{in: "1496897167_create_the_bar_2_table", out: "Create the bar 2 table"},
		{in: "1496897167_create_the_bar-2_table", out: "Create the bar-2 table"},
		{in: "1496897167_delete_some_field", out: "Delete some field"},
		{in: "31536000_initial", out: "Initial"},
		{in: "14968971672", out: ""},
	}

	for _, tc := range tt {
		t.Run(fmt.Sprintf("valid-timestanps-%s", tc.in), func(t *testing.T) {
			out := extractNameFromKey(tc.in, nameRegexp)
			assert.Equal(t, tc.out, out)
		})
	}
}

func Test_ConvertPathToKey(t *testing.T) {
	valid := []struct{
		in string
		out string
	}{
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_bar_table.up.sql", out: "1596897188_create_bar_table"},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.down.sql", out: "1596897188_create_foo_table"},
		{in: "1596897188_create_foo_table.down.sql", out: "1596897188_create_foo_table"},
		{in: "./1596897188_create_foo_table.down.sql", out: "1596897188_create_foo_table"},
	}

	invalid := []struct{
		in string
		err error
	}{
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_bar_table.sql", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.down", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.foo", err: ErrNotAMigrationFile},
		{in: "/home/vagrant/code/migrations/mysql/1596897188_create_foo_table.", err: ErrNotAMigrationFile},
		{in: ".1596897188_create_foo_table.up.sql", err: ErrNotAMigrationFile},
	}

	for _, tc := range valid {
		t.Run(tc.in, func(t *testing.T) {
			out, err := convertLocalFilePathToKey(tc.in)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, out)
		})
	}

	for _, tc := range invalid {
		t.Run(tc.in, func(t *testing.T) {
			out, err := convertLocalFilePathToKey(tc.in)
			assert.Error(t, err)
			assert.True(t, errors.Is(err, tc.err))
			assert.Equal(t, "", out)
		})
	}
}

