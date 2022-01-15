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

func Test_MigrationsCanBeSortedByVersion(t *testing.T) {
	t.Parallel()

	m1 := &Migration{
		Version:  Order{Value: "1596897167"},
		Name:     "Foo migration",
		Migrate:  []string{"CREATE foo"},
		Rollback: []string{"DROP foo"},
	}

	m2 := &Migration{
		Version:  Order{Value: "1586897167"},
		Name:     "Bar migration",
		Migrate:  []string{"CREATE bar"},
		Rollback: []string{"DROP bar"},
	}

	m3 := &Migration{
		Version:  Order{Value: "1597897167"},
		Name:     "Baz migration",
		Migrate:  []string{"CREATE baz"},
		Rollback: []string{"DROP baz"},
	}

	m4 := &Migration{
		Version:  Order{Value: "1577897167"},
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
		name     string
		version  Order
		versions []Order
		expected bool
	}{
		{
			name: "one version and one match in timestamp format",
			version: Order{
				Format: TimestampFormat,
				Value: "1546449499",
			},
			versions: []Order{
				{Value: "1546449499", Format: TimestampFormat},
			},
			expected: true,
		},
		{
			name: "two versions and no match in timestamp format",
			version: Order{
				Format: TimestampFormat,
				Value: "1546449499",
			},
			versions: []Order{
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
		{
			year: 2021,
			month: 12,
			day: 31,
			hour: 0,
			minute: 0,
			second: 1,
			exp: "20211231000001",
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

	invalidInputs := []struct{
		year int
		month int
		day int
		hour int
		minute int
		second int
	}{
		{
			year: 19,
			month: 1,
			day: 30,
			hour: 10,
			minute: 5,
			second: 59,
		},
		{
			year: 19564,
			month: 1,
			day: 30,
			hour: 10,
			minute: 5,
			second: 59,
		},
		{
			year: 1956,
			month: -1,
			day: 30,
			hour: 10,
			minute: 5,
			second: 59,
		},
		{
			year: 1956,
			month: 1,
			day: 30,
			hour: 10,
			minute: 5,
			second: -19,
		},
	}

	for _, tc := range invalidInputs {
		t.Run(fmt.Sprintf("%d-%d-%d-%d-%d-%d", tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second), func(t *testing.T) {
			_, err := DateTime(tc.year, tc.month, tc.day, tc.hour, tc.minute, tc.second)()
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidVersionFormat))
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
	t.Parallel()

	t.Run("from number", func(t *testing.T) {
		f := New(Number(1), "foo", []string{"SELECT 1"}, []string{})
		m, err := f()

		require.NoError(t, err)
		assert.Equal(t, "foo", m.Name)
		assert.Equal(t, "00000000000001_foo", m.Key)
		assert.Equal(t, "00000000000001", m.Version.Value)
		assert.Equal(t, NumberFormat, m.Version.Format)
	})

	t.Run("from datetime", func(t *testing.T) {
		f := New(DateTime(2021, 10, 30, 1, 10, 9), "foo", []string{"SELECT 1"}, []string{})
		m, err := f()

		require.NoError(t, err)
		assert.Equal(t, "foo", m.Name)
		assert.Equal(t, "20211030011009_foo", m.Key)
		assert.Equal(t, "20211030011009", m.Version.Value)
		assert.Equal(t, DatetimeFormat, m.Version.Format)
	})

	t.Run("from timestamp", func(t *testing.T) {
		f := New(Timestamp("15464494912"), "foo", []string{"SELECT 1"}, []string{})
		m, err := f()

		require.NoError(t, err)
		assert.Equal(t, "foo", m.Name)
		assert.Equal(t, "15464494912_foo", m.Key)
		assert.Equal(t, "15464494912", m.Version.Value)
		assert.Equal(t, TimestampFormat, m.Version.Format)
	})
}

func TestVersionFromString(t *testing.T) {
	t.Parallel()

	validInputs := []struct{
		in string
		format VersionFormat
	}{
		{in: "15464494912", format: TimestampFormat},
		{in: "00000000000001", format: NumberFormat},
		{in: "20190130100559", format: DatetimeFormat},
	}

	for _, tc := range validInputs {
		t.Run(tc.in, func(t *testing.T) {
			v, err := VersionFromString(tc.in)
			require.NoError(t, err)
			assert.Equal(t, v.Value, tc.in)
			assert.Equal(t, tc.format, v.Format)
		})
	}
}