package migration

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func Test_MigrationsCanBeSortedByVersion(t *testing.T) {
	m1 := Migration{
		Version: Version{Timestamp: "1596897167"},
		Name:    "Foo migration",
		Up:      "CREATE foo",
		Down:    "DROP foo",
	}

	m2 := Migration{
		Version: Version{Timestamp: "1586897167"},
		Name:    "Bar migration",
		Up:      "CREATE bar",
		Down:    "DROP bar",
	}

	m3 := Migration{
		Version: Version{Timestamp: "1597897167"},
		Name:    "Baz migration",
		Up:      "CREATE baz",
		Down:    "DROP baz",
	}

	m4 := Migration{
		Version: Version{Timestamp: "1577897167"},
		Name:    "FooBaz migration",
		Up:      "CREATE foo_baz",
		Down:    "DROP foo_baz",
	}

	var migrations = Migrations{m1, m2, m3, m4}

	sort.Sort(migrations)

	assert.Equal(t, migrations[0].Name, m4.Name)
	assert.Equal(t, migrations[1].Name, m2.Name)
	assert.Equal(t, migrations[2].Name, m1.Name)
	assert.Equal(t, migrations[3].Name, m3.Name)
}
