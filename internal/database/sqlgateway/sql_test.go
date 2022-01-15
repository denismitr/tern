package sqlgateway

import (
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSqliteGateway(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		connector := RetryingConnector{}
		g, closer := NewSqliteGateway(&connector, &SqliteOptions{})

		require.NotNil(t, closer)
		require.NotNil(t, g)

		s, ok := g.schema.(*sqliteSchemaV1)
		require.True(t, ok)

		assert.Equal(t, "migrations", s.migrationsTable)
		assert.Equal(t, "migrated_at", s.migratedAtColumn)
	})

	t.Run("custom options", func(t *testing.T) {
		connector := RetryingConnector{}
		g, closer := NewSqliteGateway(&connector, &SqliteOptions{
			database.CommonOptions{
				MigrationsTable:  "foo",
				MigratedAtColumn: "created_at",
			},
		})

		require.NotNil(t, closer)
		require.NotNil(t, g)

		s, ok := g.schema.(*sqliteSchemaV1)
		require.True(t, ok)

		assert.Equal(t, "foo", s.migrationsTable)
		assert.Equal(t, "created_at", s.migratedAtColumn)
	})
}

func TestNewMySQLGateway(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		connector := RetryingConnector{}
		g, closer := NewMySQLGateway(&connector, &MySQLOptions{})

		require.NotNil(t, closer)
		require.NotNil(t, g)

		s, ok := g.schema.(*mysqlSchemaV1)
		require.True(t, ok)

		assert.Equal(t, "migrations", s.migrationsTable)
		assert.Equal(t, "migrated_at", s.migratedAtColumn)
	})

	t.Run("custom options", func(t *testing.T) {
		connector := RetryingConnector{}
		g, closer := NewMySQLGateway(&connector, &MySQLOptions{
			CommonOptions: database.CommonOptions{
				MigrationsTable:  "foo",
				MigratedAtColumn: "created_at",
			},
			LockKey: "foobar",
			LockFor: 2,
		})

		require.NotNil(t, closer)
		require.NotNil(t, g)

		s, ok := g.schema.(*mysqlSchemaV1)
		require.True(t, ok)

		assert.Equal(t, "foo", s.migrationsTable)
		assert.Equal(t, "created_at", s.migratedAtColumn)
	})
}
