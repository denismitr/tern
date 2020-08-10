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

func Test_MigratorCanBeInstantiated(t *testing.T) {
	db, err := sqlx.Open("mysql", "tern:secret@(tern_mysql:3306)/tern_db?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	m, err := NewMigrator(db)
	assert.NoError(t, err)
	assert.NotNil(t, m)
}

func Test_ItCanMigrateUpAndDownEverythingToMysqlDBFromAGivenFolder(t *testing.T) {
	db, err := sqlx.Open("mysql", "tern:secret@(127.0.0.1:33066)/tern_db?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	m, err := NewMigrator(db, UseLocalFolder("./stubs/valid/mysql"))
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
	defer cancel()

	err = m.Up(ctx)
	assert.NoError(t, err)

	gateway, err := newMysqlGateway(db, "migrations")
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

	if err := m.Down(ctx); err != nil {
		assert.NoError(t, err)
	}

	versionsAfterDown, err := gateway.readVersions(ctx)
	if err != nil {
		assert.NoError(t, err)
	}

	assert.Len(t, versionsAfterDown, 0)

	if err := gateway.dropMigrationsTable(ctx); err != nil {
		t.Fatal(err)
	}
}


