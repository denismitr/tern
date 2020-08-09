package tern

import (
	"context"
	"database/sql"
	"fmt"
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

func Test_ItCanMigrateEverythingToSqliteDBFromAGivenFolder(t *testing.T) {
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

	tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}

	versions, err := readVersions(tx, "migrations")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		t.Fatal(err)
	}

	assert.Len(t, versions, 2)

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(mysqlDropMigrationsSchema, "migrations")); err != nil {
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}


