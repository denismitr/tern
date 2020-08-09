package tern

import (
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ExecutorCanBeCreatedFromDriver(t *testing.T) {
	db, err := sqlx.Open("mysql", "tern:secret@(tern_mysql:3306)/test_db?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	ex, err := createExecutor(db, "migrations")
	assert.NoError(t, err)
	assert.NotNil(t, ex)

	_, ok := ex.(*mysqlExecutor)
	assert.True(t, ok, "should be a value of mysqlExecutor")
}

func Test_ItWillReturnErrorOnUnsupportedDBDriver(t *testing.T) {
	db, err := sqlx.Open("sqlite3", "./test.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	ex, err := createExecutor(db, "migrations")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedDBDriver))
	assert.Nil(t, ex)
}
