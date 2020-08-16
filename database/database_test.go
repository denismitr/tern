package database


import (
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_MySQLGateway_CanBeCreatedFromDriver(t *testing.T) {
	db, err := sqlx.Open("mysql", "tern:secret@(127.0.0.1:33066)/tern_db?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	gateway, err := Create(db, "migrations")
	assert.NoError(t, err)
	assert.NotNil(t, gateway)

	defer gateway.Close()

	_, ok := gateway.(*MySQL)
	assert.True(t, ok, "should be a value of mysqlGateway")
}

func Test_ItWillReturnErrorOnUnsupportedDBDriver(t *testing.T) {
	db, err := sqlx.Open("sqlite3", "./test.sqlite")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	gateway, err := Create(db, "migrations")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedDBDriver))
	assert.Nil(t, gateway)
}
