package txm

import "github.com/jmoiron/sqlx"

type TxManager struct {
	db *sqlx.DB
}
