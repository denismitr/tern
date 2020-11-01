package database

import (
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"time"
)

func inVersions(version migration.Version, versions []migration.Version) bool {
	for _, v := range versions {
		if v.Value == version.Value {
			return true
		}
	}

	return false
}

func readVersions(tx *sql.Tx, migrationsTable string) ([]migration.Version, error) {
	rows, err := tx.Query(fmt.Sprintf("SELECT version, %s FROM %s", MigratedAtColumn, migrationsTable))
	if err != nil {
		return nil, err
	}

	var result []migration.Version

	defer rows.Close()

	for rows.Next() {
		if errRows := rows.Err(); errRows != nil {
			if errRows != sql.ErrNoRows {
				return nil, errors.Wrap(errRows, "read migration versions iteration failed")
			} else {
				break
			}
		}

		var timestamp string
		var migratedAt time.Time
		if err := rows.Scan(&timestamp, &migratedAt); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, migration.Version{Value: timestamp, MigratedAt: migratedAt})
	}

	return result, nil
}
