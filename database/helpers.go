package database

import (
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/migration"
)

func inVersions(version migration.Version, versions []migration.Version) bool {
	for _, v := range versions {
		if v.Timestamp == version.Timestamp {
			return true
		}
	}

	return false
}

func readVersions(tx *sql.Tx, migrationsTable string) ([]migration.Version, error) {
	rows, err := tx.Query(fmt.Sprintf("SELECT version FROM %s", migrationsTable))
	if err != nil {
		return nil, err
	}

	var result []migration.Version

	for rows.Next() {
		var timestamp string

		if err := rows.Scan(&timestamp); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, migration.Version{Timestamp: timestamp})
	}

	return result, nil
}
