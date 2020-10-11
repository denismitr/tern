package database

import (
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/migration"
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
	rows, err := tx.Query(fmt.Sprintf("SELECT version, migrated_at FROM %s", migrationsTable))
	if err != nil {
		return nil, err
	}

	var result []migration.Version

	for rows.Next() {
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
