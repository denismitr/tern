package database

import (
	"github.com/pkg/errors"
	"time"
)

var ErrInvalidVersionFormat = errors.New("invalid version format")
var ErrInvalidMigrationName = errors.New("invalid migration name")
var ErrInvalidMigrationInput = errors.New("invalid migration input")

type (
	Batch uint
	ID    uint

	Version struct {
		Name       string
		Batch      Batch
		ID         ID
		MigratedAt time.Time
	}

	Migration struct {
		Version  Version
		Migrate  []string
		Rollback []string
	}
)

type Migrations []Migration

func (m Migrations) Len() int {
	return len(m)
}

func (m Migrations) Less(i, j int) bool {
	return m[i].Version.ID < m[j].Version.ID
}

func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func InVersions(version Version, versions []Version) bool {
	for _, v := range versions {
		if v.ID == version.ID {
			return true
		}
	}

	return false
}
