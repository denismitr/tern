package database

import (
	"github.com/denismitr/tern/v2/migration"
)

func InVersions(version migration.Version, versions []migration.Version) bool {
	for _, v := range versions {
		if v.Value == version.Value {
			return true
		}
	}

	return false
}
