package migration

import (
	"bytes"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidTimestamp = errors.New("invalid timestamp in migration filename")

type (
	VersionFormat int

	Version struct {
		Format    VersionFormat
		Timestamp string
		CreatedAt time.Time
	}

	Migration struct {
		Key      string
		Name     string
		Version  Version
		Migrate  []string
		Rollback []string
	}

	ClockFunc func() time.Time
)

const (
	TimestampFormat VersionFormat = iota
	DatetimeFormat
)

func NewMigrationFromDB(timestamp string, createdAt time.Time, name string) *Migration {
	return &Migration{
		Key:  CreateKeyFromTimestampAndName(timestamp, name),
		Name: name,
		Version: Version{
			Timestamp: timestamp,
			CreatedAt: createdAt,
		},
	}
}

func NewMigrationFromFile(
	key string,
	name string,
	version Version,
	migrate string,
	rollback string,
) (*Migration, error) {
	return &Migration{
		Key:      key,
		Name:     name,
		Version:  version,
		Migrate:  []string{migrate},
		Rollback: []string{rollback},
	}, nil
}

func New(timestamp, name string, migrate, rollback []string) *Migration {
	return &Migration{
		Key:  CreateKeyFromTimestampAndName(timestamp, name),
		Name: name,
		Version: Version{
			Timestamp: timestamp,
		},
		Migrate:  migrate,
		Rollback: rollback,
	}
}

func (m *Migration) MigrateScripts() string {
	var ms bytes.Buffer

	for i := range m.Migrate {
		ms.WriteString(m.Migrate[i])

		if !strings.HasSuffix(m.Migrate[i], ";") {
			ms.WriteString(";")
		}

		if i < len(m.Migrate)-1 {
			ms.WriteString("\n")
		}
	}

	return ms.String()
}

func (m *Migration) RollbackScripts() string {
	var ms bytes.Buffer

	for i := range m.Rollback {
		ms.WriteString(m.Rollback[i])

		if !strings.HasSuffix(m.Rollback[i], ";") {
			ms.WriteString(";")
		}

		if i < len(m.Rollback)-1 {
			ms.WriteString("\n")
		}
	}

	return ms.String()
}

type Migrations []*Migration

func (m Migrations) Keys() (result []string) {
	for i := range m {
		result = append(result, m[i].Key)
	}
	return result
}

func (m Migrations) Len() int {
	return len(m)
}

func (m Migrations) Less(i, j int) bool {
	return m[i].Version.Timestamp < m[j].Version.Timestamp
}

func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func CreateKeyFromTimestampAndName(timestamp, name string) string {
	var result bytes.Buffer
	result.WriteString(timestamp)
	result.WriteString("_")
	result.WriteString(strings.Replace(strings.ToLower(name), " ", "_", -1))
	return result.String()
}

func GenerateVersion(cf ClockFunc, f VersionFormat) Version {
	var v Version

	v.Format = f
	if v.Format == TimestampFormat {
		v.Timestamp = strconv.Itoa(int(cf().Unix()))
	} else {
		v.Timestamp = cf().Format("2006-01-02 15:04:05")
		v.Timestamp = strings.ReplaceAll(v.Timestamp, "-", "")
		v.Timestamp = strings.ReplaceAll(v.Timestamp, ":", "")
		v.Timestamp = strings.ReplaceAll(v.Timestamp, " ", "")
	}

	return v
}
