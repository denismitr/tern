package migration

import (
	"bytes"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidVersionFormat = errors.New("invalid version format")

type (
	VersionFormat string

	Version struct {
		Format     VersionFormat
		Value      string
		MigratedAt time.Time
	}

	Migration struct {
		Key      string
		Name     string
		Version  Version
		Migrate  []string
		Rollback []string
	}

	ClockFunc func() time.Time
	Factory func() (*Migration, error)
)

const (
	TimestampFormat VersionFormat = "timestamp"
	DatetimeFormat  VersionFormat = "datetime"
	AnyFormat       VersionFormat = "any"

	MaxTimestampLength = 12
	MinTimestampLength = 9
)

func NewMigrationFromDB(version string, migratedAt time.Time, name string) Factory {
	return func() (*Migration, error) {
		m := &Migration{
			Key:  CreateKeyFromTimestampAndName(version, name),
			Name: name,
			Version: Version{
				Value:      version,
				MigratedAt: migratedAt,
			},
		}

		if err := SetVersionFormat(m); err != nil {
			return nil, err
		}

		return m, nil
	}
}

func NewMigrationFromFile(
	key string,
	name string,
	version Version,
	migrate string,
	rollback string,
) Factory {
	return func() (*Migration, error) {
		return &Migration{
			Key:      key,
			Name:     name,
			Version:  version,
			Migrate:  []string{migrate},
			Rollback: []string{rollback},
		}, nil
	}
}

func New(version, name string, migrate, rollback []string) Factory {
	return func() (*Migration, error) {
		m := &Migration{
			Key:  CreateKeyFromTimestampAndName(version, name),
			Name: name,
			Version: Version{
				Value: version,
			},
			Migrate:  migrate,
			Rollback: rollback,
		}

		if err := SetVersionFormat(m); err != nil {
			return nil, err
		}

		return m, nil
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

func NewMigrations(factories ...Factory) (Migrations, error) {
	migrations := make(Migrations, len(factories))

	for i := range factories {
		m, err := factories[i]()
		if err != nil {
			return nil, err
		}

		migrations[i] = m
	}

	return migrations, nil
}

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
	return m[i].Version.Value < m[j].Version.Value
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

func GenerateVersion(cf ClockFunc, vf VersionFormat) Version {
	var v Version

	v.Format = vf
	if v.Format == TimestampFormat {
		v.Value = strconv.Itoa(int(cf().Unix()))
	} else {
		v.Value = cf().Format("2006-01-02 15:04:05")
		v.Value = strings.ReplaceAll(v.Value, "-", "")
		v.Value = strings.ReplaceAll(v.Value, ":", "")
		v.Value = strings.ReplaceAll(v.Value, " ", "")
	}

	return v
}

func SetVersionFormat(m *Migration) error {
	if len(m.Version.Value) > MinTimestampLength && len(m.Version.Value) <= MaxTimestampLength {
		m.Version.Format = TimestampFormat
	} else if len(m.Version.Value) > MaxTimestampLength {
		m.Version.Format = DatetimeFormat
	} else {
		return errors.Wrapf(ErrInvalidVersionFormat, "%s", m.Version.Value)
	}

	return nil
}

func InVersions(version Version, versions []Version) bool {
	for _, v := range versions {
		if v.Value == version.Value {
			return true
		}
	}

	return false
}
