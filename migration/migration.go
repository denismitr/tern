package migration

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidVersionFormat = errors.New("invalid version format")
var ErrInvalidMigrationName = errors.New("invalid migration name")
var ErrInvalidMigrationInput = errors.New("invalid migration input")

type (
	Batch uint
	Version uint

	Migration struct {
		Name string
		Batch Batch
		Version  Version
		Migrate  []string
		Rollback []string
		MigratedAt time.Time
	}

	ClockFunc func() time.Time
	Factory   func() (*Migration, error)
)

var timestampRx = regexp.MustCompile(fmt.Sprintf(`^\d{%d,%d}$`, MinTimestampLength, MaxTimestampLength))
var datetimeRx = regexp.MustCompile(`^[12]\d{13}$`)

func Timestamp(t string) VersionFactory {
	return func() (Version, error) {
		var result Version
		if len(t) < MinTimestampLength || len(t) > MaxTimestampLength {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "timestamp [%s] length is invalid", t)
		}

		if !timestampRx.MatchString(t) {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "timestamp [%s] should contain only digits", t)
		}

		result.Value = t
		result.Format = TimestampFormat
		return result, nil
	}
}

func DateTime(year, month, day, hour, minute, second int) VersionFactory {
	return func() (Version, error) {
		var result Version

		if year < 1900 || year > 2200 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "year [%d] must be between 1900 and 2200", year)
		}

		y := strconv.Itoa(year)
		if len(y) != 4 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "year [%d] must consist of 4 digits", year)
		}

		if month < 1 || month > 12 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "month [%d] must be between 1 and 12", month)
		}

		m := strconv.Itoa(month)
		if len(m) == 1 {
			m = "0" + m
		}

		if day < 1 || day > 31 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "day [%d] must be between 1 and 31", day)
		}

		d := strconv.Itoa(day)
		if len(d) == 1 {
			d = "0" + d
		}

		if hour < 0 || hour > 23 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "hour [%d] must be between 0 and 23", hour)
		}

		h := strconv.Itoa(hour)
		if len(h) == 1 {
			h = "0" + h
		}

		if minute < 0 || minute > 59 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "minute [%d] must be between 0 and 59", minute)
		}

		min := strconv.Itoa(minute)
		if len(min) == 1 {
			min = "0" + min
		}

		if second < 0 || second > 59 {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "second [%d] must be between 0 and 59", second)
		}

		sec := strconv.Itoa(second)
		if len(sec) == 1 {
			sec = "0" + sec
		}

		result.Value = y + m + d + h + min + sec
		result.Format = DatetimeFormat

		return result, nil
	}
}

func Number(n uint) VersionFactory {
	return func() (Version, error) {
		var result Version

		s := strconv.Itoa(int(n))
		if len(s) > MaxVersionLen {
			return result, errors.Wrapf(ErrInvalidVersionFormat, "number can")
		}

		leftPad := MaxVersionLen - len(s)
		if leftPad > 0 {
			s = strings.Repeat("0", leftPad) + s
		}

		result.Value = s
		result.Format = NumberFormat
		return result, nil
	}
}

func NewMigrationFromDB(version string, migratedAt time.Time, name string) Factory {
	return func() (*Migration, error) {
		m := &Migration{
			Key:  CreateKeyFromVersionAndName(version, name),
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

type VersionFactory func() (Version, error)

func New(vf VersionFactory, name string, migrate, rollback []string) Factory {
	return func() (*Migration, error) {
		v, err := vf()
		if err != nil {
			return nil, err
		}

		if name == "" {
			return nil, errors.Wrap(ErrInvalidMigrationName, "migration name cannot be empty")
		}

		if len(migrate) == 0 && len(rollback) == 0 {
			return nil, errors.Wrap(
				ErrInvalidMigrationInput,
				"migration list and rollback list cannot be both empty")
		}

		m := &Migration{
			Key:      CreateKeyFromVersionAndName(v.Value, name),
			Name:     name,
			Version:  v,
			Migrate:  migrate,
			Rollback: rollback,
		}

		return m, nil
	}
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

func CreateKeyFromVersionAndName(v, name string) string {
	var result bytes.Buffer
	result.WriteString(v)
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

func VersionFromString(s string) (Version, error) {
	isDatetime := datetimeRx.MatchString(s)
	if isDatetime {
		return Version{
			Value:  s,
			Format: DatetimeFormat,
		}, nil
	}

	isTimestamp := timestampRx.MatchString(s)
	if isTimestamp {
		return Version{
			Value:  s,
			Format: TimestampFormat,
		}, nil
	}

	if isPositiveInt(s) {
		return Version{
			Value:  s,
			Format: NumberFormat,
		}, nil
	}

	return Version{}, errors.Wrapf(ErrInvalidVersionFormat, "input: %s", s)
}

func isPositiveInt(s string) bool {
	n, err := strconv.Atoi(s)
	if err != nil {
		return false
	}

	return n >= 0
}
