package migration

import (
	"bytes"
	"github.com/pkg/errors"
	"regexp"
	"strings"
	"time"
	"unicode"
)

var ErrInvalidTimestamp = errors.New("invalid timestamp in migration filename")

type Version struct {
	Timestamp string
	CreatedAt time.Time
}

type Migration struct {
	Key     string
	Name    string
	Version Version
	Up      []string
	Down    []string
}

func (m *Migration) MigrateScripts() string {
	var ms bytes.Buffer

	for i := range m.Up {
		ms.WriteString(m.Up[i])

		if !strings.HasSuffix(m.Up[i], ";") {
			ms.WriteString(";")
		}
	}

	return ms.String()
}

func (m *Migration) RollbackScripts() string {
	var ms bytes.Buffer

	for i := range m.Down {
		ms.WriteString(m.Down[i])

		if !strings.HasSuffix(m.Down[i], ";") {
			ms.WriteString(";")
		}
	}

	return ms.String()
}



type Migrations []Migration

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

func ExtractVersionFromKey(key string, r *regexp.Regexp) (string, error) {
	matches := r.FindStringSubmatch(key)
	if len(matches) < 2 {
		return "", ErrInvalidTimestamp
	}

	return matches[1], nil
}

func ExtractNameFromKey(key string, r *regexp.Regexp) string {
	matches := r.FindStringSubmatch(key)
	if len(matches) < 2 {
		return ""
	}

	return UcFirst(strings.Replace(matches[1], "_", " ", -1))
}

func UcFirst(s string) string {
	r := []rune(s)

	if len(r) == 0 {
		return ""
	}

	f := string(unicode.ToUpper(r[0]))

	return f + string(r[1:])
}
