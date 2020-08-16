package migration

import (
	"github.com/pkg/errors"
	"regexp"
	"strings"
	"unicode"
)

var ErrInvalidTimestamp = errors.New("invalid timestamp in migration filename")
var versionRegexp *regexp.Regexp
var nameRegexp *regexp.Regexp

func init() {
	var err error
	versionRegexp, err = regexp.Compile(`^(?P<version>\d{1,12})(_\w+)?$`)
	if err != nil {
		panic(err)
	}
	nameRegexp, err = regexp.Compile(`^\d{1,12}_(?P<name>\w+[\w_-]+)?$`)
	if err != nil {
		panic(err)
	}
}

type Migration struct {
	Key     string
	Name    string
	Version string
	Up      string
	Down    string
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
	return m[i].Version < m[j].Version
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
