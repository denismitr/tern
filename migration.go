package tern

import (
	"github.com/pkg/errors"
	"regexp"
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
	Name    string
	Version string
	Up      string
	Down    string
}

type Migrations []Migration

func (m Migrations) Len() int {
	return len(m)
}

func (m Migrations) Less(i, j int) bool {
	return m[i].Version < m[j].Version
}

func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}
