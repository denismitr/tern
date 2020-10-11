package tern

import (
	"github.com/denismitr/tern/internal/logger"
)

type OptionFunc func(*Migrator) error
type ActionConfigurator func(a *action)

func UseColorLogger(p logger.Printer, printSql, printDebug bool) OptionFunc {
	return func(m *Migrator) error {
		m.lg = logger.New(p, printSql, printDebug)
		return nil
	}
}


