package tern

import (
	"github.com/denismitr/tern/v3/internal/logger"
)

func UseColorLogger(p logger.Printer, printSql, printDebug bool) OptionFunc {
	return func(m *Migrator) error {
		m.lg = logger.NewColorLogger(p, printSql, printDebug)
		return nil
	}
}

func UseLogger(p logger.Printer, printSql, printDebug bool) OptionFunc {
	return func(m *Migrator) error {
		m.lg = logger.NewBWLogger(p, printSql, printDebug)
		return nil
	}
}
