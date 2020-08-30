package logger

import (
	"bytes"
	"fmt"
	"github.com/logrusorgru/aurora/v3"
	"log"
)

type Printer interface {
	Output(calldepth int, s string) error
}

type Logger interface {
	Successf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Error(err error)
	SQL(query string, args ...interface{})
}

type ColoredLogger struct {
	printer Printer
	debug   bool
	sql     bool
}

var _ Logger = (*ColoredLogger)(nil)

func New(p Printer, sql, debug bool) *ColoredLogger {
	return &ColoredLogger{
		printer: p,
		debug: debug,
		sql: sql,
	}
}

func (c ColoredLogger) Debugf(format string, args ...interface{}) {
	if c.debug {
		msg := fmt.Sprintf("\nTern debug: "+format, args...)
		_ = log.Output(2, aurora.Yellow(msg).String())
	}
}

func (c ColoredLogger) Successf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\nTern: "+format, args...)
	_ = log.Output(2, aurora.Green(msg).String())
}

func (c ColoredLogger) Error(err error) {
	msg := fmt.Sprintf("\nTern error: %s", err.Error())
	_ = log.Output(2, aurora.Red(msg).String())
}

func (c ColoredLogger) SQL(query string, args ...interface{}) {
	if c.sql {
		var buf bytes.Buffer
		buf.WriteString("\nTern running sql: ")
		buf.WriteString(query)
		buf.WriteString("\nquery parameters: ")

		for i := range args {
			if i+1 < len(args) {
				buf.WriteString(fmt.Sprintf("{%#v}, ", args[i]))
			} else {
				buf.WriteString(fmt.Sprintf("{%#v}", args[i]))
			}
		}

		_ = log.Output(2, aurora.Gray(5, buf.String()).String())
	}
}
