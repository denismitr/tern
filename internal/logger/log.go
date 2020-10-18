package logger

import (
	"bytes"
	"fmt"
	"github.com/logrusorgru/aurora/v3"
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

type BWLogger struct {
	printer Printer
	debug   bool
	sql     bool
}

var _ Logger = (*ColoredLogger)(nil)
var _ Logger = (*BWLogger)(nil)

func NewColorLogger(p Printer, sql, debug bool) *ColoredLogger {
	return &ColoredLogger{
		printer: p,
		debug: debug,
		sql: sql,
	}
}

func NewBWLogger(p Printer, sql, debug bool) *ColoredLogger {
	return &ColoredLogger{
		printer: p,
		debug: debug,
		sql: sql,
	}
}

func (cl *ColoredLogger) Debugf(format string, args ...interface{}) {
	if cl.debug {
		msg := fmt.Sprintf("\nTern debug: "+format, args...)
		_ = cl.printer.Output(2, aurora.Yellow(msg).String())
	}
}

func (cl *ColoredLogger) Successf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\nTern: "+format, args...)
	_ = cl.printer.Output(2, aurora.Green(msg).String())
}

func (cl *ColoredLogger) Error(err error) {
	msg := fmt.Sprintf("\nTern error: %s", err.Error())
	_ = cl.printer.Output(2, aurora.Red(msg).String())
}

func (cl *ColoredLogger) SQL(query string, args ...interface{}) {
	if cl.sql {
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

		_ = cl.printer.Output(2, aurora.Gray(15, buf.String()).String())
	}
}

func (bwl *BWLogger) Debugf(format string, args ...interface{}) {
	if bwl.debug {
		msg := fmt.Sprintf("\nTern debug: "+format, args...)
		_ = bwl.printer.Output(2, msg)
	}
}

func (bwl *BWLogger) Successf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\nTern: "+format, args...)
	_ = bwl.printer.Output(2, msg)
}

func (bwl *BWLogger) Error(err error) {
	msg := fmt.Sprintf("\nTern error: %s", err.Error())
	_ = bwl.printer.Output(2, msg)
}

func (bwl *BWLogger) SQL(query string, args ...interface{}) {
	if bwl.sql {
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

		_ = bwl.printer.Output(2, buf.String())
	}
}
