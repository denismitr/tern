package logger

type NullLogger struct {}

var _ Logger = (*NullLogger)(nil)

func (l NullLogger) Successf(_ string, _ ...interface{}) {}

func (l NullLogger) Debugf(_ string, _ ...interface{}) {}

func (l NullLogger) SQL(_ string, _ ...interface{}) {}

func (NullLogger) Error(_ error) {

}
