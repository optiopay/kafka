package kafka

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

type nullLogger struct {
}

func (nullLogger) Debug(args ...interface{}) {}
func (nullLogger) Info(args ...interface{})  {}
func (nullLogger) Warn(args ...interface{})  {}
func (nullLogger) Error(args ...interface{}) {}
