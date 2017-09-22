package kafka

import (
	"testing"
)

// Logger is general logging interface that can be provided by popular logging
// frameworks.
//
// * https://github.com/go-kit/kit/tree/master/log
// * https://github.com/husio/log
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// nullLogger implements Logger interface, but discards all messages
type nullLogger struct {
}

func (nullLogger) Debug(msg string, args ...interface{}) {}
func (nullLogger) Info(msg string, args ...interface{})  {}
func (nullLogger) Warn(msg string, args ...interface{})  {}
func (nullLogger) Error(msg string, args ...interface{}) {}

// testLogger implements Logger interface, writing all data to stdout.
type testLogger struct {
	T *testing.T
}

func (t testLogger) Debug(msg string, args ...interface{}) {
	t.T.Logf("DEBUG "+msg+"\n", args...)
}
func (t testLogger) Info(msg string, args ...interface{}) {
	t.T.Logf("INFO "+msg+"\n", args...)
}
func (t testLogger) Warn(msg string, args ...interface{}) {
	t.T.Logf("WARN "+msg+"\n", args...)
}
func (t testLogger) Error(msg string, args ...interface{}) {
	t.T.Logf("ERROR "+msg+"\n", args...)
}
