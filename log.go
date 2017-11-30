package kafka

import (
	"context"
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

type loggerContextKey struct{}

var (
	loggerContextKeyValue = loggerContextKey{}
)

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

// WithLogger creates a context that uses the given logger.
func WithLogger(ctx context.Context, logger Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, loggerContextKeyValue, logger)
}

// getLogFromContext looks in the given context for a logger.
// If found it returns that one, otherwise the given default is returned.
func getLogFromContext(ctx context.Context, defaultLogger Logger) Logger {
	if ctx != nil {
		if raw := ctx.Value(loggerContextKeyValue); raw != nil {
			if logger, ok := raw.(Logger); ok && logger != nil {
				return logger
			}
		}
	}
	return defaultLogger
}
