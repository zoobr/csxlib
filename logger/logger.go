package logger

import (
	pkgerrors "github.com/pkg/errors"
	"go.uber.org/zap"
)

// stackTracer is errors interface from package 'github.com/pkg/errors'
type stackTracer interface {
	StackTrace() pkgerrors.StackTrace
}

// logger is a struct for loggers
type logger struct {
	logger    *zap.SugaredLogger // logger for common errors. Prints standart stacktrace
	errLogger *zap.SugaredLogger // logger for errors from package 'github.com/pkg/errors'. Prints stracktrace saved in error
}

// initialize initializes loggers
func (l *logger) initialize(loggerMode string) (func(), error) {
	var err error
	l.logger, err = newLogger(loggerMode, false)
	if err != nil {
		return nil, err
	}

	l.errLogger, err = newLogger(loggerMode, true)
	if err != nil {
		return nil, err
	}

	return l.finish, nil
}

// finish is finalizer. It flushes any buffered log entries
func (l *logger) finish() {
	l.logger.Sync()
	l.errLogger.Sync()
}

// choose chooses logger by error type
func (l *logger) choose(err error) *zap.SugaredLogger {
	if _, ok := err.(stackTracer); ok {
		return l.errLogger
	}
	return l.logger
}

// error logs error message with optional payload and additional data
func (l *logger) error(err error, payload interface{}, keysAndValues ...interface{}) {
	kvs := []interface{}{"payload", payload}
	kvs = append(kvs, keysAndValues...)
	l.choose(err).With(kvs...).Errorf("%+v", err)
}

// fatal logs error message with optional payload and additional data, then calls os.Exit(1)
func (l *logger) fatal(err error, payload interface{}, keysAndValues ...interface{}) {
	kvs := []interface{}{"payload", payload}
	kvs = append(kvs, keysAndValues...)
	l.choose(err).With(kvs...).Fatalf("%+v", err)
}

// Log implements go-kit Logger interface. It creates a log event from keyvals
// See https://pkg.go.dev/github.com/go-kit/log#Logger for details
func (l *logger) Log(keyvals ...interface{}) error {
	l.logger.Error(keyvals...)
	return nil
}
