package logger

import (
	"context"
	"errors"
	"net/http"

	"fmt"

	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/hashicorp/go-uuid"

	httptransport "github.com/go-kit/kit/transport/http"
)

// global logger instance
var lg = logger{}

// Init initializes logger
func Init(loggerMode string) (func(), error) {
	return lg.initialize(loggerMode)
}

// Debug logs debug message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Debug for details
func Debug(args ...interface{}) {
	lg.logger.Debug(args...)
}

// Einfo logs with request ID getting from context
func Einfo(ctx context.Context, args ...interface{}) {
	reqID, err := getReqID(ctx)
	if err == nil {
		lg.logger.Infof("Request ID: %s Payload: %s", reqID, args)
	}
}

// Info logs info message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Info for details.
func Info(args ...interface{}) {
	lg.logger.Info(args...)
}

// Warn logs warn message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Warn for details.
func Warn(args ...interface{}) {
	lg.logger.Warn(args...)
}

// Error logs error message with optional payload and additional data
func Error(err error, payload interface{}, keysAndValues ...interface{}) {
	lg.error(err, payload, keysAndValues...)
}

// DPanic logs panic message. In development, the logger then panics.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.DPanic for details
func DPanic(args ...interface{}) {
	lg.logger.DPanic(args...)
}

// Panic uses fmt.Sprint to construct and log a message, then panics.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Panic for details
func Panic(args ...interface{}) {
	lg.logger.Panic(args...)
}

// Fatal logs error message with optional payload and additional data, then calls os.Exit(1)
func Fatal(err error, payload interface{}, keysAndValues ...interface{}) {
	lg.fatal(err, payload, keysAndValues)
}

// Debugf uses fmt.Sprintf to log a templated message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Debugf for details
func Debugf(template string, args ...interface{}) {
	lg.logger.Debugf(template, args...)
}

// Infof uses fmt.Sprintf to log a templated message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Infof for details
func Infof(template string, args ...interface{}) {
	lg.logger.Infof(template, args...)
}

// Warnf uses fmt.Sprintf to log a templated message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Warnf for details
func Warnf(template string, args ...interface{}) {
	lg.logger.Warnf(template, args...)
}

// DPanicf uses fmt.Sprintf to log a templated message. In development, the logger then panics.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.DPanicf for details
func DPanicf(template string, args ...interface{}) {
	lg.logger.DPanicf(template, args...)
}

// Panicf uses fmt.Sprintf to log a templated message, then panics.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Panicf for details
func Panicf(template string, args ...interface{}) {
	lg.logger.Panicf(template, args...)
}

// Debugw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Debugw for details
func Debugw(msg string, keysAndValues ...interface{}) {
	lg.logger.Debugw(msg, keysAndValues...)
}

// Infow logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Infow for details
func Infow(msg string, keysAndValues ...interface{}) {
	lg.logger.Infow(msg, keysAndValues...)
}

// Warnw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Warnw for details
func Warnw(msg string, keysAndValues ...interface{}) {
	lg.logger.Warnw(msg, keysAndValues...)
}

// DPanicw logs a message with some additional context. In development, the
// logger then panics.The variadic key-value
// pairs are treated as they are in With.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.DPanicw for details
func DPanicw(msg string, keysAndValues ...interface{}) {
	lg.logger.DPanicw(msg, keysAndValues...)
}

// Panicw logs a message with some additional context, then panics. The
// variadic key-value pairs are treated as they are in With.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Panicw for details
func Panicw(msg string, keysAndValues ...interface{}) {
	lg.logger.Panicw(msg, keysAndValues...)
}

// ServerErrorLogger return HTTP server error logger for go-kit server
func ServerErrorLogger() httptransport.ServerOption {
	return httptransport.ServerErrorLogger(&lg)
}

// LoggerEndpointMiddleware returns go-kit middlewarefor which logs errors, panics & success (in debug)
func LoggerEndpointMiddleware() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			defer func(begin time.Time) {
				r := recover()
				if r != nil { // panic
					Error(fmt.Errorf("Panic: %v", r), request, "duration", time.Since(begin))
				} else if err != nil { // error
					Error(err, request, "duration", time.Since(begin))
				} else { // success
					Debugw("Success", "payload", request, "duration", time.Since(begin))
				}
			}(time.Now())

			return next(ctx, request)
		}
	}
}

// LoggerPathThrough returns a go-kit request function
// to add the request ID into context for path through logging
func LoggerPathThrough() httptransport.RequestFunc {
	return func(ctx context.Context, req *http.Request) context.Context {
		reqID, err := uuid.GenerateUUID()
		if err != nil {
			return ctx
		}
		return context.WithValue(ctx, "reqID", reqID)
	}
}

// GetReqID returns request ID or error from context
// for path through logging
func GetReqID(ctx *context.Context) (reqID string, err error) {
	return getReqID(*ctx)
}

// getReqID returns request ID or error from context
// for path through logging
func getReqID(ctx context.Context) (reqID string, err error) {
	val := ctx.Value("reqID")
	if val == nil {
		return "", errors.New("can't get reqID from context")
	}
	return val.(string), nil
}
