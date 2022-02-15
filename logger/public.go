package logger

// global logger instance
var lg = logger{}

// Init initializes logger
func Init(loggerMode string) (func(), error) {
	return lg.initialize(loggerMode)
}

// GetLogger returns logger instance
func GetLogger() *logger {
	return &lg
}

// Debug logs debug message.
// See https://pkg.go.dev/go.uber.org/zap#SugaredLogger.Debug for details
func Debug(args ...interface{}) {
	lg.logger.Debug(args...)
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
