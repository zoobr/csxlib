package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	loggerModeDev     = "dev"     // development logger mode
	loggerModeProd    = "prod"    // production logger mode
	loggerModeTesting = "testing" // logger mode for tests
	defaultLoggerMode = loggerModeDev
)

var (
	logger *zap.Logger

	Trace func(msg string, fields ...zapcore.Field)
	Info  func(msg string, fields ...zapcore.Field)
	Warn  func(msg string, fields ...zapcore.Field)
	Error func(msg string, fields ...zapcore.Field)
	Fatal func(msg string, fields ...zapcore.Field)
	Sugar *zap.SugaredLogger
	Sync  func() error
)

func init() {
	InitLogger(defaultLoggerMode)
}

func createLogger(loggerMode string) *zap.Logger {
	var configEncoder zapcore.EncoderConfig
	logLevel := zapcore.DebugLevel
	if loggerMode == loggerModeProd { // logger for development mode
		configEncoder = zap.NewProductionEncoderConfig()
		logLevel = zapcore.InfoLevel
	} else {
		if loggerMode != loggerModeDev && len(loggerMode) > 0 {
			fmt.Printf("wrong logger mode: %s, will use dev logger", loggerMode)
		} else if len(loggerMode) == 0 {
			fmt.Printf("logger mode is empty, will use dev logger")
		}
		configEncoder = zap.NewDevelopmentEncoderConfig()
	}
	configEncoder.EncodeLevel = zapcore.CapitalColorLevelEncoder
	core := zapcore.NewCore(zapcore.NewJSONEncoder(configEncoder), os.Stdout, logLevel)
	return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.PanicLevel))
}

func NewLogger(loggerMode string) *zap.Logger {
	return createLogger(loggerMode)
}

func InitLogger(loggerMode string) {
	logger = createLogger(loggerMode)
	Trace = logger.Debug
	Error = logger.Error
	Info = logger.Info
	Warn = logger.Warn
	Fatal = logger.Panic
	Sugar = logger.Sugar()
	Sync = logger.Sync
}
