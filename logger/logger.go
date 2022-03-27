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
	logger *zap.SugaredLogger
	Debug  func(...interface{})
	Debugf func(string, ...interface{})
	Debugw func(string, ...interface{})
	Info   func(...interface{})
	Infof  func(string, ...interface{})
	Warn   func(...interface{})
	Warnf  func(string, ...interface{})
	Error  func(...interface{})
	Errorf func(string, ...interface{})
	Panic  func(...interface{})
	Panicf func(string, ...interface{})
	Sync   func() error
)

var (
	JSONEncoder      = 1
	ConsoleEncoder   = 2
	MapObjectEncoder = 3
	defaultConfig    = Config{
		LoggerMode:  "prod",
		EncoderType: 1,
		Colors:      false,
	}
)

type Config struct {
	LoggerMode  string
	EncoderType int
	Colors      bool
}

func init() {
	Init(nil)
}

func prepareConfig(config *Config) zapcore.Core {
	if config == nil {
		config = &defaultConfig
	}
	var configEncoder zapcore.EncoderConfig
	logLevel := zapcore.DebugLevel
	loggerMode := config.LoggerMode
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
	if config.Colors {
		configEncoder.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	var newEncoder zapcore.Encoder
	switch config.EncoderType {
	case ConsoleEncoder:
		newEncoder = zapcore.NewConsoleEncoder(configEncoder)
	default:
		newEncoder = zapcore.NewJSONEncoder(configEncoder)
	}
	core := zapcore.NewCore(newEncoder, os.Stdout, logLevel)
	return core
}

func createSugaredLogger(config *Config) *zap.SugaredLogger {
	core := prepareConfig(config)
	return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.PanicLevel)).Sugar()
}

func createLogger(config *Config) *zap.Logger {
	core := prepareConfig(config)
	return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.PanicLevel))
}

func NewSugaredLogger(config *Config) *zap.SugaredLogger {
	return createSugaredLogger(config)
}

func NewLogger(config *Config) *zap.Logger {
	return createLogger(config)
}

func Init(config *Config) {
	logger = createSugaredLogger(config)
	Debug = logger.Debug
	Debugf = logger.Debugf
	Debugw = logger.Debugw
	Error = logger.Error
	Errorf = logger.Errorf
	Info = logger.Info
	Infof = logger.Infof
	Warn = logger.Warn
	Warnf = logger.Warnf
	Panic = logger.Panic
	Panicf = logger.Panicf
	Sync = logger.Sync
}
