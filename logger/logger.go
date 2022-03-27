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
	// JSONEncoder JSON log format
	JSONEncoder = 0
	// ConsoleEncoder Console log format
	ConsoleEncoder = 1

	// default config
	defaultConfig = Config{
		LoggerMode:  "prod",
		EncoderType: 1,
		EncodeLevel: zapcore.CapitalLevelEncoder,
		EncodeTime:  zapcore.RFC3339TimeEncoder,
	}
)

// Config struct for init logger configaration
type Config struct {
	LoggerMode  string
	EncoderType int
	EncodeLevel zapcore.LevelEncoder
	EncodeTime  zapcore.TimeEncoder
}

func init() {
	Init(nil)
}

func prepareConfig(config *Config) zapcore.Core {
	// prepare config
	if config == nil {
		// use default config
		fmt.Printf("No config set, default config will be used, prod mode, JSON encoder without colors")
		config = &defaultConfig
	} else {
		// check config params and set defaults is empty
		if config.EncodeLevel == nil {
			config.EncodeLevel = zapcore.CapitalLevelEncoder
		}
		if config.TimeFormat == nil {
			config.EncodeTime = zapcore.RFC3339TimeEncoder
		}
	}

	// prepare logger mode
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

	configEncoder.EncodeLevel = config.EncodeLevel
	configEncoder.EncodeTime = config.EncodeTime

	// prepare encoder
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

// NewSugaredLogger constructor for create sugared logger
func NewSugaredLogger(config *Config) *zap.SugaredLogger {
	return createSugaredLogger(config)
}

// NewLogger constructor for create logger
func NewLogger(config *Config) *zap.Logger {
	return createLogger(config)
}

// Init prepare logger structure
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
