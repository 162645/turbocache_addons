package logger

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Logger 接口定义
type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})

	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithContext(ctx context.Context) Logger
	WithError(err error) Logger
}

// Fields 日志字段类型（与 map[string]interface{} 等效）
type Fields = map[string]interface{}

// Config 日志配置
type Config struct {
	Level      string `yaml:"level" json:"level"`             // 日志级别
	Format     string `yaml:"format" json:"format"`           // 日志格式：json, text
	Output     string `yaml:"output" json:"output"`           // 输出：stdout, stderr, file
	File       string `yaml:"file" json:"file"`               // 文件路径
	MaxSize    int    `yaml:"max_size" json:"max_size"`       // 最大文件大小(MB)
	MaxAge     int    `yaml:"max_age" json:"max_age"`         // 最大保留天数
	MaxBackups int    `yaml:"max_backups" json:"max_backups"` // 最大备份数
	Compress   bool   `yaml:"compress" json:"compress"`       // 是否压缩
}

// DefaultConfig 默认配置
var DefaultConfig = Config{
	Level:      "info",
	Format:     "json",
	Output:     "stdout",
	MaxSize:    100,
	MaxAge:     30,
	MaxBackups: 10,
	Compress:   true,
}

// 全局 logger 实例
var defaultLogger Logger

// logrusLogger logrus 实现
type logrusLogger struct {
	logger *logrus.Logger
	entry  *logrus.Entry
}

// Init 初始化日志系统
func Init(config Config) error {
	logger := logrus.New()

	// 设置日志级别
	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		return fmt.Errorf("invalid log level: %v", err)
	}
	logger.SetLevel(level)

	// 设置日志格式
	switch strings.ToLower(config.Format) {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339Nano,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
				logrus.FieldKeyFunc:  "function",
				logrus.FieldKeyFile:  "file",
			},
		})
	case "text":
		logger.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: time.RFC3339Nano,
			FullTimestamp:   true,
			CallerPrettyfier: func(f *runtime.Frame) (string, string) {
				filename := strings.Split(f.File, "/")
				return f.Function, fmt.Sprintf("%s:%d", filename[len(filename)-1], f.Line)
			},
		})
	default:
		return fmt.Errorf("unsupported log format: %s", config.Format)
	}

	// 设置输出
	switch strings.ToLower(config.Output) {
	case "stdout":
		logger.SetOutput(os.Stdout)
	case "stderr":
		logger.SetOutput(os.Stderr)
	case "file":
		if config.File == "" {
			return fmt.Errorf("log file path is required when output is file")
		}
		file, err := os.OpenFile(config.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("failed to open log file: %v", err)
		}
		logger.SetOutput(file)
	default:
		return fmt.Errorf("unsupported log output: %s", config.Output)
	}

	// 启用调用者信息
	logger.SetReportCaller(true)

	defaultLogger = &logrusLogger{
		logger: logger,
		entry:  logrus.NewEntry(logger),
	}

	return nil
}

// GetDefaultLogger 获取默认日志器
func GetDefaultLogger() Logger {
	if defaultLogger == nil {
		// 如果没有初始化，使用默认配置
		if err := Init(DefaultConfig); err != nil {
			panic(fmt.Sprintf("failed to init default logger: %v", err))
		}
	}
	return defaultLogger
}

// NewLogger 创建新的日志器实例
func NewLogger(config Config) (Logger, error) {
	logger := logrus.New()

	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %v", err)
	}
	logger.SetLevel(level)

	switch strings.ToLower(config.Format) {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339Nano,
		})
	case "text":
		logger.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: time.RFC3339Nano,
			FullTimestamp:   true,
		})
	}

	return &logrusLogger{
		logger: logger,
		entry:  logrus.NewEntry(logger),
	}, nil
}

// 实现 Logger 接口
func (l *logrusLogger) Debug(args ...interface{}) {
	l.entry.Debug(args...)
}

func (l *logrusLogger) Info(args ...interface{}) {
	l.entry.Info(args...)
}

func (l *logrusLogger) Warn(args ...interface{}) {
	l.entry.Warn(args...)
}

func (l *logrusLogger) Error(args ...interface{}) {
	l.entry.Error(args...)
}

func (l *logrusLogger) Fatal(args ...interface{}) {
	l.entry.Fatal(args...)
}

func (l *logrusLogger) Debugf(format string, args ...interface{}) {
	l.entry.Debugf(format, args...)
}

func (l *logrusLogger) Infof(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

func (l *logrusLogger) Warnf(format string, args ...interface{}) {
	l.entry.Warnf(format, args...)
}

func (l *logrusLogger) Errorf(format string, args ...interface{}) {
	l.entry.Errorf(format, args...)
}

func (l *logrusLogger) Fatalf(format string, args ...interface{}) {
	l.entry.Fatalf(format, args...)
}

func (l *logrusLogger) WithField(key string, value interface{}) Logger {
	return &logrusLogger{
		logger: l.logger,
		entry:  l.entry.WithField(key, value),
	}
}

func (l *logrusLogger) WithFields(fields map[string]interface{}) Logger {
	return &logrusLogger{
		logger: l.logger,
		entry:  l.entry.WithFields(fields),
	}
}

func (l *logrusLogger) WithContext(ctx context.Context) Logger {
	return &logrusLogger{
		logger: l.logger,
		entry:  l.entry.WithContext(ctx),
	}
}

func (l *logrusLogger) WithError(err error) Logger {
	return &logrusLogger{
		logger: l.logger,
		entry:  l.entry.WithError(err),
	}
}

// 便捷方法
func Debug(args ...interface{}) {
	GetDefaultLogger().Debug(args...)
}

func Info(args ...interface{}) {
	GetDefaultLogger().Info(args...)
}

func Warn(args ...interface{}) {
	GetDefaultLogger().Warn(args...)
}

func Error(args ...interface{}) {
	GetDefaultLogger().Error(args...)
}

func Fatal(args ...interface{}) {
	GetDefaultLogger().Fatal(args...)
}

func Debugf(format string, args ...interface{}) {
	GetDefaultLogger().Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	GetDefaultLogger().Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	GetDefaultLogger().Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	GetDefaultLogger().Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	GetDefaultLogger().Fatalf(format, args...)
}

func WithField(key string, value interface{}) Logger {
	return GetDefaultLogger().WithField(key, value)
}

func WithFields(fields map[string]interface{}) Logger {
	return GetDefaultLogger().WithFields(fields)
}

func WithContext(ctx context.Context) Logger {
	return GetDefaultLogger().WithContext(ctx)
}

func WithError(err error) Logger {
	return GetDefaultLogger().WithError(err)
}
