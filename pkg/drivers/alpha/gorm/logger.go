package gorm

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm/logger"
)

type Logger struct{}

func (l *Logger) LogMode(logger.LogLevel) logger.Interface {
	return l
}

func (l *Logger) Info(ctx context.Context, s string, i ...interface{}) {
	logrus.WithContext(ctx).
		WithField("data", i).
		WithField("msg", s).
		Info("gorm Info")
}

func (l *Logger) Warn(ctx context.Context, s string, i ...interface{}) {
	logrus.WithContext(ctx).
		WithField("data", i).
		WithField("msg", s).
		Warn("gorm Warn")
}

func (l *Logger) Error(ctx context.Context, s string, i ...interface{}) {
	logrus.WithContext(ctx).
		WithField("data", i).
		WithField("msg", s).
		Error("gorm Error")
}

func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		sql, rows := fc()
		logrus.WithContext(ctx).
			WithTime(begin).
			WithError(err).
			WithField("sql", sql).
			WithField("rows", rows).
			Trace("gorm Trace")
	}
}
