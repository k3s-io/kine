package log

import (
	"context"

	"github.com/sirupsen/logrus"
)

type logKey string

const key logKey = "kine-logger"

type logger interface {
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Debugf(msg string, args ...interface{})
}

func SetLogger(ctx context.Context, logger logger) context.Context {
	return context.WithValue(ctx, key, logger)
}

func getLogger(ctx context.Context) logger {
	if log, _ := ctx.Value("k3s-logger").(logger); log != nil {
		log.(*logrus.Logger).SetLevel(logrus.InfoLevel)
		return log
	}
	log, _ := ctx.Value(key).(logger)
	if log == nil {
		return logrus.StandardLogger()
	}
	return log
}

func Infof(ctx context.Context, msg string, args ...interface{}) {
	getLogger(ctx).Infof(msg, args...)
}

func Warnf(ctx context.Context, msg string, args ...interface{}) {
	getLogger(ctx).Warnf(msg, args...)
}

func Errorf(ctx context.Context, msg string, args ...interface{}) {
	getLogger(ctx).Warnf(msg, args...)
}

func Debugf(ctx context.Context, msg string, args ...interface{}) {
	getLogger(ctx).Debugf(msg, args...)
}
