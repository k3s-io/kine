package gorm

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/rancher/kine/pkg/tls"
)

type DatabaseErrorHandler interface {
	HandleInsertionError(err error) error
}

type Driver interface {
	PrepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error)
	GetOpenFunctor() func(string) gorm.Dialector
	DatabaseErrorHandler
}

type DatabaseBackend struct {
	DB *gorm.DB
	DatabaseErrorHandler
}

func New(ctx context.Context, dialect gorm.Dialector, handlers DatabaseErrorHandler) (backend *DatabaseBackend, err error) {
	db, err := gorm.Open(dialect, &gorm.Config{
		Logger:      &Logger{},
		PrepareStmt: true,
	})

	if err != nil {
		return
	}

	rawDB, err := db.DB()
	if err != nil {
		return
	}

	// Actually, I don't think this is much needed
	// GORM offers automatic pinging so no worries
majorScope:
	for i := 0; i < 300; i++ {
		for i := 0; i < 3; i++ {
			if err = rawDB.Ping(); err == nil {
				break majorScope
			}
		}

		err = rawDB.Close()
		logrus.WithError(err).Error("failed to ping connection")
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-time.After(time.Second):
		}
	}
	backend = &DatabaseBackend{DB: db, DatabaseErrorHandler: handlers}
	return
}
