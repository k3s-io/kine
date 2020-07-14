package gorm

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type GormBacked struct {
	DB                   *gorm.DB
	HandleInsertionError func(error) error
}

var columns = "id as theid, name, created, deleted, create_revision, prev_revision, lease, value, old_value"

func New(ctx context.Context, dialect gorm.Dialector) (*GormBacked, error) {
	db, err := gorm.Open(dialect, &gorm.Config{
		Logger: &Logger{},
	})
	if err != nil {
		return nil, err
	}

	rawDB, err := db.DB()
	if err != nil {
		return nil, err
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
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	if err := db.AutoMigrate(&KineEntry{}); err != nil {
		return nil, err
	}

	backend := &GormBacked{DB: db}
	return backend, nil
}
