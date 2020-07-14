package sqlite

import (
	"context"

	gormSqlite "gorm.io/driver/sqlite"

	"github.com/rancher/kine/pkg/drivers/alpha/gorm"
	kineSqlite "github.com/rancher/kine/pkg/drivers/sqlite"
)

func New(ctx context.Context, dsn string) (*gorm.GormBacked, error) {
	dsn, err := kineSqlite.PrepareDSN(dsn)
	if err != nil {
		return nil, err
	}
	dialector := gormSqlite.Open(dsn)
	backend, err := gorm.New(ctx, dialector)
	if err == nil {
		backend.HandleInsertionError = gorm.TransformTranslateErrToHandleInsertionError(kineSqlite.TranslateError)
	}
	return backend, err
}
