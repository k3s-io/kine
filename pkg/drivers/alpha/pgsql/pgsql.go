package pgsql

import (
	"context"

	gormPostgres "gorm.io/driver/postgres"

	"github.com/rancher/kine/pkg/drivers/alpha/gorm"
	kinePgsql "github.com/rancher/kine/pkg/drivers/pgsql"
	"github.com/rancher/kine/pkg/tls"
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config) (*gorm.GormBacked, error) {
	dsn, err := kinePgsql.PrepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}
	dialector := gormPostgres.Open(dsn)
	backend, err := gorm.New(ctx, dialector)
	if err == nil {
		backend.HandleInsertionError = gorm.TransformTranslateErrToHandleInsertionError(kinePgsql.TranslateError)
	}
	return backend, err
}
