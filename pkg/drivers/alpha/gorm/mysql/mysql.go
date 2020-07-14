package mysql

import (
	"context"
	cryptotls "crypto/tls"

	gormMysql "gorm.io/driver/mysql"

	"github.com/rancher/kine/pkg/drivers/alpha/gorm"
	kineMysql "github.com/rancher/kine/pkg/drivers/mysql"
	"github.com/rancher/kine/pkg/tls"
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config) (*gorm.GormBacked, error) {
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		tlsConfig.MinVersion = cryptotls.VersionTLS11
	}

	dsn, err := kineMysql.PrepareDSN(dataSourceName, tlsConfig)
	if err != nil {
		return nil, err
	}

	dialector := gormMysql.Open(dsn)
	backend, err := gorm.New(ctx, dialector)
	if err == nil {
		backend.HandleInsertionError = gorm.TransformTranslateErrToHandleInsertionError(kineMysql.TranslateError)
	}
	return backend, err
}
