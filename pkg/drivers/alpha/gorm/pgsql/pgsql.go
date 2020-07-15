package pgsql

import (
	gormPostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"

	kinePgsql "github.com/rancher/kine/pkg/drivers/pgsql"
	"github.com/rancher/kine/pkg/tls"
)

type Driver struct{}

func (b *Driver) PrepareDSN(dsn string, tlsInfo tls.Config) (string, error) {
	dsn, err := kinePgsql.PrepareDSN(dsn, tlsInfo)
	if err != nil {
		return "", err
	}
	return dsn, nil
}

func (b *Driver) HandleInsertionError(err error) (newErr error) {
	if newErr = kinePgsql.TranslateError(err); newErr == err {
		newErr = nil
	}
	return
}

func (b *Driver) GetOpenFunctor() func(string) gorm.Dialector {
	return gormPostgres.Open
}
