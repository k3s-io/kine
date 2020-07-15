package mysql

import (
	cryptotls "crypto/tls"

	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	kineMysql "github.com/rancher/kine/pkg/drivers/mysql"
	"github.com/rancher/kine/pkg/tls"
)

type Driver struct{}

func (b *Driver) PrepareDSN(dsn string, tlsInfo tls.Config) (string, error) {
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return "", err
	}

	if tlsConfig != nil {
		tlsConfig.MinVersion = cryptotls.VersionTLS11
	}

	dsn, err = kineMysql.PrepareDSN(dsn, tlsConfig)
	if err != nil {
		return "", err
	}
	return dsn, nil
}

func (b *Driver) HandleInsertionError(err error) (newErr error) {
	if newErr = kineMysql.TranslateError(err); newErr == err {
		newErr = nil
	}
	return
}

func (b *Driver) GetOpenFunctor() func(string) gorm.Dialector {
	return gormMysql.Open
}
