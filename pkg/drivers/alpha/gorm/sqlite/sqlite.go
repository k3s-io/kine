package sqlite

import (
	gormSqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"

	kineSqlite "github.com/rancher/kine/pkg/drivers/sqlite"
	"github.com/rancher/kine/pkg/tls"
)

type Driver struct{}

func (b *Driver) PrepareDSN(dsn string, _ tls.Config) (string, error) {
	dsn, err := kineSqlite.PrepareDSN(dsn)
	if err != nil {
		return "", err
	}
	return dsn, nil
}

func (b *Driver) HandleInsertionError(err error) (newErr error) {
	if newErr = kineSqlite.TranslateError(err); newErr == err {
		newErr = nil
	}
	return
}

func (b *Driver) GetOpenFunctor() func(string) gorm.Dialector {
	return gormSqlite.Open
}
