package mysql

import (
	"fmt"
	"net/url"

	mssql "github.com/denisenkom/go-mssqldb"
	gormMssql "gorm.io/driver/sqlserver"
	"gorm.io/gorm"

	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
)

const (
	defaultDSN = "sqlserver://sa@localhost?kubernetes"
)

type Driver struct{}

func (b *Driver) PrepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = fmt.Sprintf("sqlserver://%s", dataSourceName)
	}

	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}

	u.RawQuery = FillDefaultAndExtraOptions(queryMap, tlsInfo).Encode()
	return u.String(), nil
}

func (b *Driver) HandleInsertionError(err error) error {
	if mssqlErr, convertible := err.(mssql.Error); convertible {
		switch mssqlErr.Number {
		/* Server: Msg 2627
		   Violation of PRIMARY KEY constraint Constraint Name.
		   Cannot insert duplicate key in object Table Name.
		*/
		/* Server: Msg 2601
		Cannot insert duplicate key row in object '<Object Name>'
		with unique index '<Index Name>'.
		*/
		case 2627, 2601:
			return server.ErrKeyExists
		default:
			return nil
		}
	}
	return nil
}

func (b *Driver) GetOpenFunctor() func(string) gorm.Dialector {
	return gormMssql.Open
}

func FillDefaultAndExtraOptions(queryMap url.Values, tlsInfo tls.Config) url.Values {
	params := url.Values{}

	// certificate - The file that contains the public key certificate of the **CA** that signed the SQL Server certificate.
	// The specified certificate overrides the go platform specific CA certificates.
	if _, found := queryMap["certificate"]; !found && tlsInfo.CAFile != "" {
		params.Add("certificate", tlsInfo.CAFile)
	}

	// default database to use
	if _, found := queryMap["database"]; !found {
		params.Add("database", "kubernetes")
	}

	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	return params
}
