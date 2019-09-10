package mysql

import (
	"context"
	cryptotls "crypto/tls"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
)

const (
	defaultUnixDSN = "root@unix(/var/run/mysqld/mysqld.sock)/"
	defaultHostDSN = "root@tcp(127.0.0.1)/"
)

var (
	schema = []string{
		`create table if not exists {{ .TableName }}
			(
				id INTEGER AUTO_INCREMENT,
				name TEXT,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER,
 				prev_revision INTEGER,
				lease INTEGER,
 				value BLOB,
 				old_value BLOB,
				PRIMARY KEY (id)
			);`,
	}
	nameIdx     = "create index kine_name_index on {{ .TableName }} (name(100))"
	revisionIdx = "create index kine_name_prev_revision_uindex on {{ .TableName }} (name(100), prev_revision)"
	createDB    = "create database if not exists "
)

func New(dataSourceName, tableName string, tlsInfo tls.Config) (server.Backend, error) {
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		tlsConfig.MinVersion = cryptotls.VersionTLS11
	}

	parsedDSN, err := prepareDSN(dataSourceName, tlsConfig)
	if err != nil {
		return nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return nil, err
	}

	dialect, err := generic.Open("mysql", parsedDSN, tableName, "?", false)
	if err != nil {
		return nil, err
	}
	dialect.LastInsertID = true
	if err := setup(dialect.DB, tableName); err != nil {
		return nil, err
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}

func setup(db *sql.DB, tableName string) error {
	for _, stmt := range schema {
		renderedSchemaSQL, err := generic.RenderSchemaSQLFromTemplate("schema", stmt, generic.SQLSchemaTemplateParameter{
			TableName: tableName,
		})
		if err != nil {
			return err
		}
		_, err = db.Exec(renderedSchemaSQL)
		if err != nil {
			return err
		}
	}

	// check if duplicate indexes
	indexes := []string{
		nameIdx,
		revisionIdx}
	for i, idx := range indexes {
		renderedIndexSQL, err := generic.RenderSchemaSQLFromTemplate("index", idx, generic.SQLSchemaTemplateParameter{
			TableName: tableName,
		})
		if err != nil {
			return err
		}
		indexes[i] = renderedIndexSQL
	}

	for _, idx := range indexes {
		err := createIndex(db, idx)
		if err != nil {
			return err
		}
	}
	return nil
}

func createDBIfNotExist(dataSourceName string) error {
	config, err := mysql.ParseDSN(dataSourceName)
	if err != nil {
		return err
	}
	dbName := config.DBName

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return err
	}
	_, err = db.Exec(createDB + dbName)
	if err != nil {
		if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1049 {
			return err
		}
		config.DBName = ""
		db, err = sql.Open("mysql", config.FormatDSN())
		if err != nil {
			return err
		}
		_, err = db.Exec(createDB + dbName)
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareDSN(dataSourceName string, tlsConfig *cryptotls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultUnixDSN
		if tlsConfig != nil {
			dataSourceName = defaultHostDSN
		}
	}
	config, err := mysql.ParseDSN(dataSourceName)
	if err != nil {
		return "", err
	}
	// setting up tlsConfig
	if tlsConfig != nil {
		if err := mysql.RegisterTLSConfig("kine", tlsConfig); err != nil {
			return "", err
		}
		config.TLSConfig = "kine"
	}
	dbName := "kubernetes"
	if len(config.DBName) > 0 {
		dbName = config.DBName
	}
	config.DBName = dbName
	parsedDSN := config.FormatDSN()

	return parsedDSN, nil
}

func createIndex(db *sql.DB, indexStmt string) error {
	_, err := db.Exec(indexStmt)
	if err != nil {
		if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1061 {
			return err
		}
	}
	return nil
}
