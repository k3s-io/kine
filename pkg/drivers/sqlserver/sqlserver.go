package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
)

const (
	defaultDSN = "sqlserver://sa:"
)

var (
	schema = []string{
		`if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_NAME = N'kine')
		begin
 			create table kine (
				id int primary key identity (1, 1),
				name varchar(630),
				created int,
				deleted int,
				create_revision int,
				prev_revision int,
				lease int,
				value varbinary(max),
				old_value varbinary(max) )
		end 
		`,
		`if not exists ( select * from sys.indexes
			where name = 'kine_name_index' and
			object_id = OBJECT_ID('kine')) 
		begin
    		create nonclustered index kine_name_index on kine (name)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_prev_revision_uindex' and
			object_id = OBJECT_ID('kine')
		) begin
		create unique index kine_name_prev_revision_uindex on kine (name, prev_revision)
		end
		`,
	}
	createDB = "create database "
	columns  = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	revSQL   = `
		SELECT TOP 1 rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC`
	compactRevSQL = `
		SELECT TOP 1 crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC`

	idOfKey = `
		AND mkv.id <= ? AND mkv.id > (
			SELECT TOP 1 ikv.id
			FROM kine ikv
			WHERE
				ikv.name = ? AND
				ikv.id <= ?
			ORDER BY ikv.id DESC )`

	listSQL = fmt.Sprintf(`SELECT TOP 100 PERCENT (%s)[a], (%s)[b], %s
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE ?
				%%s
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  ( kv.deleted = 0 OR 'true' = ? )
		ORDER BY kv.id ASC
		`, revSQL, compactRevSQL, columns)
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return nil, err
	}
	dialect, err := setupGenericDriver(ctx, "sqlserver", parsedDSN, "@p", true)
	if err != nil {
		return nil, err
	}
	dialect.LastInsertID = false
	dialect.TranslateErr = func(err error) error {
		// Need to verify msqql error code for unique constraint violation
		if err, ok := err.(mssql.Error); ok && err.Number == 2627 {
			return server.ErrKeyExists
		}
		return err
	}

	if err := setup(dialect.DB); err != nil {
		return nil, err
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil

}

func setupGenericDriver(ctx context.Context, driverName, dataSourceName string, paramCharacter string, numbered bool) (*generic.Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = generic.OpenAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return &generic.Generic{
		DB: db,
		GetRevisionSQL: generic.QueryBuilder(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        generic.QueryBuilder(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: generic.QueryBuilder(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  generic.QueryBuilder(fmt.Sprintf(listSQL, idOfKey), paramCharacter, numbered),

		CountSQL: generic.QueryBuilder(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQL: generic.QueryBuilder(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: generic.QueryBuilder(`
			DELETE FROM kine
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: generic.QueryBuilder(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: generic.QueryBuilder(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?);select SCOPE_IDENTITY()`, paramCharacter, numbered),

		InsertSQL: generic.QueryBuilder(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?); select SCOPE_IDENTITY()`, paramCharacter, numbered),

		FillSQL: generic.QueryBuilder(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?);select SCOPE_IDENTITY()`, paramCharacter, numbered),
		RevSQL: revSQL,
		ApplyLimit: func(sql string, limit int64) string {
			limitRewrite := fmt.Sprintf("SELECT TOP %d ", limit)
			sql = strings.Replace(sql, "SELECT TOP 100 PERCENT", limitRewrite, 1)
			return sql
		},
	}, err

}

func setup(db *sql.DB) error {
	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateConnector(dataSourceName string) (*mssql.Connector, error) {
	conn, err := mssql.NewConnector(dataSourceName)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := u.Query().Get("database")
	db, err := sql.Open("sqlserver", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if _, ok := err.(mssql.Error); !ok {
		return err
	}

	if err := err.(mssql.Error); err.Number != 1801 { // 1801 = database already exists
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(createDB + dbName + ":")
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		return "", fmt.Errorf("invalid dsn")
	} else {
		dataSourceName = "sqlserver://" + dataSourceName
	}

	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}

	queryMap := u.Query()
	params := url.Values{}

	if _, ok := queryMap["certificate"]; tlsInfo.CertFile != "" && !ok {
		params.Add("certificate", tlsInfo.CAFile)
	}

	if _, ok := queryMap["database"]; !ok {
		params.Add("database", "kubernetes")
	}

	for k, v := range queryMap {
		params.Add(k, v[0])
	}

	u.RawQuery = params.Encode()
	return u.String(), nil
}
