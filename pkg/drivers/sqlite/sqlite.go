package sqlite

import (
	"context"
	"database/sql"
	"os"

	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"

	// sqlite db driver
	_ "github.com/mattn/go-sqlite3"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS {{ .TableName }}  
			(
				id INTEGER primary key autoincrement,
				name INTEGER,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`,
		`CREATE INDEX IF NOT EXISTS kine_name_index ON {{ .TableName }} (name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON {{ .TableName }} (name, prev_revision)`,
	}
)

func New(dataSourceName, tableName string) (server.Backend, error) {
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared"
	}

	dialect, err := generic.Open("sqlite3", dataSourceName, tableName, "?", false)
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

	return nil
}
