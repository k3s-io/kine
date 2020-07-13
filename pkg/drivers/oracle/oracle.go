package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/godror/godror"

	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
)

const (
	defaultHostDSN = "system@localhost"
)

var (
	schema = []string{`
		CREATE TABLE kine (
			id NUMBER GENERATED ALWAYS as IDENTITY,
			name VARCHAR(630),
			created INTEGER,
			deleted INTEGER,
			create_revision INTEGER,
			prev_revision INTEGER,
			lease INTEGER,
			value BLOB,
			old_value BLOB,
			CONSTRAINT kine_pk PRIMARY KEY (id)
		)`,
		`CREATE INDEX kine_name_index ON kine (name)`,
		`CREATE INDEX kine_name_id_index ON kine (name,id)`,
		`CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
	}
	procedureTemplate = []string{
		"declare",
		"begin",
		"%s",
		"exception when others then",
		"if SQLCODE = -955 then null; else raise; end if;",
		"end;",
	}
)

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName)
	if err != nil {
		return nil, err
	}

	dialect, err := generic.Open(ctx, "godror", parsedDSN, "?", false)
	if err != nil {
		return nil, err
	}
	dialect.InsertReturningInto = true
	dialect.TranslateErr = func(err error) error {
		// ORA-00001: unique constraint violated
		if err, ok := err.(*godror.OraErr); ok && err.Code() == 1 {
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

func setup(db *sql.DB) error {
	var str strings.Builder

	for _, cmd := range procedureTemplate {
		if cmd != "%s" {
			str.WriteString(cmd + "\n")
		} else {
			for _, stmt := range schema {
				str.WriteString(fmt.Sprintf("execute immediate '%s';\n", stmt))
			}
		}
	}
	_, err := db.Exec(str.String())
	return err
}

func prepareDSN(dataSourceName string) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultHostDSN
	}
	config, err := godror.ParseConnString(dataSourceName)
	if err != nil {
		return "", err
	}

	parsedDSN := config.StringWithPassword()
	return parsedDSN, nil
}
