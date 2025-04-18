//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func getSchema(tableName string) []string {
	quotedTableName := `"` + tableName + `"`
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + quotedTableName + `
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name INTEGER,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`,
		`CREATE INDEX IF NOT EXISTS "` + tableName + `_name_index" ON ` + quotedTableName + ` (name)`,
		`CREATE INDEX IF NOT EXISTS "` + tableName + `_name_id_index" ON ` + quotedTableName + ` (name,id)`,
		`CREATE INDEX IF NOT EXISTS "` + tableName + `_id_deleted_index" ON ` + quotedTableName + ` (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS "` + tableName + `_prev_revision_index" ON ` + quotedTableName + ` (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS "` + tableName + `_name_prev_revision_uindex" ON ` + quotedTableName + ` (name, prev_revision)`,
		`PRAGMA wal_checkpoint(TRUNCATE)`,
	}
}

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, "sqlite3", cfg)
	return false, backend, err
}

func NewVariant(ctx context.Context, driverName string, cfg *drivers.Config) (server.Backend, *generic.Generic, error) {
	dataSourceName := cfg.DataSourceName
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"
	}

	tableName := cfg.TableName
	if tableName == "" {
		tableName = "kine"
	}
	quotedTableName := `"` + tableName + `"`

	dialect, err := generic.Open(ctx, driverName, dataSourceName, cfg.ConnectionPoolConfig, "?", false, cfg.MetricsRegisterer, tableName)
	if err != nil {
		return nil, nil, err
	}

	dialect.LastInsertID = true
	dialect.GetSizeSQL = `SELECT SUM(pgsize) FROM dbstat`
	dialect.CompactSQL = `
		DELETE FROM ` + quotedTableName + ` AS kv
		WHERE
			kv.id IN (
				SELECT kp.prev_revision AS id
				FROM ` + quotedTableName + ` AS kp
				WHERE
					kp.name != 'compact_rev_key' AND
					kp.prev_revision != 0 AND
					kp.id <= ?
				UNION
				SELECT kd.id AS id
				FROM ` + quotedTableName + ` AS kd
				WHERE
					kd.deleted != 0 AND
					kd.id <= ?
			)`
	dialect.PostCompactSQL = `PRAGMA wal_checkpoint(FULL)`
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(sqlite3.Error); ok {
			return fmt.Sprint(err.ExtendedCode)
		}
		return err.Error()
	}

	if err := setup(dialect.DB, cfg.TableName); err != nil {
		return nil, nil, errors.Wrap(err, "setup db")
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

func setup(db *sql.DB, tableName string) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	for _, stmt := range getSchema(tableName) {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func init() {
	drivers.Register("sqlite", New)
	drivers.SetDefault("sqlite")
}
