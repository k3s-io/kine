//go:build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

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

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
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
		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
	}
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "sqlite3", cfg, false)
	return false, backend, err
}

func NewWithLitestream(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "litestream", cfg, true)
	return false, backend, err
}

func NewVariant(ctx context.Context, wg *sync.WaitGroup, driverName string, cfg *drivers.Config, litestream bool) (server.Backend, *generic.Generic, error) {
	dataSourceName := cfg.DataSourceName
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"
	}

	noCompactCheckpoint := strings.Contains(dataSourceName, "_kine_disable_compact_wal_checkpoint")
	noAutoCheckpoint := strings.Contains(dataSourceName, "_kine_disable_wal_autocheckpoint")

	if driverName == "litestream" {
		logrus.Infof("Litestream compatibility options enabled (all WAL checkpointing disabled)")
		noCompactCheckpoint = true
		noAutoCheckpoint = true
	}

	dialect, err := generic.Open(ctx, wg, driverName, dataSourceName, cfg.ConnectionPoolConfig, "?", false, cfg.MetricsRegisterer)
	if err != nil {
		return nil, nil, err
	}

	dialect.LastInsertID = true
	dialect.GetSizeSQL = `SELECT SUM(pgsize) FROM dbstat`
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		WHERE
			kv.id IN (
				SELECT kp.prev_revision AS id
				FROM kine AS kp
				WHERE
					kp.name != 'compact_rev_key' AND
					kp.prev_revision != 0 AND
					kp.id <= ?
				UNION
				SELECT kd.id AS id
				FROM kine AS kd
				WHERE
					kd.deleted != 0 AND
					kd.id <= ?
			)`
	if noCompactCheckpoint {
		logrus.Infof("WAL checkpoint on compact is disabled")
	} else {
		dialect.PostCompactSQL = `PRAGMA wal_checkpoint(FULL)`
	}
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

	if err := setup(dialect.DB, noCompactCheckpoint, noAutoCheckpoint); err != nil {
		return nil, nil, errors.Wrap(err, "setup db")
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), dialect, nil
}

func setup(db *sql.DB, noCheckpointing, noAutoCheckpoint bool) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	schema := append([]string{}, schema...)
	if !noCheckpointing {
		schema = append(schema, `PRAGMA wal_checkpoint(TRUNCATE)`)
	}
	if noAutoCheckpoint {
		logrus.Infof("WAL auto-checkpoint is disabled")
		schema = append(schema, `PRAGMA wal_autocheckpoint = 0`)
	}

	for _, stmt := range schema {
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
	sql.Register("litestream", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) (err error) {
			return conn.SetFileControlInt("main", sqlite3.SQLITE_FCNTL_PERSIST_WAL, 1)
		},
	})

	drivers.Register("sqlite", New)
	drivers.Register("litestream", NewWithLitestream)
	drivers.SetDefault("sqlite")
}
