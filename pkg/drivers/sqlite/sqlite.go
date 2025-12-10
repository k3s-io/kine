package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
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
		`PRAGMA wal_checkpoint(TRUNCATE)`,
	}
)

func getDataSourceName(dsn string) (string, error) {
	dsnUrl, _ := url.Parse(dsn)
	if len(dsnUrl.RawPath) == 0 {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return dsn, err
		}
		dsnUrl.Path = "./db/state.db"
	}
	query := dsnUrl.Query()
	query.Set("cache", "shared")
	query.Add("_pragma", "journal_mode(wal)")
	query.Add("_pragma", "busy_timeout(30000)")
	query.Add("_pragma", "synchronous(normal)")
	query.Set("_txlock", "immediate") 
	dsnUrl.RawQuery = query.Encode()
	return dsnUrl.String(), nil
}

func NewVariant(ctx context.Context, wg *sync.WaitGroup, driverName string, cfg *drivers.Config) (server.Backend, *generic.Generic, error) {
	dataSourceName, err := getDataSourceName(cfg.DataSourceName)
	if err != nil {
		return nil, nil, err
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
	dialect.PostCompactSQL = `PRAGMA wal_checkpoint(FULL)`
	dialect.TranslateErr = translateErr
	dialect.ErrCode = errorCode

	if err := setup(dialect.DB); err != nil {
		return nil, nil, errors.Wrap(err, "setup db")
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), dialect, nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

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
	drivers.Register("sqlite", New)
	drivers.SetDefault("sqlite")
}
