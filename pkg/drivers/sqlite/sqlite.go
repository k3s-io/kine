package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

var (
	DefaultParams = "_journal_mode=WAL&_busy_timeout=30000&_synchronous=NORMAL&_txlock=immediate&_stmt_cache_size=20&cache=shared"
	DefaultDSN    = "./db/state.db?" + DefaultParams

	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT,
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
		`CREATE INDEX IF NOT EXISTS kine_id_compact_rev_key_with_prev_revision_index ON kine(id, name, prev_revision) WHERE name != 'compact_rev_key' AND prev_revision != 0`,
	}
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "sqlite3", cfg)
	return false, backend, err
}

func NewWithLitestream(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "litestream", cfg)
	return false, backend, err
}

func NewVariant(ctx context.Context, wg *sync.WaitGroup, driverName string, cfg *drivers.Config) (server.Backend, *generic.Generic, error) {
	if cfg.DataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		cfg.DataSourceName = DefaultDSN
	}

	noCompactCheckpoint := strings.Contains(cfg.DataSourceName, "_kine_disable_compact_wal_checkpoint")
	noAutoCheckpoint := strings.Contains(cfg.DataSourceName, "_kine_disable_wal_autocheckpoint")
	noStartupVacuum := strings.Contains(cfg.DataSourceName, "_kine_disable_startup_vacuum")

	if driverName == "litestream" {
		logrus.Infof("Litestream compatibility options enabled (all WAL checkpointing and startup VACUUM disabled)")
		noCompactCheckpoint = true
		noAutoCheckpoint = true
		noStartupVacuum = true
	}

	connector, err := newConnector(driverName, cfg.DataSourceName)
	if err != nil {
		return nil, nil, err
	}

	dialect, err := generic.OpenDB(ctx, wg, driverName, connector, cfg.ConnectionPoolConfig, "?", false, cfg.MetricsRegisterer)
	if err != nil {
		return nil, nil, err
	}

	dialect.LastInsertID = true
	dialect.GetSizeSQL = query.New(`
		SELECT (page_count - freelist_count) * page_size
		FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()`,
		"?", false, "GetSize")
	dialect.CompactSQL = query.New(`
		DELETE FROM kine AS kv
		WHERE kv.id IN (
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
					kd.id <= ?)`,
		"?", false, "Compact")
	if noCompactCheckpoint {
		logrus.Infof("WAL checkpoint on compact is disabled")
	} else {
		dialect.PostCompactSQL = postCompact()
	}
	dialect.TranslateErr = translateErr
	dialect.ErrCode = errCode

	if err := setup(dialect.DB, noCompactCheckpoint, noAutoCheckpoint, noStartupVacuum); err != nil {
		return nil, nil, fmt.Errorf("setup db: %w", err)
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), dialect, nil
}

func setup(db *sql.DB, noCheckpointing, noAutoCheckpoint, noStartupVacuum bool) error {
	logrus.Infof("Kine built with sqlite from %s", version())
	logrus.Info("Configuring database table schema and indexes, this may take a moment...")

	schema := append([]string{}, schema...)
	if !noCheckpointing {
		schema = append(schema, `PRAGMA wal_checkpoint(TRUNCATE)`)
	}
	if noAutoCheckpoint {
		logrus.Infof("WAL auto-checkpoint is disabled")
		schema = append(schema, `PRAGMA wal_autocheckpoint = 0`)
	}

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", query.Strip(stmt))
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")

	if noStartupVacuum {
		logrus.Infof("Startup VACUUM is disabled")
	} else {
		logrus.Infof("Running startup VACUUM to reclaim disk space, this may take a moment...")

		if _, err := db.Exec(`VACUUM`); err != nil {
			logrus.Warnf("Startup VACUUM failed (non-fatal): %v", err)
		} else {
			logrus.Infof("Startup VACUUM completed successfully")
		}
	}

	return nil
}

func init() {
	drivers.Register("sqlite", New)
	drivers.Register("litestream", NewWithLitestream)
	drivers.SetDefault("sqlite")
}

// sqliteConnector wraps SQLiteDriver and a DSN with methods to satisfy the driver.Connector interface
type sqliteConnector struct {
	driver driver.Driver
	dsn    string
}

func (c *sqliteConnector) Connect(_ context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

func (c *sqliteConnector) Driver() driver.Driver {
	return c.driver
}
