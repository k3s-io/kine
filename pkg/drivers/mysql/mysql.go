package mysql

import (
	"context"
	cryptotls "crypto/tls"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/k3s-io/kine/pkg/identity"
	"github.com/sirupsen/logrus"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
)

const (
	defaultUnixDSN = "root@unix(/var/run/mysqld/mysqld.sock)/"
	defaultHostDSN = "root@tcp(127.0.0.1)/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
			(
				id BIGINT UNSIGNED AUTO_INCREMENT,
				name VARCHAR(630) CHARACTER SET ascii,
				created INTEGER,
				deleted INTEGER,
				create_revision BIGINT UNSIGNED,
				prev_revision BIGINT UNSIGNED,
				lease INTEGER,
				value MEDIUMBLOB,
				old_value MEDIUMBLOB,
				PRIMARY KEY (id)
			);`,
		`CREATE INDEX kine_name_index ON kine (name)`,
		`CREATE INDEX kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
	}
	schemaMigrations = []string{
		`ALTER TABLE kine MODIFY COLUMN id BIGINT UNSIGNED AUTO_INCREMENT, MODIFY COLUMN create_revision BIGINT UNSIGNED, MODIFY COLUMN prev_revision BIGINT UNSIGNED`,
		// Creating an empty migration to ensure that postgresql and mysql migrations match up
		// with each other for a give value of KINE_SCHEMA_MIGRATION env var
		``,
	}
	createDB = "CREATE DATABASE IF NOT EXISTS `%s`;"
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	config, err := prepareConfig(cfg)
	if err != nil {
		return false, nil, err
	}

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return false, nil, err
	}

	if err := createDBIfNotExist(config, connector); err != nil {
		return false, nil, err
	}

	dialect, err := generic.Open(ctx, wg, "mysql", connector, cfg.ConnectionPoolConfig, "?", false, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, err
	}

	dialect.LastInsertID = true
	dialect.GetSizeSQL = `
		SELECT SUM(data_length + index_length)
		FROM information_schema.TABLES
		WHERE table_schema = DATABASE() AND table_name = 'kine'`
	dialect.CompactSQL = `
		DELETE kv FROM kine AS kv
		INNER JOIN (
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
		) AS ks
		ON kv.id = ks.id`
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*mysql.MySQLError); ok && err.Number == 1062 {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*mysql.MySQLError); ok {
			return fmt.Sprint(err.Number)
		}
		return err.Error()
	}
	if err := setup(dialect.DB); err != nil {
		return false, nil, err
	}

	dialect.Migrate(context.Background())
	return true, logstructured.New(sqllog.New(dialect, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	var exists bool
	err := db.QueryRow("SELECT 1 FROM information_schema.TABLES WHERE table_schema = DATABASE() AND table_name = ?", "kine").Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("Failed to check existence of database table %s, going to attempt create: %v", "kine", err)
	}

	if !exists {
		for _, stmt := range schema {
			logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
			if _, err := db.Exec(stmt); err != nil {
				if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1061 {
					return err
				}
			}
		}
	}

	// Run enabled schama migrations.
	// Note that the schema created by the `schema` var is always the latest revision;
	// migrations should handle deltas between prior schema versions.
	schemaVersion, _ := strconv.ParseUint(os.Getenv("KINE_SCHEMA_MIGRATION"), 10, 64)
	for i, stmt := range schemaMigrations {
		if i >= int(schemaVersion) {
			break
		}
		if stmt == "" {
			continue
		}
		logrus.Tracef("SETUP EXEC MIGRATION %d: %v", i, util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1061 {
				return err
			}
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func createDBIfNotExist(config *mysql.Config, connector driver.Connector) error {
	dbName := config.DBName
	db := sql.OpenDB(connector)
	defer db.Close()

	var exists bool
	err := db.QueryRow("SELECT 1 FROM information_schema.SCHEMATA WHERE schema_name = ?", dbName).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check existence of database %s, going to attempt create: %v", dbName, err)
	}

	if !exists {
		stmt := fmt.Sprintf(createDB, dbName)
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if _, err = db.Exec(stmt); err != nil {
			if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1049 {
				return err
			}

			createConfig := config.Clone()
			createConfig.DBName = ""
			createConnector, err := mysql.NewConnector(createConfig)
			if err != nil {
				return err
			}

			db := sql.OpenDB(createConnector)
			defer db.Close()
			if _, err = db.Exec(stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareConfig(cfg *drivers.Config) (*mysql.Config, error) {
	tlsConfig, err := cfg.BackendTLSConfig.ClientConfig()
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		tlsConfig.MinVersion = cryptotls.VersionTLS11
	}

	dataSourceName := cfg.DataSourceName
	if len(cfg.DataSourceName) == 0 {
		dataSourceName = defaultUnixDSN
		if tlsConfig != nil {
			dataSourceName = defaultHostDSN
		}
	}
	config, err := mysql.ParseDSN(dataSourceName)
	if err != nil {
		return nil, err
	}

	// setting up tlsConfig
	config.TLSConfig = "preferred"
	if tlsConfig != nil {
		if err := mysql.RegisterTLSConfig("kine", tlsConfig); err != nil {
			return nil, err
		}
		config.TLSConfig = "kine"
	}

	dbName := "kubernetes"
	if len(config.DBName) > 0 {
		dbName = config.DBName
	}
	config.DBName = dbName

	if cfg.TokenSource != nil {
		config.AllowCleartextPasswords = true
		if err := config.Apply(mysql.BeforeConnect(func(ctx context.Context, config *mysql.Config) error {
			token, err := cfg.TokenSource(ctx, identity.Config{
				Endpoint: config.Addr,
				User:     config.User,
			})
			if err != nil {
				return err
			}
			config.Passwd = token
			return nil
		})); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func init() {
	drivers.Register("mysql", New)
}
