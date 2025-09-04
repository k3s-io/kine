package mysql

import (
	"context"
	cryptotls "crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
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

var createDB = "CREATE DATABASE IF NOT EXISTS `%s`;"

func getSchema(tableName string) []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS "` + tableName + `"
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
		`CREATE INDEX "` + tableName + `_name_index" ON "` + tableName + `" (name)`,
		`CREATE INDEX "` + tableName + `_name_id_index" ON "` + tableName + `" (name,id)`,
		`CREATE INDEX "` + tableName + `_id_deleted_index" ON "` + tableName + `" (id,deleted)`,
		`CREATE INDEX "` + tableName + `_prev_revision_index" ON "` + tableName + `" (prev_revision)`,
		`CREATE UNIQUE INDEX "` + tableName + `_name_prev_revision_uindex" ON "` + tableName + `" (name, prev_revision)`,
	}
}

func getSchemaMigrations(tableName string) []string {
	return []string{
		`ALTER TABLE "` + tableName + `" MODIFY COLUMN id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL UNIQUE, MODIFY COLUMN create_revision BIGINT UNSIGNED, MODIFY COLUMN prev_revision BIGINT UNSIGNED`,
		// Creating an empty migration to ensure that postgresql and mysql migrations match up
		// with each other for a give value of KINE_SCHEMA_MIGRATION env var
		``,
	}
}

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	tlsConfig, err := cfg.BackendTLSConfig.ClientConfig()
	if err != nil {
		return false, nil, err
	}

	if tlsConfig != nil {
		tlsConfig.MinVersion = cryptotls.VersionTLS11
	}

	parsedDSN, err := prepareDSN(cfg.DataSourceName, tlsConfig)
	if err != nil {
		return false, nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return false, nil, err
	}

	tableName := cfg.TableName
	if tableName == "" {
		tableName = "kine"
	}

	dialect, err := generic.Open(ctx, "mysql", parsedDSN, cfg.ConnectionPoolConfig, "?", false, cfg.MetricsRegisterer, tableName)
	if err != nil {
		return false, nil, err
	}

	dialect.LastInsertID = true
	dialect.GetSizeSQL = `
		SELECT SUM(data_length + index_length)
		FROM information_schema.TABLES
		WHERE table_schema = DATABASE() AND table_name = '` + tableName + `'`
	dialect.CompactSQL = `
		DELETE kv FROM "` + tableName + `" AS kv
		INNER JOIN (
			SELECT kp.prev_revision AS id
			FROM "` + tableName + `" AS kp
			WHERE
				kp.name != 'compact_rev_key' AND
				kp.prev_revision != 0 AND
				kp.id <= ?
			UNION
			SELECT kd.id AS id
			FROM "` + tableName + `" AS kd
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
	if err := setup(dialect.DB, tableName); err != nil {
		return false, nil, err
	}

	dialect.Migrate(context.Background())
	return true, logstructured.New(sqllog.New(dialect, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), nil
}

func setup(db *sql.DB, tableName string) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	var exists bool
	err := db.QueryRow("SELECT 1 FROM information_schema.TABLES WHERE table_schema = DATABASE() AND table_name = ?", tableName).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("Failed to check existence of database table %s, going to attempt create: %v", tableName, err)
	}

	if !exists {
		for _, stmt := range getSchema(tableName) {
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
	for i, stmt := range getSchemaMigrations(tableName) {
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
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT 1 FROM information_schema.SCHEMATA WHERE schema_name = ?", dbName).Scan(&exists)
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
			config.DBName = ""
			db, err = sql.Open("mysql", config.FormatDSN())
			if err != nil {
				return err
			}
			defer db.Close()
			if _, err = db.Exec(stmt); err != nil {
				return err
			}
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

	// ensure that ASNI_QUOTES is set in sql_mode
	// this is required for using "" for quoting identifiers
	// https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_ansi_quotes
	if config.Params == nil {
		config.Params = map[string]string{}
	}

	if mode, exists := config.Params["sql_mode"]; exists {
		// check if ANSI_QUOTES is already set
		if !strings.Contains(strings.ToUpper(mode), "ANSI_QUOTES") {
			if mode == "" {
				config.Params["sql_mode"] = "ANSI_QUOTES"
			} else {
				config.Params["sql_mode"] = mode + ",ANSI_QUOTES"
			}
		}
	} else {
		config.Params["sql_mode"] = "ANSI_QUOTES"
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

func init() {
	drivers.Register("mysql", New)
}
