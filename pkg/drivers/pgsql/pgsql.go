package pgsql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/identity"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
 			(
				id BIGSERIAL PRIMARY KEY,
				name text COLLATE "C",
				created INTEGER,
				deleted INTEGER,
				create_revision BIGINT,
				prev_revision BIGINT,
 				lease INTEGER,
 				value bytea,
 				old_value bytea
 			);`,

		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		`CREATE INDEX IF NOT EXISTS kine_list_query_index on kine(name, id DESC, deleted)`,
	}
	schemaMigrations = []string{
		`ALTER TABLE kine ALTER COLUMN id SET DATA TYPE BIGINT, ALTER COLUMN create_revision SET DATA TYPE BIGINT, ALTER COLUMN prev_revision SET DATA TYPE BIGINT; ALTER SEQUENCE kine_id_seq AS BIGINT`,
		// It is important to set the collation to "C" to ensure that LIKE and COMPARISON
		// queries use the index.
		`ALTER TABLE kine ALTER COLUMN name SET DATA TYPE TEXT COLLATE "C" USING name::TEXT COLLATE "C"`,
	}
	createDB = `CREATE DATABASE "%s";`
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	parsedDSN, err := prepareDSN(cfg.DataSourceName, cfg.BackendTLSConfig)
	if err != nil {
		return false, nil, err
	}

	config, err := pgxpool.ParseConfig(parsedDSN)
	if err != nil {
		return false, nil, err
	}

	if err := createDBIfNotExist(cfg, config); err != nil {
		return false, nil, err
	}

	connector := stdlib.GetConnector(*config.ConnConfig, prepareOptions(cfg)...)
	dialect, err := generic.Open(ctx, wg, "pgx", connector, cfg.ConnectionPoolConfig, "$", true, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, err
	}
	columns := "kv.id AS theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease"
	withVal := columns + ", kv.value"
	listFmt := `
		SELECT
			(SELECT MAX(rkv.id) AS id FROM kine AS rkv),
			(SELECT MAX(crkv.prev_revision) AS prev_revision FROM kine AS crkv WHERE crkv.name = 'compact_rev_key'),
			maxkv.*
		FROM (
			SELECT DISTINCT ON (name)
				%s
			FROM
				kine AS kv
			WHERE
				kv.name LIKE ? 
				%%s
			ORDER BY kv.name, theid DESC
		) AS maxkv
		WHERE
			maxkv.deleted = 0 OR ?
		ORDER BY maxkv.name, maxkv.theid DESC
	`
	listSQL := fmt.Sprintf(listFmt, columns)
	listValSQL := fmt.Sprintf(listFmt, withVal)

	countSQL := `
		SELECT
			(SELECT MAX(rkv.id) AS id FROM kine AS rkv),
			COUNT(c.theid)
		FROM (
			SELECT DISTINCT ON (name)
				kv.id AS theid, kv.deleted
			FROM kine AS kv
			WHERE
				kv.name LIKE ?
				%s
			ORDER BY kv.name, theid DESC
			) AS c
		WHERE c.deleted = 0 OR ?
		`
	dialect.GetSizeSQL = `SELECT pg_total_relation_size('kine')`
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		USING	(
			SELECT kp.prev_revision AS id
			FROM kine AS kp
			WHERE
				kp.name != 'compact_rev_key' AND
				kp.prev_revision != 0 AND
				kp.id <= $1
			UNION
			SELECT kd.id AS id
			FROM kine AS kd
			WHERE
				kd.deleted != 0 AND
				kd.id <= $2
		) AS ks
		WHERE kv.id = ks.id`
	dialect.GetCurrentSQL = q(fmt.Sprintf(listSQL, "AND kv.name > ?"))
	dialect.GetCurrentValSQL = q(fmt.Sprintf(listValSQL, "AND kv.name > ?"))
	dialect.ListRevisionStartSQL = q(fmt.Sprintf(listSQL, "AND kv.id <= ?"))
	dialect.ListRevisionStartValSQL = q(fmt.Sprintf(listValSQL, "AND kv.id <= ?"))
	dialect.GetRevisionAfterSQL = q(fmt.Sprintf(listSQL, "AND kv.name > ? AND kv.id <= ?"))
	dialect.GetRevisionAfterValSQL = q(fmt.Sprintf(listValSQL, "AND kv.name > ? AND kv.id <= ?"))
	dialect.CountCurrentSQL = q(fmt.Sprintf(countSQL, "AND kv.name > ?"))
	dialect.CountRevisionSQL = q(fmt.Sprintf(countSQL, "AND kv.name > ? AND kv.id <= ?"))
	dialect.FillRetryDuration = time.Millisecond + 5
	dialect.InsertRetry = func(err error) bool {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation && err.ConstraintName == "kine_pkey" {
			return true
		}
		return false
	}
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*pgconn.PgError); ok {
			return err.Code
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
	var version string
	collationSupported := true
	if err := db.QueryRow("select version()").Scan(&version); err == nil && strings.Contains(strings.ToLower(version), "cockroachdb") {
		// CockroadDB does not seem to support "C" as a collation
		// It looks like it's using golang.org/x/text/language and ends up calling something like v, err := language.Parse("C")
		// which parses it as a BCP47 language tag instead of a collation.
		collationSupported = false
	}

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
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
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		if stmt == "" {
			continue
		}
		logrus.Tracef("SETUP EXEC MIGRATION %d: %v", i, util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func createDBIfNotExist(cfg *drivers.Config, config *pgxpool.Config) error {
	dbName := config.ConnConfig.Database

	postgresConfig := config.Copy()
	postgresConfig.ConnConfig.Database = "postgres"
	db := stdlib.OpenDB(*postgresConfig.ConnConfig, prepareOptions(cfg)...)
	defer db.Close()

	var exists bool
	err := db.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", dbName).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check existence of database %s, going to attempt create: %v", dbName, err)
	}

	if !exists {
		stmt := fmt.Sprintf(createDB, dbName)
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if _, err = db.Exec(stmt); err != nil {
			logrus.Warnf("failed to create database %s: %v", dbName, err)
		} else {
			logrus.Tracef("created database: %s", dbName)
		}
	}
	return nil
}

func q(sql string) string {
	regex := regexp.MustCompile(`\?`)
	pref := "$"
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return pref + strconv.Itoa(n)
	})
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "postgres://" + dataSourceName
	}
	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	// makes quoting database and schema reference the same unquoted identifier
	// See: https://www.postgresql.org/docs/12/sql-syntax-lexical.html#:~:text=unquoted%20names%20are%20always%20folded%20to%20lower%20case
	u.Path = strings.ToLower(u.Path)

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	// set up tls dsn
	params := url.Values{}
	sslmode := ""
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()
	return u.String(), nil
}

func prepareOptions(cfg *drivers.Config) []stdlib.OptionOpenDB {
	var opts []stdlib.OptionOpenDB
	if cfg.TokenSource != nil {
		opts = append(opts, stdlib.OptionBeforeConnect(func(ctx context.Context, config *pgx.ConnConfig) error {
			token, err := cfg.TokenSource(ctx, identity.Config{
				Endpoint: config.Host + ":" + strconv.Itoa(int(config.Port)),
				User:     config.User,
			})
			if err != nil {
				return err
			}
			config.Password = token
			return nil
		}))
	}
	return opts
}

func init() {
	drivers.Register("postgres", New)
	drivers.Register("postgresql", New)
}
