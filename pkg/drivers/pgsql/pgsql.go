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
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/lib/pq"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
 			(
				id BIGSERIAL PRIMARY KEY,
				name VARCHAR(630),
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
	}
	schemaMigrations = []string{
		`ALTER TABLE kine ALTER COLUMN id SET DATA TYPE BIGINT, ALTER COLUMN create_revision SET DATA TYPE BIGINT, ALTER COLUMN prev_revision SET DATA TYPE BIGINT; ALTER SEQUENCE kine_id_seq AS BIGINT`,
	}
	createDB = "CREATE DATABASE "
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return nil, err
	}

	dialect, err := generic.Open(ctx, "pgx", parsedDSN, connPoolConfig, "$", true, metricsRegisterer)
	if err != nil {
		return nil, err
	}
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
		return nil, err
	}

	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}

func createResourceTable(db *sql.DB, tableName string) error {

	createTableQuery := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        name VARCHAR(256),
        namespace VARCHAR(64),
        apigroup VARCHAR(128),
        region VARCHAR(128),
        data JSON,
        created_time TIMESTAMPTZ,
        update_time TIMESTAMPTZ,
        PRIMARY KEY (name)
    );`, pq.QuoteIdentifier(tableName))

	if _, err := db.Exec(createTableQuery); err != nil {
		return err
	}

	// 创建索引
	createIndexQueries := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_name_index ON %s (name);`, tableName, pq.QuoteIdentifier(tableName)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_name_created_time_index ON %s (name, created_time);`, tableName, pq.QuoteIdentifier(tableName)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_name_update_time_index ON %s (name, update_time);`, tableName, pq.QuoteIdentifier(tableName)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_namespace_created_time_index ON %s (namespace, created_time);`, tableName, pq.QuoteIdentifier(tableName)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_namespace_update_time_index ON %s (namespace, update_time);`, tableName, pq.QuoteIdentifier(tableName)),
	}

	for _, query := range createIndexQueries {
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	// 创建特定资源的表和索引
	// 注册所有的api-resources,为他们创建表格
	resources := []string{
		"bindings", "componentstatuses", "configmaps", "endpoints", "events",
		"limitranges", "namespaces", "nodes", "persistentvolumeclaims", "persistentvolumes",
		"pods", "podtemplates", "replicationcontrollers", "resourcequotas", "secrets",
		"serviceaccounts", "services", "mutatingwebhookconfigurations", "validatingadmissionpolicies", "validatingadmissionpolicybindings",
		"validatingwebhookconfigurations", "customresourcedefinitions", "apiservices", "controllerrevisions", "daemonsets",
		"deployments", "replicasets", "statefulsets", "selfsubjectreviews", "tokenreviews",
		"localsubjectaccessreviews", "selfsubjectaccessreviews", "selfsubjectrulesreviews", "subjectaccessreviews", "horizontalpodautoscalers",
		"cronjobs", "jobs", "certificatesigningrequests", "leases", "endpointslices",
		"events_k8s_io_events", "flowschemas", "prioritylevelconfigurations", "helmchartconfigs", "helmcharts",
		"addons", "etcdsnapshotfiles", "metrics_k8s_io_nodes", "metrics_k8s_io_pods", "ingressclasses",
		"ingresses", "networkpolicies", "runtimeclasses", "poddisruptionbudgets", "clusterrolebindings",
		"clusterroles", "rolebindings", "roles", "priorityclasses", "csidrivers",
		"csinodes", "csistoragecapacities", "storageclasses", "volumeattachments", "traefik_containo_us_ingressroutes",
		"traefik_containo_us_ingressroutetcps", "traefik_containo_us_ingressrouteudps", "traefik_containo_us_middlewares", "traefik_containo_us_middlewaretcps", "traefik_containo_us_serverstransports",
		"traefik_containo_us_tlsoptions", "traefik_containo_us_tlsstores", "traefik_containo_us_traefikservices", "traefik_io_ingressroutes", "traefik_io_ingressroutetcps",
		"traefik_io_ingressrouteudps", "traefik_io_middlewares", "traefik_io_middlewaretcps", "traefik_io_serverstransports", "serverstransporttcps",
		"traefik_io_tlsoptions", "traefik_io_tlsstores", "traefik_io_traefikservices",
	}
	for _, resource := range resources {
		err := createResourceTable(db, resource)
		if err != nil {
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
		logrus.Tracef("SETUP EXEC MIGRATION %d: %v", i, util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := strings.SplitN(u.Path, "/", 2)[1]
	u.Path = "/postgres"
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		logrus.Warnf("failed to ensure existence of database %s: unable to connect to default postgres database: %v", dbName, err)
		return nil
	}
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", dbName).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check existence of database %s, going to attempt create: %v", dbName, err)
	}

	stmt := createDB + dbName + ";"

	if !exists {
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
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

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
