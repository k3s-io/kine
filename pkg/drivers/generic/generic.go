package generic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lib/pq"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	flowcontrolv1 "k8s.io/api/flowcontrol/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxIdleConns = 2 // copied from database/sql
)

// explicit interface check
var _ server.Dialect = (*Generic)(nil)

var (
	columns = "kv.id AS theid, kv.name AS thename, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	revSQL  = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv`

	compactRevSQL = `
		SELECT MAX(crkv.prev_revision) AS prev_revision
		FROM kine AS crkv
		WHERE crkv.name = 'compact_rev_key'`

	listSQL = fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT (%s), (%s), %s
			FROM kine AS kv
			JOIN (
				SELECT MAX(mkv.id) AS id
				FROM kine AS mkv
				WHERE
					mkv.name LIKE ?
					%%s
				GROUP BY mkv.name) AS maxkv
				ON maxkv.id = kv.id
			WHERE
				kv.deleted = 0 OR
				?
		) AS lkv
		ORDER BY lkv.thename ASC
		`, revSQL, compactRevSQL, columns)

	tableName = ""

	resourcesTemplate = []string{
		"/bindings/", "/componentstatuses/", "/configmaps/", "/endpoints/", "/events/",
		"/limitranges/", "/namespaces/", "/nodes/", "/persistentvolumeclaims/", "/persistentvolumes/",
		"/pods/", "/podtemplates/", "/replicationcontrollers/", "/resourcequotas/", "/secrets/",
		"/serviceaccounts/", "/services/specs/", "/mutatingwebhookconfigurations/", "/validatingadmissionpolicies/", "/validatingadmissionpolicybindings/",
		"/validatingwebhookconfigurations/", "/customresourcedefinitions/" /*"/apiservices/",*/, "/controllerrevisions/", "/daemonsets/",
		"/deployments/", "/replicasets/", "/statefulsets/", "/selfsubjectreviews/", "/tokenreviews/",
		"/localsubjectaccessreviews/", "/selfsubjectaccessreviews/", "/selfsubjectrulesreviews/", "/subjectaccessreviews/", "/horizontalpodautoscalers/",
		"/cronjobs/", "/jobs/", "/certificatesigningrequests/", "/leases/", "/endpointslices/",
		"/flowschemas/", "/prioritylevelconfigurations/", "/helmchartconfigs/", "/helmcharts/", "/addons/",
		"/etcdsnapshotfiles/", "/ingressclasses/", "/ingresses/", "/networkpolicies/", "/runtimeclasses/",
		"/poddisruptionbudgets/", "/clusterrolebindings/", "/clusterroles/", "/rolebindings/", "/roles/",
		"/priorityclasses/", "/csidrivers/", "/csinodes/", "/csistoragecapacities/", "/storageclasses/",
		"/volumeattachments/", "/traefik.containo.us/ingressroutes/", "/traefik.containo.us/ingressroutetcps/", "/traefik.containo.us/ingressrouteudps/", "/traefik.containo.us/middlewares/",
		"/traefik.containo.us/middlewaretcps/", "/traefik.containo.us/serverstransports/", "/traefik.containo.us/tlsoptions/", "/traefik.containo.us/tlsstores/", "/traefik.containo.us/traefikservices/",
		"/traefik.io/ingressroutes/", "/traefik.io/ingressroutetcps/", "/traefik.io/ingressrouteudps/", "/traefik.io/middlewares/", "/traefik.io/middlewaretcps/",
		"/traefik.io/serverstransports/", "/serverstransporttcps/", "/traefik.io/tlsoptions/", "/traefik.io/tlsstores/", "/traefik.io/traefikservices/",
	}

	tableMap = map[string]string{
		"/bindings/": "bindings", "/componentstatuses/": "componentstatuses", "/configmaps/": "configmaps", "/endpoints/": "endpoints", "/events/": "events",
		"/limitranges/": "limitranges", "/namespaces/": "namespaces", "/minions/": "nodes", "/persistentvolumeclaims/": "persistentvolumeclaims", "/persistentvolumes/": "persistentvolumes",
		"/pods/": "pods", "/podtemplates/": "podtemplates", "/replicationcontrollers/": "replicationcontrollers", "/resourcequotas/": "resourcequotas", "/secrets/": "secrets",
		"/serviceaccounts/": "serviceaccounts", "/services/specs/": "services", "/mutatingwebhookconfigurations/": "mutatingwebhookconfigurations", "/validatingadmissionpolicies/": "validatingadmissionpolicies", "/validatingadmissionpolicybindings/": "validatingadmissionpolicybindings",
		"/validatingwebhookconfigurations/": "validatingwebhookconfigurations", "/customresourcedefinitions/": "customresourcedefinitions", "/apiservices/": "apiservices", "/controllerrevisions/": "controllerrevisions", "/daemonsets/": "daemonsets",
		"/deployments/": "deployments", "/replicasets/": "replicasets", "/statefulsets/": "statefulsets", "/selfsubjectreviews/": "selfsubjectreviews", "/tokenreviews/": "tokenreviews",
		"/localsubjectaccessreviews/": "localsubjectaccessreviews", "/selfsubjectaccessreviews/": "selfsubjectaccessreviews", "/selfsubjectrulesreviews/": "selfsubjectrulesreviews", "/subjectaccessreviews/": "subjectaccessreviews", "/horizontalpodautoscalers/": "horizontalpodautoscalers",
		"/cronjobs/": "cronjobs", "/jobs/": "jobs", "/certificatesigningrequests/": "certificatesigningrequests", "/leases/": "leases", "/endpointslices/": "endpointslices",
		"/flowschemas/": "flowschemas", "/prioritylevelconfigurations/": "prioritylevelconfigurations", "/helmchartconfigs/": "helmchartconfigs", "/helmcharts/": "helmcharts", "/addons/": "addons",
		"/etcdsnapshotfiles/": "etcdsnapshotfiles", "/ingressclasses/": "ingressclasses", "/ingresses/": "ingresses", "/networkpolicies/": "networkpolicies", "/runtimeclasses/": "runtimeclasses",
		"/poddisruptionbudgets/": "poddisruptionbudgets", "/clusterrolebindings/": "clusterrolebindings", "/clusterroles/": "clusterroles", "/rolebindings/": "rolebindings", "/roles/": "roles",
		"/priorityclasses/": "priorityclasses", "/csidrivers/": "csidrivers", "/csinodes/": "csinodes", "/csistoragecapacities/": "csistoragecapacities", "/storageclasses/": "storageclasses",
		"/volumeattachments/": "volumeattachments", "/traefik.containo.us/ingressroutes/": "traefik_containo_us_ingressroutes", "/traefik.containo.us/ingressroutetcps/": "traefik_containo_us_ingressroutetcps", "/traefik.containo.us/ingressrouteudps/": "traefik_containo_us_ingressrouteudps", "/traefik.containo.us/middlewares/": "traefik_containo_us_middlewares",
		"/traefik.containo.us/middlewaretcps/": "traefik_containo_us_middlewaretcps", "/traefik.containo.us/serverstransports/": "traefik_containo_us_serverstransports", "/traefik.containo.us/tlsoptions/": "traefik_containo_us_tlsoptions", "/traefik.containo.us/tlsstores/": "traefik_containo_us_tlsstores", "/traefik.containo.us/traefikservices/": "traefik_containo_us_traefikservices",
		"/traefik.io/ingressroutes/": "traefik_io_ingressroutes", "/traefik.io/ingressroutetcps/": "traefik_io_ingressroutetcps", "/traefik.io/ingressrouteudps/": "traefik_io_ingressrouteudps", "/traefik.io/middlewares/": "traefik_io_middlewares", "/traefik.io/middlewaretcps/": "traefik_io_middlewaretcps",
		"/traefik.io/serverstransports/": "traefik_io_serverstransports", "/serverstransporttcps/": "serverstransporttcps", "/traefik.io/tlsoptions/": "traefik_io_tlsoptions", "/traefik.io/tlsstores/": "traefik_io_tlsstores", "/traefik.io/traefikservices/": "traefik_io_traefikservices",
	}

	apiGroupMap = map[string]string{
		"/bindings/": "core", "/componentstatuses/": "core", "/configmaps/": "core", "/endpoints/": "core", "/events/": "core",
		"/limitranges/": "core", "/namespaces/": "core", "/minions/": "core", "/persistentvolumeclaims/": "core", "/persistentvolumes/": "core",
		"/pods/": "core", "/podtemplates/": "core", "/replicationcontrollers/": "core", "/resourcequotas/": "core", "/secrets/": "core",
		"/serviceaccounts/": "core", "/services/specs/": "core", "/mutatingwebhookconfigurations/": "admissionregistration.k8s.io", "/validatingadmissionpolicies/": "admissionregistration.k8s.io", "/validatingadmissionpolicybindings/": "admissionregistration.k8s.io",
		"/validatingwebhookconfigurations/": "admissionregistration.k8s.io", "/customresourcedefinitions/": "apiextensions.k8s.io", "/apiservices/": "apiextensions.k8s.io", "/controllerrevisions/": "apps", "/daemonsets/": "apps",
		"/deployments/": "apps", "/replicasets/": "apps", "/statefulsets/": "apps", "/selfsubjectreviews/": "authentication.k8s.io", "/tokenreviews/": "authentication.k8s.io",
		"/localsubjectaccessreviews/": "authorization.k8s.io", "/selfsubjectaccessreviews/": "authorization.k8s.io", "/selfsubjectrulesreviews/": "authorization.k8s.io", "/subjectaccessreviews/": "authorization.k8s.io", "/horizontalpodautoscalers/": "autoscaling",
		"/cronjobs/": "batch", "/jobs/": "batch", "/certificatesigningrequests/": "certificates.k8s.io", "/leases/": "coordination.k8s.io", "/endpointslices/": "discovery.k8s.io",
		"/flowschemas/": "flowcontrol.apiserver.k8s.io", "/prioritylevelconfigurations/": "flowcontrol.apiserver.k8s.io", "/helmchartconfigs/": "helm.cattle.io", "/helmcharts/": "helm.cattle.io", "/addons/": "k3s.cattle.io",
		"/etcdsnapshotfiles/": "k3s.cattle.io", "/ingressclasses/": "networking.k8s.io", "/ingresses/": "networking.k8s.io", "/networkpolicies/": "networking.k8s.io", "/runtimeclasses/": "node.k8s.io",
		"/poddisruptionbudgets/": "policy", "/clusterrolebindings/": "rbac.authorization.k8s.io", "/clusterroles/": "rbac.authorization.k8s.io", "/rolebindings/": "rbac.authorization.k8s.io", "/roles/": "rbac.authorization.k8s.io",
		"/priorityclasses/": "scheduling.k8s.io", "/csidrivers/": "storage.k8s.io", "/csinodes/": "storage.k8s.io", "/csistoragecapacities/": "storage.k8s.io", "/storageclasses/": "storage.k8s.io",
		"/volumeattachments/": "storage.k8s.io", "/traefik.containo.us/ingressroutes/": "traefik.containo.us", "/traefik.containo.us/ingressroutetcps/": "traefik.containo.us", "/traefik.containo.us/ingressrouteudps/": "traefik.containo.us", "/traefik.containo.us/middlewares/": "traefik.containo.us",
		"/traefik.containo.us/middlewaretcps/": "traefik.containo.us", "/traefik.containo.us/serverstransports/": "traefik.containo.us", "/traefik.containo.us/tlsoptions/": "traefik.containo.us", "/traefik.containo.us/tlsstores/": "traefik.containo.us", "/traefik.containo.us/traefikservices/": "traefik.containo.us",
		"/traefik.io/ingressroutes/": "traefik.io", "/traefik.io/ingressroutetcps/": "traefik.io", "/traefik.io/ingressrouteudps/": "traefik.io", "/traefik.io/middlewares/": "traefik.io", "/traefik.io/middlewaretcps/": "traefik.io",
		"/traefik.io/serverstransports/": "traefik.io", "/serverstransporttcps/": "traefik.io", "/traefik.io/tlsoptions/": "traefik.io", "/traefik.io/tlsstores/": "traefik.io", "/traefik.io/traefikservices/": "traefik.io",
	}
)

type ErrRetry func(error) bool
type TranslateErr func(error) error
type ErrCode func(error) string

type ConnectionPoolConfig struct {
	MaxIdle     int           // zero means defaultMaxIdleConns; negative means 0
	MaxOpen     int           // <= 0 means unlimited
	MaxLifetime time.Duration // maximum amount of time a connection may be reused
}

type Generic struct {
	sync.Mutex

	LockWrites            bool
	LastInsertID          bool
	DB                    *sql.DB
	GetCurrentSQL         string
	GetRevisionSQL        string
	RevisionSQL           string
	ListRevisionStartSQL  string
	GetRevisionAfterSQL   string
	CountCurrentSQL       string
	CountRevisionSQL      string
	AfterSQL              string
	DeleteSQL             string
	CompactSQL            string
	UpdateCompactSQL      string
	PostCompactSQL        string
	InsertSQL             string
	FillSQL               string
	InsertLastInsertIDSQL string
	GetSizeSQL            string
	Retry                 ErrRetry
	InsertRetry           ErrRetry
	TranslateErr          TranslateErr
	ErrCode               ErrCode
	FillRetryDuration     time.Duration
	//protobuf解码器
	protobufSerializer runtime.Serializer
}

// 字符串匹配，用于提取JSON中的信息
func extractValue(jsonStr, key string) (string, error) {
	// 创建匹配键值对的正则表达式
	regexPattern := fmt.Sprintf(`"%s"\s*:\s*"(.*?)"`, key)
	re := regexp.MustCompile(regexPattern)

	// 查找匹配项
	matches := re.FindStringSubmatch(jsonStr)
	if len(matches) < 2 {
		return "", fmt.Errorf("key %s not found", key)
	}
	return matches[1], nil
}

// 同样是字符串匹配，目的是提取出某条resources数据对应的表名及资源名
func containsAndReturnRemainder(str1, str2 string) (bool, string) {
	index := strings.Index(str1, str2)
	if index == -1 {
		return false, ""
	}
	remainder := str1[index+len(str2):]
	return true, remainder
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func (d *Generic) Migrate(ctx context.Context) {
	var (
		count     = 0
		countKV   = d.queryRow(ctx, "SELECT COUNT(*) FROM key_value")
		countKine = d.queryRow(ctx, "SELECT COUNT(*) FROM kine")
	)

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := d.execute(ctx,
		`INSERT INTO kine(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`)
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}

	//迁移kine中的数据到pod、service等各个表中
	if err := d.migrateData(ctx); err != nil {
		logrus.Fatalf("Data migration failed: %v", err)
	}
}

func (d *Generic) migrateData(ctx context.Context) error {
	// 正则表达式用于提取name字段中的信息
	re := regexp.MustCompile(`^/registry/([^/]+)/(.+)$`)

	// 查询kine表中的所有数据
	rows, err := d.DB.QueryContext(ctx, `SELECT name, value FROM kine`)
	if err != nil {
		return fmt.Errorf("query kine table failed: %v", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return fmt.Errorf("scan row failed: %v", err)
		}

		matches := re.FindStringSubmatch(name)
		if matches == nil {
			continue // 跳过不匹配的行
		}

		// 提取表名、命名空间和数据名称
		tableName := matches[1]

		// 插入到相应的表中
		switch tableName {
		case "pods":
			_, err := d.DB.ExecContext(ctx, `INSERT INTO pods (data) VALUES (?)`, value)
			if err != nil {
				logrus.Errorf("Insert into pods failed: %v", err)
			}
		case "deployments":
			_, err := d.DB.ExecContext(ctx, `INSERT INTO deployments (data) VALUES (?)`, value)
			if err != nil {
				logrus.Errorf("Insert into deployments failed: %v", err)
			}
		case "services":
			_, err := d.DB.ExecContext(ctx, `INSERT INTO services (data) VALUES (?)`, value)
			if err != nil {
				logrus.Errorf("Insert into services failed: %v", err)
			}
		default:
			logrus.Warnf("Unknown table name: %s", tableName)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration failed: %v", err)
	}

	return nil
}

func configureConnectionPooling(connPoolConfig ConnectionPoolConfig, db *sql.DB, driverName string) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring %s database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", driverName, connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
}

func openAndTest(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, driverName, dataSourceName string, connPoolConfig ConnectionPoolConfig, paramCharacter string, numbered bool, metricsRegisterer prometheus.Registerer) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(connPoolConfig, db, driverName)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "kine"))
	}

	// 初始化解码器
	// 初始化 Scheme
	myScheme := runtime.NewScheme()

	// 注册 Kubernetes 核心对象
	if err := corev1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add Kubernetes core types to scheme: %v", err)
	}

	// 注册 Kubernetes apps/v1 对象
	if err := appsv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add Kubernetes apps/v1 types to scheme: %v", err)
	}

	// 注册 Kubernetes batch/v1 对象
	if err := batchv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add Kubernetes batch/v1 types to scheme: %v", err)
	}

	// 注册 Kubernetes rbac/v1 对象
	if err := rbacv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add Kubernetes rbac/v1 types to scheme: %v", err)
	}

	if err := apiextensionsv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add apiextensions/v1 types to scheme: %v", err)
	}
	if err := apiregistrationv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add apiregistration/v1 types to scheme: %v", err)
	}
	if err := flowcontrolv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add flowcontrol/v1 types to scheme: %v", err)
	}
	if err := coordinationv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add coordination/v1 types to scheme: %v", err)
	}
	if err := discoveryv1.AddToScheme(myScheme); err != nil {
		log.Fatalf("Failed to add discovery/v1 types to scheme: %v", err)
	}

	// 初始化 CodecFactory
	codecFactory := serializer.NewCodecFactory(myScheme)

	// 获取 Protobuf 序列化器
	serializerInfo, ok := runtime.SerializerInfoForMediaType(codecFactory.SupportedMediaTypes(), runtime.ContentTypeProtobuf)
	if !ok {
		log.Fatalf("No Protobuf serializer found")
	}
	protobufSerializer := serializerInfo.Serializer

	return &Generic{
		DB:                 db,
		protobufSerializer: protobufSerializer,
		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM kine AS kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, "AND mkv.name > ?"), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(fmt.Sprintf(listSQL, "AND mkv.name > ? AND mkv.id <= ?"), paramCharacter, numbered),

		CountCurrentSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "AND mkv.name > ?")), paramCharacter, numbered),

		CountRevisionSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "AND mkv.name > ? AND mkv.id <= ?")), paramCharacter, numbered),

		AfterSQL: q(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine AS kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM kine AS kv
			WHERE kv.id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
	}, err
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{}) (result *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), util.Stripped(sql), args)
	}()
	return d.DB.QueryContext(ctx, sql, args...)
}

func (d *Generic) queryRow(ctx context.Context, sql string, args ...interface{}) (result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, util.Stripped(sql))
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(result.Err()), util.Stripped(sql), args)
	}()
	return d.DB.QueryRowContext(ctx, sql, args...)
}

func (d *Generic) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for i := uint(0); i < 20; i++ {
		logrus.Tracef("EXEC (try: %d) %v : %s", i, args, util.Stripped(sql))
		startTime := time.Now()
		result, err = d.DB.ExecContext(ctx, sql, args...)
		metrics.ObserveSQL(startTime, d.ErrCode(err), util.Stripped(sql), args)
		if err != nil && d.Retry != nil && d.Retry(err) {
			wait(i)
			continue
		}
		return result, err
	}
	return
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("SETCOMPACTREVISION %v", revision)
	_, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	return err
}

func (d *Generic) Compact(ctx context.Context, revision int64) (int64, error) {
	logrus.Tracef("COMPACT %v", revision)
	res, err := d.execute(ctx, d.CompactSQL, revision, revision)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (d *Generic) PostCompact(ctx context.Context) error {
	logrus.Trace("POSTCOMPACT")
	if d.PostCompactSQL != "" {
		_, err := d.execute(ctx, d.PostCompactSQL)
		return err
	}
	return nil
}

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, d.GetRevisionSQL, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, startKey, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, startKey, revision, includeDeleted)
}

func (d *Generic) CountCurrent(ctx context.Context, prefix, startKey string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountCurrentSQL, prefix, startKey, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountRevisionSQL, prefix, startKey, revision, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *Generic) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	if d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = d.TranslateErr(err)
			}
		}()
	}

	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}

		id, err = row.LastInsertId()
		if err != nil {
			return 0, err
		}
	} else {
		// Drivers without LastInsertID support may conflict on the serial id key when inserting rows,
		// as the ID is reserved at the beginning of the implicit transaction, but does not become
		// visible until the transaction completes, at which point we may have already created a gap fill record.
		// Retry the insert if the driver indicates a retriable insert error, to avoid presenting a spurious
		// duplicate key error to the client.
		wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
		for i := uint(0); i < 20; i++ {
			row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
			err = row.Scan(&id)
			if err != nil && d.InsertRetry != nil && d.InsertRetry(err) {
				wait(i)
				continue
			}
			break
		}
		if err != nil {
			return 0, err
		}
	}

	resourceName := ""
	namespace := ""
	apigroup := ""
	region := ""
	creationTime := ""

	for _, resource := range resourcesTemplate {
		if found, remainder := containsAndReturnRemainder(key, resource); found {
			tableName = tableMap[resource]
			resourceName = remainder
			apigroup = apiGroupMap[resource]
			break
		}
	}

	//如果没匹配到对应的resources，则直接返回，不需要进行后续操作
	if resourceName == "" {
		return id, nil
	}

	if dVal == 1 {
		// 定义删除语句
		deleteQuery := `
						DELETE FROM %s WHERE name = $1
						`

		formattedDeleteQuery := fmt.Sprintf(deleteQuery, pq.QuoteIdentifier(tableName))

		// 执行删除
		_, err = d.execute(ctx, formattedDeleteQuery, resourceName)
		if err != nil {
			panic(err)
		}
	} else {

		encodedData := value

		fmt.Println("正在解码：", tableName)

		// 解码 Protobuf 数据
		gvk := &schema.GroupVersionKind{} // 替换为实际的 GVK
		obj, _, err := d.protobufSerializer.Decode(encodedData, gvk, nil)
		if err != nil {
			log.Fatalf("Failed to decode protobuf: %v", err)
		}

		// 将解码后的对象转换为 JSON 格式
		jsonData, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal JSON: %v", err)
		}

		namespace, err = extractValue(string(jsonData), "namespace")
		if err != nil {
			namespace = "no-namespace"
		}
		region, err = extractValue(string(jsonData), "nodeName")
		if err != nil {
			region = "no-region"
		}
		creationTime, err = extractValue(string(jsonData), "creationTimestamp")
		if err != nil {
			creationTime = "no-creationTime"
		}

		// 获取当前时间
		currentTime := time.Now().UTC()

		// 格式化时间
		formattedTime := currentTime.Format("2006-01-02T15:04:05Z")

		// 如果是创建操作
		if cVal == 1 {
			// 定义创建语句
			insertQuery := `
    						INSERT INTO %s (name, namespace, apigroup, region, data, created_time, update_time)
    						VALUES ($1, $2, $3, $4, $5, $6, $7)
    						`
			formattedInsertQuery := fmt.Sprintf(insertQuery, pq.QuoteIdentifier(tableName))

			// 执行插入
			_, err = d.execute(ctx, formattedInsertQuery, resourceName, namespace, apigroup, region, jsonData, creationTime, creationTime)
			if err != nil {
				panic(err)
			}

		} else {
			// 定义更新语句
			updateQuery := `
							UPDATE %s SET namespace = $1, region = $2,data = $3, update_time = $4 WHERE name = $5
							`

			formattedUpdateQuery := fmt.Sprintf(updateQuery, pq.QuoteIdentifier(tableName))

			// 执行更新
			_, err = d.execute(ctx, formattedUpdateQuery, namespace, region, jsonData, formattedTime, resourceName)
			if err != nil {
				panic(err)
			}
		}

	}

	return id, err
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}
	var size int64
	row := d.queryRow(ctx, d.GetSizeSQL)
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func (d *Generic) FillRetryDelay(ctx context.Context) {
	time.Sleep(d.FillRetryDuration)
}
