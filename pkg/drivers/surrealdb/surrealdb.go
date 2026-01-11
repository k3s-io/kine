package surrealdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

const (
	defaultDSN = "ws://127.0.0.1:8000/kine/kubernetes"
)

var (
	schema = []string{
		`DEFINE TABLE IF NOT EXISTS kine SCHEMAFULL`,
		`DEFINE FIELD IF NOT EXISTS name ON kine TYPE string`,
		`DEFINE FIELD IF NOT EXISTS created ON kine TYPE number`,
		`DEFINE FIELD IF NOT EXISTS deleted ON kine TYPE number`,
		`DEFINE FIELD IF NOT EXISTS create_revision ON kine TYPE number`,
		`DEFINE FIELD IF NOT EXISTS prev_revision ON kine TYPE number`,
		`DEFINE FIELD IF NOT EXISTS lease ON kine TYPE number`,
		`DEFINE FIELD IF NOT EXISTS value ON kine TYPE string FLEXIBLE`,
		`DEFINE FIELD IF NOT EXISTS old_value ON kine TYPE string FLEXIBLE`,
		`DEFINE INDEX IF NOT EXISTS idx_kine_name ON kine FIELDS name`,
		`DEFINE INDEX IF NOT EXISTS idx_kine_name_id ON kine FIELDS name, id`,
		`DEFINE INDEX IF NOT EXISTS idx_kine_id_deleted ON kine FIELDS id, deleted`,
		`DEFINE INDEX IF NOT EXISTS idx_kine_prev_revision ON kine FIELDS prev_revision`,
		`DEFINE INDEX IF NOT EXISTS idx_kine_name_prev_revision ON kine FIELDS name, prev_revision UNIQUE`,
	}
)

// KineRecord represents a record in the kine table
type KineRecord struct {
	ID             string `json:"id,omitempty"`
	Name           string `json:"name"`
	Created        int64  `json:"created"`
	Deleted        int64  `json:"deleted"`
	CreateRevision int64  `json:"create_revision"`
	PrevRevision   int64  `json:"prev_revision"`
	Lease          int64  `json:"lease"`
	Value          string `json:"value"`
	OldValue       string `json:"old_value,omitempty"`
}

type surrealDBBackend struct {
	db        *surrealdb.DB
	cfg       *drivers.Config
	mu        sync.RWMutex
	namespace string
	database  string
	nextID    int64
	idMu      sync.Mutex
}

func parseDSN(dsn string) (endpoint, namespace, database, username, password string, err error) {
	parts := strings.Split(dsn, "://")
	if len(parts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid DSN format")
	}

	protocol := parts[0]
	remainder := parts[1]

	queryIdx := strings.Index(remainder, "?")
	var query string
	if queryIdx > 0 {
		query = remainder[queryIdx+1:]
		remainder = remainder[:queryIdx]
	}

	hostParts := strings.SplitN(remainder, "/", 2)
	if len(hostParts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid DSN format, expected protocol://host:port/namespace/database")
	}

	endpoint = fmt.Sprintf("%s://%s", protocol, hostParts[0])

	nsParts := strings.Split(hostParts[1], "/")
	if len(nsParts) != 2 {
		return "", "", "", "", "", fmt.Errorf("namespace and database required")
	}

	namespace = nsParts[0]
	database = nsParts[1]

	username = "root"
	password = "root"

	if query != "" {
		params := strings.Split(query, "&")
		for _, param := range params {
			kv := strings.SplitN(param, "=", 2)
			if len(kv) == 2 {
				switch kv[0] {
				case "user", "username":
					username = kv[1]
				case "pass", "password":
					password = kv[1]
				}
			}
		}
	}

	if envUser := os.Getenv("SURREALDB_USER"); envUser != "" {
		username = envUser
	}
	if envPass := os.Getenv("SURREALDB_PASS"); envPass != "" {
		password = envPass
	}

	return endpoint, namespace, database, username, password, nil
}

// execQuery executes a raw SurrealQL query using reflection to access the connection's Send method
func (s *surrealDBBackend) execQuery(ctx context.Context, query string, vars map[string]interface{}) ([]map[string]interface{}, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	// Use reflection to access the unexported connection field
	dbValue := reflect.ValueOf(db).Elem()
	connField := dbValue.FieldByName("conn")

	if !connField.IsValid() {
		return nil, fmt.Errorf("unable to access connection field")
	}

	// Get the connection interface
	conn := connField.Interface()

	// Use reflection to call Send method
	connValue := reflect.ValueOf(conn)
	sendMethod := connValue.MethodByName("Send")

	if !sendMethod.IsValid() {
		return nil, fmt.Errorf("unable to access Send method")
	}

	// Prepare parameters
	params := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf("query"),
		reflect.ValueOf(query),
	}

	if vars != nil {
		params = append(params, reflect.ValueOf(vars))
	}

	// Call Send method
	results := sendMethod.Call(params)
	if len(results) != 2 {
		return nil, fmt.Errorf("unexpected return values from Send")
	}

	// Check for error
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	// Parse response
	response := results[0].Interface()

	// Convert response to JSON and parse
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	var result struct {
		Result []map[string]interface{} `json:"result"`
	}

	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		// Try alternate format
		var altResult []map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &altResult); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}
		return altResult, nil
	}

	return result.Result, nil
}

// New creates a new SurrealDB backend
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	dataSourceName := cfg.DataSourceName
	if dataSourceName == "" {
		dataSourceName = defaultDSN
	}

	endpoint, namespace, database, username, password, err := parseDSN(dataSourceName)
	if err != nil {
		return false, nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	logrus.Infof("Connecting to SurrealDB at %s (namespace: %s, database: %s)", endpoint, namespace, database)

	db, err := surrealdb.New(endpoint)
	if err != nil {
		return false, nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	if _, err := db.SignIn(ctx, map[string]interface{}{
		"user": username,
		"pass": password,
	}); err != nil {
		db.Close(ctx)
		return false, nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	if err := db.Use(ctx, namespace, database); err != nil {
		db.Close(ctx)
		return false, nil, fmt.Errorf("failed to use namespace/database: %w", err)
	}

	backend := &surrealDBBackend{
		db:        db,
		cfg:       cfg,
		namespace: namespace,
		database:  database,
		nextID:    1,
	}

	if err := backend.setup(ctx); err != nil {
		db.Close(ctx)
		return false, nil, fmt.Errorf("failed to setup schema: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logrus.Infof("Closing SurrealDB connection...")
		if err := db.Close(context.Background()); err != nil {
			logrus.Errorf("Failed to close SurrealDB connection: %v", err)
		}
	}()

	return false, backend, nil
}

func (s *surrealDBBackend) setup(ctx context.Context) error {
	logrus.Infof("Setting up SurrealDB schema...")

	for _, stmt := range schema {
		if stmt == "" {
			continue
		}
		logrus.Tracef("Executing: %s", stmt)

		_, err := s.execQuery(ctx, stmt, nil)
		if err != nil {
			logrus.Warnf("Schema statement may have failed (might be ok if already exists): %v", err)
			// Don't fail on schema errors as they might already exist
		}
	}

	logrus.Infof("SurrealDB schema setup completed")
	return nil
}

func (s *surrealDBBackend) getNextID() int64 {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

func (s *surrealDBBackend) getRecordID(id interface{}) int64 {
	switch v := id.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		// Parse record ID format "kine:123"
		parts := strings.Split(v, ":")
		if len(parts) == 2 {
			if idNum, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				return idNum
			}
		}
	case map[string]interface{}:
		if idVal, ok := v["id"]; ok {
			return s.getRecordID(idVal)
		}
	}
	return 0
}

func (s *surrealDBBackend) Start(ctx context.Context) error {
	logrus.Info("SurrealDB backend started")

	// Initialize nextID by querying current max
	query := "SELECT id FROM kine ORDER BY id DESC LIMIT 1"
	results, err := s.execQuery(ctx, query, nil)
	if err == nil && len(results) > 0 {
		if idVal, ok := results[0]["id"]; ok {
			maxID := s.getRecordID(idVal)
			s.nextID = maxID + 1
			logrus.Infof("Initialized next ID to %d", s.nextID)
		}
	}

	return nil
}

func (s *surrealDBBackend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	logrus.Tracef("Get: key=%s, rangeEnd=%s, limit=%d, revision=%d, keysOnly=%v", key, rangeEnd, limit, revision, keysOnly)

	query := "SELECT * FROM kine WHERE name = $name AND deleted = 0"
	if revision > 0 {
		query += " AND id <= $revision"
	}
	query += " ORDER BY id DESC LIMIT 1"

	vars := map[string]interface{}{
		"name": key,
	}
	if revision > 0 {
		vars["revision"] = revision
	}

	results, err := s.execQuery(ctx, query, vars)
	if err != nil {
		return 0, nil, fmt.Errorf("query failed: %w", err)
	}

	if len(results) == 0 {
		return 0, nil, nil
	}

	rec := results[0]
	recID := s.getRecordID(rec["id"])

	kv := &server.KeyValue{
		Key:            rec["name"].(string),
		CreateRevision: int64(rec["create_revision"].(float64)),
		ModRevision:    recID,
		Lease:          int64(rec["lease"].(float64)),
	}

	if !keysOnly {
		if val, ok := rec["value"].(string); ok {
			kv.Value = []byte(val)
		}
	}

	return recID, kv, nil
}

func (s *surrealDBBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	logrus.Tracef("Create: key=%s, lease=%d", key, lease)

	newID := s.getNextID()

	query := fmt.Sprintf("CREATE kine:%d CONTENT {name: $name, created: 1, deleted: 0, create_revision: $rev, prev_revision: 0, lease: $lease, value: $value, old_value: ''}", newID)

	vars := map[string]interface{}{
		"name":  key,
		"rev":   newID,
		"lease": lease,
		"value": string(value),
	}

	_, err := s.execQuery(ctx, query, vars)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "unique") {
			return 0, server.ErrKeyExists
		}
		return 0, fmt.Errorf("create failed: %w", err)
	}

	return newID, nil
}

func (s *surrealDBBackend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	logrus.Tracef("Delete: key=%s, revision=%d", key, revision)

	// Get current value first
	currentRev, currentKV, err := s.Get(ctx, key, "", 0, 0, false)
	if err != nil {
		return 0, nil, false, err
	}

	if currentKV == nil {
		return currentRev, nil, false, nil
	}

	if revision > 0 && currentRev != revision {
		return currentRev, currentKV, false, nil
	}

	newID := s.getNextID()

	query := fmt.Sprintf("CREATE kine:%d CONTENT {name: $name, created: 0, deleted: 1, create_revision: $create_rev, prev_revision: $prev_rev, lease: 0, value: '', old_value: $old_value}", newID)

	vars := map[string]interface{}{
		"name":       key,
		"create_rev": currentKV.CreateRevision,
		"prev_rev":   currentRev,
		"old_value":  string(currentKV.Value),
	}

	_, err = s.execQuery(ctx, query, vars)
	if err != nil {
		return 0, nil, false, fmt.Errorf("delete failed: %w", err)
	}

	return newID, currentKV, true, nil
}

func (s *surrealDBBackend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	logrus.Tracef("List: prefix=%s, startKey=%s, limit=%d, revision=%d, keysOnly=%v", prefix, startKey, limit, revision, keysOnly)

	query := "SELECT * FROM kine WHERE deleted = 0"

	if prefix != "" {
		query += " AND string::startsWith(name, $prefix)"
	}
	if startKey != "" {
		query += " AND name >= $start_key"
	}
	if revision > 0 {
		query += " AND id <= $revision"
	}

	query += " ORDER BY name ASC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	vars := map[string]interface{}{}
	if prefix != "" {
		vars["prefix"] = prefix
	}
	if startKey != "" {
		vars["start_key"] = startKey
	}
	if revision > 0 {
		vars["revision"] = revision
	}

	results, err := s.execQuery(ctx, query, vars)
	if err != nil {
		return 0, nil, fmt.Errorf("list query failed: %w", err)
	}

	// Get current revision
	currentRev, _ := s.CurrentRevision(ctx)

	// Group by name and keep latest
	latestRecords := make(map[string]map[string]interface{})
	for _, rec := range results {
		name := rec["name"].(string)
		recID := s.getRecordID(rec["id"])

		if existing, exists := latestRecords[name]; !exists || recID > s.getRecordID(existing["id"]) {
			latestRecords[name] = rec
		}
	}

	var kvList []*server.KeyValue
	for _, rec := range latestRecords {
		kv := &server.KeyValue{
			Key:            rec["name"].(string),
			CreateRevision: int64(rec["create_revision"].(float64)),
			ModRevision:    s.getRecordID(rec["id"]),
			Lease:          int64(rec["lease"].(float64)),
		}

		if !keysOnly {
			if val, ok := rec["value"].(string); ok {
				kv.Value = []byte(val)
			}
		}

		kvList = append(kvList, kv)
	}

	return currentRev, kvList, nil
}

func (s *surrealDBBackend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	logrus.Tracef("Count: prefix=%s, startKey=%s, revision=%d", prefix, startKey, revision)

	currentRev, kvList, err := s.List(ctx, prefix, startKey, 0, revision, true)
	if err != nil {
		return 0, 0, err
	}

	return currentRev, int64(len(kvList)), nil
}

func (s *surrealDBBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	logrus.Tracef("Update: key=%s, revision=%d, lease=%d", key, revision, lease)

	// Get current value
	currentRev, currentKV, err := s.Get(ctx, key, "", 0, 0, false)
	if err != nil {
		return 0, nil, false, err
	}

	if currentKV == nil {
		return currentRev, nil, false, nil
	}

	if revision > 0 && currentRev != revision {
		return currentRev, currentKV, false, nil
	}

	newID := s.getNextID()

	query := fmt.Sprintf("CREATE kine:%d CONTENT {name: $name, created: 0, deleted: 0, create_revision: $create_rev, prev_revision: $prev_rev, lease: $lease, value: $value, old_value: $old_value}", newID)

	vars := map[string]interface{}{
		"name":       key,
		"create_rev": currentKV.CreateRevision,
		"prev_rev":   currentRev,
		"lease":      lease,
		"value":      string(value),
		"old_value":  string(currentKV.Value),
	}

	_, err = s.execQuery(ctx, query, vars)
	if err != nil {
		return 0, nil, false, fmt.Errorf("update failed: %w", err)
	}

	return newID, currentKV, true, nil
}

func (s *surrealDBBackend) Watch(ctx context.Context, key string, revision int64) server.WatchResult {
	logrus.Tracef("Watch: key=%s, revision=%d", key, revision)

	eventCh := make(chan []*server.Event, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)

		// Simple polling implementation
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		lastRev := revision

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentRev, kv, err := s.Get(ctx, key, "", 0, 0, false)
				if err != nil {
					errCh <- err
					return
				}

				if currentRev > lastRev {
					event := &server.Event{
						KV: kv,
					}

					if kv == nil {
						event.Delete = true
					}

					eventCh <- []*server.Event{event}
					lastRev = currentRev
				}
			}
		}
	}()

	return server.WatchResult{
		CurrentRevision: revision,
		CompactRevision: 0,
		Events:          eventCh,
		Errorc:          errCh,
	}
}

func (s *surrealDBBackend) DbSize(ctx context.Context) (int64, error) {
	logrus.Trace("DbSize")

	query := "SELECT count() FROM kine GROUP ALL"
	results, err := s.execQuery(ctx, query, nil)
	if err != nil {
		return 0, fmt.Errorf("size query failed: %w", err)
	}

	if len(results) > 0 {
		if count, ok := results[0]["count"]; ok {
			return int64(count.(float64)) * 200, nil // Estimate 200 bytes per record
		}
	}

	return 0, nil
}

func (s *surrealDBBackend) CurrentRevision(ctx context.Context) (int64, error) {
	logrus.Trace("CurrentRevision")

	query := "SELECT id FROM kine ORDER BY id DESC LIMIT 1"
	results, err := s.execQuery(ctx, query, nil)
	if err != nil {
		return 0, fmt.Errorf("revision query failed: %w", err)
	}

	if len(results) > 0 {
		return s.getRecordID(results[0]["id"]), nil
	}

	return 0, nil
}

func (s *surrealDBBackend) Compact(ctx context.Context, revision int64) (int64, error) {
	logrus.Tracef("Compact: revision=%d", revision)

	query := "DELETE FROM kine WHERE (prev_revision != 0 AND id <= $revision AND name != 'compact_rev_key') OR (deleted = 1 AND id <= $revision)"

	vars := map[string]interface{}{
		"revision": revision,
	}

	results, err := s.execQuery(ctx, query, vars)
	if err != nil {
		return 0, fmt.Errorf("compact failed: %w", err)
	}

	deleted := int64(len(results))
	logrus.Infof("Compacted %d records up to revision %d", deleted, revision)

	return deleted, nil
}

func init() {
	drivers.Register("surreal", New)
	drivers.Register("surrealdb", New)
	drivers.Register("ws", New)
	drivers.Register("wss", New)

	logrus.Info("SurrealDB driver registered")
}
