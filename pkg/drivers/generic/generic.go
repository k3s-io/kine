package generic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxIdleConns = 20 // database/sql default is 2, but profiling suggests that kine needs ~10-15 connections available under moderate load
)

// explicit interface check
var _ server.Dialect = (*Generic)(nil)

var (
	Columns    = "id, name, created, deleted, create_revision, prev_revision, lease"
	WithVal    = Columns + ", value"
	WithOldVal = WithVal + ", old_value"
	ListFmt    = `
		SELECT current_rev, compact_rev, %s
		FROM (%s) AS current, (%s) AS compact, kine
		INNER JOIN (%s) AS mkv USING (id)
		WHERE (deleted = 0 OR ?)
		ORDER BY name ASC
		`
	CurrentRevSQL = `SELECT MAX(id) AS current_rev FROM kine`
	CompactRevSQL = `SELECT MAX(prev_revision) AS compact_rev FROM kine WHERE name = 'compact_rev_key'`
	FiltersRevSQL = `SELECT MAX(id) AS id FROM kine WHERE name LIKE ? ESCAPE '^' %s GROUP BY name`

	listSQL    = fmt.Sprintf(ListFmt, Columns, CurrentRevSQL, CompactRevSQL, FiltersRevSQL)
	listValSQL = fmt.Sprintf(ListFmt, WithVal, CurrentRevSQL, CompactRevSQL, FiltersRevSQL)
)

type ErrRetry func(error) bool
type TranslateErr func(error) error
type ErrCode func(error) string
type SubstituteFunc func(string) string

type ConnectionPoolConfig struct {
	MaxIdle     int           // zero means defaultMaxIdleConns; negative means 0
	MaxOpen     int           // <= 0 means unlimited
	MaxLifetime time.Duration // maximum amount of time a connection may be reused
	MaxIdleTime time.Duration // maximum amount of time a connection may remain idle
}

type Generic struct {
	sync.Mutex

	LockWrites              bool
	LastInsertID            bool
	DB                      *sql.DB
	GetSingleSQL            *query.Named
	GetSingleValSQL         *query.Named
	ListCurrentSQL          *query.Named
	ListCurrentValSQL       *query.Named
	ListRevisionStartSQL    *query.Named
	ListRevisionStartValSQL *query.Named
	GetRevisionAfterSQL     *query.Named
	GetRevisionAfterValSQL  *query.Named
	CountCurrentSQL         *query.Named
	CountRevisionSQL        *query.Named
	AfterOldValSQL          *query.Named
	CurrentRevSQL           *query.Named
	CompactRevSQL           *query.Named
	DeleteSQL               *query.Named
	CompactSQL              *query.Named
	UpdateCompactSQL        *query.Named
	PostCompactSQL          *query.Named
	InsertSQL               *query.Named
	FillSQL                 *query.Named
	InsertLastInsertIDSQL   *query.Named
	GetSizeSQL              *query.Named
	Retry                   ErrRetry
	InsertRetry             ErrRetry
	TranslateErr            TranslateErr
	TranslateStartKeyFunc   SubstituteFunc
	ErrCode                 ErrCode
	FillRetryDuration       time.Duration
}

func (d *Generic) Migrate(ctx context.Context) {
	var (
		count     = 0
		countKV   = d.queryRow(ctx, query.New("SELECT COUNT(*) FROM key_value", "?", false, ""))
		countKine = d.queryRow(ctx, query.New("SELECT COUNT(*) FROM kine", "?", false, ""))
	)

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := d.execute(ctx,
		query.New(`INSERT INTO kine(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`, "?", false, ""))
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func configureConnectionPooling(connPoolConfig ConnectionPoolConfig, db *sql.DB, driverName string) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof(
		"Configuring %s database connection pooling: maxIdle=%d, maxOpen=%d, maxLifetime=%s, maxIdleTime=%s",
		driverName, connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime, connPoolConfig.MaxIdleTime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
	db.SetConnMaxIdleTime(connPoolConfig.MaxIdleTime)
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

func Open(ctx context.Context, wg *sync.WaitGroup, driverName, dataSourceName string, connPoolConfig ConnectionPoolConfig, paramCharacter string, numbered bool, metricsRegisterer prometheus.Registerer) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("Failed to ping database connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logrus.Infof("Closing database connections...")
		if err := db.Close(); err != nil {
			logrus.Errorf("Failed to close database: %v", err)
		}
	}()

	configureConnectionPooling(connPoolConfig, db, driverName)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "kine"))
	}

	return &Generic{
		DB: db,

		ListCurrentSQL:          query.New(fmt.Sprintf(listSQL, "AND name >= ?"), paramCharacter, numbered, "ListCurrent"),
		ListCurrentValSQL:       query.New(fmt.Sprintf(listValSQL, "AND name >= ?"), paramCharacter, numbered, "ListCurrentVal"),
		ListRevisionStartSQL:    query.New(fmt.Sprintf(listSQL, "AND id <= ?"), paramCharacter, numbered, "ListRevisionStart"),
		ListRevisionStartValSQL: query.New(fmt.Sprintf(listValSQL, "AND id <= ?"), paramCharacter, numbered, "ListRevisionStartVal"),
		GetRevisionAfterSQL:     query.New(fmt.Sprintf(listSQL, "AND name >= ? AND id <= ?"), paramCharacter, numbered, "GetRevisionAfter"),
		GetRevisionAfterValSQL:  query.New(fmt.Sprintf(listValSQL, "AND name >= ? AND id <= ?"), paramCharacter, numbered, "GetRevisionAfterVal"),
		CurrentRevSQL:           query.New(CurrentRevSQL, paramCharacter, numbered, "CurrentRev"),
		CompactRevSQL:           query.New(CompactRevSQL, paramCharacter, numbered, "CompactRev"),

		GetSingleSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current, (%s) AS compact, kine
			WHERE id = (SELECT MAX(id) FROM kine WHERE name = ?)
			AND (deleted = 0 OR ?)`,
			Columns, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "GetSingle"),

		GetSingleValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current, (%s) AS compact, kine
			WHERE id = (SELECT MAX(id) FROM kine WHERE name = ?)
			AND (deleted = 0 OR ?)`,
			WithVal, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "GetSingleVal"),

		CountCurrentSQL: query.New(fmt.Sprintf(`
			SELECT (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, fmt.Sprintf(FiltersRevSQL, "AND name >= ?")), paramCharacter, numbered, "CountCurrent"),

		CountRevisionSQL: query.New(fmt.Sprintf(`
			SELECT (%s), (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, CompactRevSQL, fmt.Sprintf(FiltersRevSQL, "AND name >= ? AND id <= ?")), paramCharacter, numbered, "CountRevision"),

		AfterOldValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current, (%s) AS compact, kine
			WHERE	name LIKE ? ESCAPE '^'
			AND	id > ?
			ORDER BY id ASC`,
			WithOldVal, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "AfterOldVal"),

		DeleteSQL: query.New(`
			DELETE FROM kine
			WHERE id = ?`,
			paramCharacter, numbered, "Delete"),

		UpdateCompactSQL: query.New(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`,
			paramCharacter, numbered, "UpdateCompact"),

		InsertLastInsertIDSQL: query.New(`
			INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			SELECT ?, ?, ?, ?, ?, ?, ?, (SELECT value FROM kine WHERE id = ?) AS old_value`,
			paramCharacter, numbered, "InsertLastInsertID"),

		InsertSQL: query.New(`
			INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			SELECT ?, ?, ?, ?, ?, ?, ?, (SELECT value FROM kine WHERE id = ?) AS old_value RETURNING id`,
			paramCharacter, numbered, "Insert"),

		FillSQL: query.New(`
			INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			paramCharacter, numbered, "Fill"),
	}, err
}

func (d *Generic) prepare(ctx context.Context, sql *query.Named) (*query.Stmt, error) {
	logrus.Tracef("PREPARE: %s", sql)
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	stmt, err := conn.PrepareContext(ctx, sql.Query)
	if err != nil {
		return nil, err
	}
	return &query.Stmt{Named: sql, Stmt: stmt}, nil
}

func (d *Generic) queryStatement(ctx context.Context, stmt *query.Stmt, args ...any) (result *sql.Rows, err error) {
	query := stmt.Fill(args)
	logrus.Tracef("QUERY PREPARED: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), 0, query)
	}()
	return stmt.Stmt.QueryContext(ctx, args...)
}

func (d *Generic) query(ctx context.Context, sql *query.Named, args ...any) (result *sql.Rows, err error) {
	query := sql.Fill(args)
	logrus.Tracef("QUERY: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), 0, query)
	}()
	return d.DB.QueryContext(ctx, sql.Query, args...)
}

func (d *Generic) queryRow(ctx context.Context, sql *query.Named, args ...any) (result *sql.Row) {
	query := sql.Fill(args)
	logrus.Tracef("QUERY ROW: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(result.Err()), 0, query)
	}()
	return d.DB.QueryRowContext(ctx, sql.Query, args...)
}

func (d *Generic) execute(ctx context.Context, sql *query.Named, args ...any) (result sql.Result, err error) {
	retries := 0
	startTime := time.Now()
	query := sql.Fill(args)
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), retries, query)
	}()

	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for ; retries < 20; retries++ {
		logrus.Tracef("EXEC (try=%d): %s", retries, query)
		result, err = d.DB.ExecContext(ctx, sql.Query, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			wait(uint(retries))
			continue
		}
		return result, err
	}
	return result, err
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.CompactRevSQL)
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
	if d.PostCompactSQL != nil {
		_, err := d.execute(ctx, d.PostCompactSQL)
		return err
	}
	return nil
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	var sql *query.Named
	if startKey == "" && !strings.HasSuffix(prefix, "%") {
		if keysOnly {
			sql = d.GetSingleSQL
		} else {
			sql = d.GetSingleValSQL
		}
		return d.query(ctx, sql, prefix, includeDeleted)
	}
	prefix = strings.ReplaceAll(prefix, `_`, `^_`)
	if keysOnly {
		sql = d.ListCurrentSQL
	} else {
		sql = d.ListCurrentValSQL
	}
	if limit > 0 {
		sql = sql.Appendf("LIMIT %d", limit)
	}
	return d.query(ctx, sql, prefix, startKey, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	prefix = strings.ReplaceAll(prefix, `_`, `^_`)
	var sql *query.Named
	if startKey == "" {
		if keysOnly {
			sql = d.ListRevisionStartSQL
		} else {
			sql = d.ListRevisionStartValSQL
		}
		if limit > 0 {
			sql = sql.Appendf("LIMIT %d", limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}
	if keysOnly {
		sql = d.GetRevisionAfterSQL
	} else {
		sql = d.GetRevisionAfterValSQL
	}
	if limit > 0 {
		sql = sql.Appendf("LIMIT %d", limit)
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

func (d *Generic) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, int64, error) {
	var (
		rev     sql.NullInt64
		compact sql.NullInt64
		id      int64
	)

	row := d.queryRow(ctx, d.CountRevisionSQL, prefix, startKey, revision, false)
	err := row.Scan(&rev, &compact, &id)
	return rev.Int64, compact.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.CurrentRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterOldValSQL
	if limit > 0 {
		sql = sql.Appendf("LIMIT %d", limit)
	}
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) PrepareAfter(ctx context.Context, limit int64) (*query.Stmt, error) {
	sql := d.AfterOldValSQL
	if limit > 0 {
		sql = sql.Appendf("LIMIT %d", limit)
	}
	return d.prepare(ctx, sql)
}

func (d *Generic) QueryAfter(ctx context.Context, stmt *query.Stmt, prefix string, rev int64) (*sql.Rows, error) {
	return d.queryStatement(ctx, stmt, prefix, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *Generic) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

//nolint:revive
func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value []byte) (id int64, err error) {
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
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, previousRevision)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	// Drivers without LastInsertID support may conflict on the serial id key when inserting rows,
	// as the ID is reserved at the beginning of the implicit transaction, but does not become
	// visible until the transaction completes, at which point we may have already created a gap fill record.
	// Retry the insert if the driver indicates a retriable insert error, to avoid presenting a spurious
	// duplicate key error to the client.
	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for i := uint(0); i < 20; i++ {
		row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, previousRevision)
		err = row.Scan(&id)

		if err != nil && d.InsertRetry != nil && d.InsertRetry(err) {
			logrus.Warnf("Retriable insert error for key %v: %v", key, err)
			metrics.InsertErrorsTotal.WithLabelValues("true").Inc()
			wait(i)
			continue
		}

		if err != nil {
			metrics.InsertErrorsTotal.WithLabelValues("false").Inc()
			logrus.WithField("key", key).WithField("createRevision", createRevision).WithField("previousRevision", previousRevision).Errorf("Insert error for key %v: %v", key, err)
		}

		return id, err
	}
	return
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == nil {
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

func (d *Generic) TranslateStartKey(startKey string) string {
	if d.TranslateStartKeyFunc != nil {
		return d.TranslateStartKeyFunc(startKey)
	}
	return startKey
}
