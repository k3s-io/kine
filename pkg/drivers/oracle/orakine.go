package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	columns = "kv.id AS theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	revSQL  = `
	SELECT MAX(rkv.id) AS id
	FROM kine rkv`

	compactRevSQL = `
	SELECT MAX(crkv.prev_revision) AS prev_revision
	FROM kine crkv
	WHERE crkv.name = 'compact_rev_key'`

	idOfKey = `
	AND
	mkv.id <= ? AND
	mkv.id > (
		SELECT MAX(ikv.id) AS id
		FROM kine AS ikv
		WHERE
			ikv.name = ? AND
			ikv.id <= ?)`

	listSQL = fmt.Sprintf(`
	SELECT *
	FROM (
		SELECT (%s), (%s), %s
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) AS id
			FROM kine mkv
			WHERE
				mkv.name LIKE ?
				%%s
			GROUP BY mkv.name) maxkv
			ON maxkv.id = kv.id
			WHERE 
			CASE WHEN :2 = 1 THEN 1 ELSE kv.deleted END = 0
	) lkv
	ORDER BY lkv.theid ASC
	`, revSQL, compactRevSQL, columns)
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
	CountSQL              string
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
	TranslateErr          TranslateErr
	ErrCode               ErrCode
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

	return &Generic{
		DB: db,

		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(fmt.Sprintf(listSQL, idOfKey), paramCharacter, numbered),

		CountSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQL: q(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM kine kv
			WHERE kv.id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO ORACLE.kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id INTO ?`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, EMPTY_BLOB(), EMPTY_BLOB())`, paramCharacter, numbered),
	}, err
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{}) (result *sql.Rows, err error) {
	d.execute(ctx, "ALTER SESSION SET current_schema = ORACLE")
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

func (d *Generic) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf(" SELECT * FROM( %s ) WHERE rownum <= %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, boolOracle(sql, includeDeleted))
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf(" SELECT * FROM( %s ) WHERE rownum <= %d", sql, limit)
		}

		return d.query(ctx, sql, prefix, revision, boolOracle(sql, includeDeleted))
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf(" SELECT * FROM( %s ) WHERE rownum <= %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, revision, startKey, revision, boolOracle(sql, includeDeleted))
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountSQL, prefix, false)
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
		sql = fmt.Sprintf(" SELECT * FROM( %s ) WHERE rownum <= %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0)
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
		if isNil(prevValue) && isNil(value) {
			d.InsertLastInsertIDSQL = q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, EMPTY_BLOB(), EMPTY_BLOB())`, ":", true)
			row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl)
			if err != nil {
				return 0, err
			}
			return row.LastInsertId()
		} else if isNil(value) {
			d.InsertLastInsertIDSQL = q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, EMPTY_BLOB(), ?)`, ":", true)
			row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, prevValue)
			if err != nil {
				return 0, err
			}
			return row.LastInsertId()
		} else if isNil(prevValue) {
			d.InsertLastInsertIDSQL = q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?,EMPTY_BLOB())`, ":", true)
			row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value)
			if err != nil {
				return 0, err
			}
			return row.LastInsertId()
		}
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	if isNil(prevValue) && isNil(value) {
		d.InsertSQL = q(`INSERT INTO ORACLE.kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, EMPTY_BLOB(), EMPTY_BLOB()) RETURNING id INTO ?`, ":", true)
		_, err = d.execute(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, sql.Out{Dest: &id})
	} else if isNil(value) {
		d.InsertSQL = q(`INSERT INTO ORACLE.kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, EMPTY_BLOB(), ?) RETURNING id INTO ?`, ":", true)
		_, err = d.execute(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, prevValue, sql.Out{Dest: &id})
	} else if isNil(prevValue) {
		d.InsertSQL = q(`INSERT INTO ORACLE.kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, EMPTY_BLOB()) RETURNING id INTO ?`, ":", true)
		_, err = d.execute(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, sql.Out{Dest: &id})
	} else {
		_, err = d.execute(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue, sql.Out{Dest: &id})
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

func boolOracle(sql string, includeDeleted bool) string {
	if includeDeleted {
		return `1`
	}
	return `0`
}

func isNil(val []byte) bool {
	return len(val) == 0
}