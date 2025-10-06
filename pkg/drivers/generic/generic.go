package generic

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	columns    = "kv.id AS theid, kv.name AS thename, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease"
	withVal    = columns + ", kv.value"
	withOldVal = withVal + ", kv.old_value"
	revSQL     = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv`

	compactRevSQL = `
		SELECT MAX(crkv.prev_revision) AS prev_revision
		FROM kine AS crkv
		WHERE crkv.name = 'compact_rev_key'`
	listFmt = `
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
		`
	listSQL       = fmt.Sprintf(listFmt, revSQL, compactRevSQL, columns)
	listValSQL    = fmt.Sprintf(listFmt, revSQL, compactRevSQL, withVal)
	listOldValSQL = fmt.Sprintf(listFmt, revSQL, compactRevSQL, withOldVal)
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

	LockWrites              bool
	LastInsertID            bool
	DB                      *sql.DB
	GetCurrentSQL           string
	GetCurrentValSQL        string
	ListRevisionStartSQL    string
	ListRevisionStartValSQL string
	GetRevisionAfterSQL     string
	GetRevisionAfterValSQL  string
	CountCurrentSQL         string
	CountRevisionSQL        string
	AfterOldValSQL          string
	DeleteSQL               string
	CompactSQL              string
	UpdateCompactSQL        string
	PostCompactSQL          string
	InsertSQL               string
	FillSQL                 string
	InsertLastInsertIDSQL   string
	GetSizeSQL              string
	Retry                   ErrRetry
	InsertRetry             ErrRetry
	TranslateErr            TranslateErr
	ErrCode                 ErrCode
	FillRetryDuration       time.Duration
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

func openAndTest(connector driver.Connector) (*sql.DB, error) {
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, wg *sync.WaitGroup, driverName string, connector driver.Connector, connPoolConfig ConnectionPoolConfig, paramCharacter string, numbered bool, metricsRegisterer prometheus.Registerer) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(connector)
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

		GetCurrentSQL:           q(fmt.Sprintf(listSQL, "AND mkv.name > ?"), paramCharacter, numbered),
		GetCurrentValSQL:        q(fmt.Sprintf(listValSQL, "AND mkv.name > ?"), paramCharacter, numbered),
		ListRevisionStartSQL:    q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		ListRevisionStartValSQL: q(fmt.Sprintf(listValSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:     q(fmt.Sprintf(listSQL, "AND mkv.name > ? AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterValSQL:  q(fmt.Sprintf(listValSQL, "AND mkv.name > ? AND mkv.id <= ?"), paramCharacter, numbered),

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

		AfterOldValSQL: q(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine AS kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, withOldVal), paramCharacter, numbered),

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

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	var sql string
	if keysOnly {
		sql = d.GetCurrentSQL
	} else {
		sql = d.GetCurrentValSQL
	}
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, startKey, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	var sql string
	if startKey == "" {
		if keysOnly {
			sql = d.ListRevisionStartSQL
		} else {
			sql = d.ListRevisionStartValSQL
		}
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	if keysOnly {
		sql = d.GetRevisionAfterSQL
	} else {
		sql = d.GetRevisionAfterValSQL
	}
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
	sql := d.AfterOldValSQL
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

//nolint:revive
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
		return row.LastInsertId()
	}

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
			logrus.Warnf("retriable insert error for key %v: %v", key, err)
			metrics.InsertErrorsTotal.WithLabelValues("true").Inc()
			wait(i)
			continue
		}

		if err != nil {
			metrics.InsertErrorsTotal.WithLabelValues("false").Inc()
			logrus.WithField("key", key).WithField("createRevision", createRevision).WithField("previousRevision", previousRevision).Errorf("insert error for key %v: %v", key, err)
		}

		return id, err
	}
	return
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
