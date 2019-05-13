package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS key_value
			(
				id INTEGER primary key autoincrement,
				name INTEGER,
				created INTEGER,
				deleted INTEGER,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`,
	}

	columns             = "id, name, created, deleted, prev_revision, lease, value, old_value"
	revSQL              = "SELECT id FROM key_value ORDER BY id DESC LIMIT 1"
	compactRevSQL       = `SELECT prev_revision FROM key_value WHERE name = 'compact_rev_key' ORDER BY id DESC LIMIT 1`
	getCurrentSQL       = fmt.Sprintf(`SELECT (%s), (%s), %s FROM key_value WHERE name LIKE ? GROUP BY name HAVING MAX(id) ORDER BY id ASC`, revSQL, compactRevSQL, columns)
	getRevisionSQL      = fmt.Sprintf(`SELECT 0, 0, %s FROM key_value WHERE id = ?`, columns)
	revisionSQL         = fmt.Sprintf(`SELECT (%s), (%s), %s FROM key_value WHERE name LIKE ? AND id <= ? GROUP BY name HAVING MAX(id) ORDER BY id ASC`, revSQL, compactRevSQL, columns)
	idOfKey             = "SELECT id FROM key_value WHERE name = ? AND id <= ? ORDER BY id DESC LIMIT 1"
	getRevisionAfterSQL = fmt.Sprintf(`SELECT (%s), (%s), %s FROM key_value WHERE name LIKE ? AND id <= ? AND id >= (%s) GROUP BY name HAVING MAX(id) ORDER BY id ASC`,
		revSQL, compactRevSQL, columns, idOfKey)
	countSQL         = fmt.Sprintf(`SELECT MAX(num), COUNT(num) FROM (SELECT (%s) AS num FROM key_value WHERE name LIKE ? GROUP BY name HAVING MAX(id) ORDER BY id ASC)`, revSQL)
	sinceSQL         = fmt.Sprintf("SELECT 0, 0, %s FROM key_value WHERE id > ? ORDER BY id ASC", columns)
	deleteSQL        = "DELETE FROM key_value WHERE id = ?"
	updateCompactSQL = "UPDATE key_value SET prev_revision = ? WHERE name = 'compact_rev_key'"
)

type Driver struct {
	db *sql.DB
}

func Open(dataSourceName string) (*Driver, error) {
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared"
	}
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}

	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	return &Driver{
		db: db,
	}, nil
}

func (d *Driver) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	logrus.Tracef("QUERY %v : %s", args, sql)
	return d.db.QueryContext(ctx, sql, args...)
}

func (d *Driver) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	logrus.Tracef("QUERY ROW %v : %s", args, sql)
	return d.db.QueryRowContext(ctx, sql, args...)
}

func (d *Driver) execute(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	logrus.Tracef("EXEC %v : %s", args, sql)
	return d.db.ExecContext(ctx, sql, args...)
}

func (d *Driver) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Driver) SetCompactRevision(ctx context.Context, revision int64) error {
	result, err := d.execute(ctx, updateCompactSQL, revision)
	if err != nil {
		return err
	}
	num, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if num != 0 {
		return nil
	}
	_, err = d.Insert(ctx, "compact_rev_key", false, false, revision, 0, []byte(""), nil)
	return err
}

func (d *Driver) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, getRevisionSQL, revision)
}

func (d *Driver) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, deleteSQL, revision)
	return err
}

func (d *Driver) ListCurrent(ctx context.Context, prefix string, limit int64) (*sql.Rows, error) {
	sql := getCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix)
}

func (d *Driver) List(ctx context.Context, prefix, startKey string, limit, revision int64) (*sql.Rows, error) {
	if startKey == "" {
		sql := revisionSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		logrus.Trace("SQL: ", sql, " params ", prefix, ", ", revision)
		return d.query(ctx, sql, prefix, revision)
	}

	sql := getRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	logrus.Trace("SQL: ", sql, " params ", prefix, ", ", revision)
	return d.query(ctx, sql, prefix, revision, startKey, revision)
}

func (d *Driver) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, countSQL, prefix)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Driver) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Driver) Since(ctx context.Context, rev int64) (*sql.Rows, error) {
	sql := sinceSQL
	return d.query(ctx, sql, rev)
}

func (d *Driver) Insert(ctx context.Context, key string, create, delete bool, previousRevision int64, ttl int64, value, prevValue []byte) (int64, error) {
	result, err := d.execute(ctx,
		`insert into key_value(name, created, deleted, prev_revision, lease, value, old_value)
			values(?,?, ?, ?, ?, ?, ?)`, key, create, delete, previousRevision, ttl, value, prevValue)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
