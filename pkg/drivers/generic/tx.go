package generic

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

// explicit interface check
var _ server.Transaction = (*Tx)(nil)

type Tx struct {
	i uuid.UUID
	x *sql.Tx
	d *Generic
}

func (d *Generic) BeginTx(ctx context.Context, opts *sql.TxOptions) (server.Transaction, error) {
	if opts == nil {
		opts = &sql.TxOptions{Isolation: sql.LevelRepeatableRead}
	}
	id := uuid.New()
	logrus.Tracef("TX BEGIN id=%s, isolation='%s'", id, opts.Isolation)
	x, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		i: id,
		x: x,
		d: d,
	}, nil
}

func (t *Tx) Commit() error {
	logrus.Tracef("TX COMMIT id=%s", t.i)
	return t.x.Commit()
}

func (t *Tx) MustCommit() {
	if err := t.Commit(); err != nil {
		logrus.Fatalf("Transaction commit failed: %v", err)
	}
}

func (t *Tx) Rollback() error {
	logrus.Tracef("TX ROLLBACK id=%s", t.i)
	return t.x.Rollback()
}

func (t *Tx) MustRollback() {
	if err := t.Rollback(); err != nil {
		if err != sql.ErrTxDone {
			logrus.Fatalf("Transaction rollback failed: %v", err)
		}
	}
}

func (t *Tx) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := t.queryRow(ctx, t.d.CompactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (t *Tx) SetCompactRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("TX SETCOMPACTREVISION %v", revision)
	_, err := t.execute(ctx, t.d.UpdateCompactSQL, revision)
	return err
}

func (t *Tx) Compact(ctx context.Context, from, to int64) (int64, error) {
	logrus.Tracef("TX COMPACT %v => %v", from, to)
	res, err := t.execute(ctx, t.d.CompactSQL, from, to, from, to)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (t *Tx) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("TX DELETEREVISION %v", revision)
	_, err := t.execute(ctx, t.d.DeleteSQL, revision)
	return err
}

func (t *Tx) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := t.queryRow(ctx, t.d.CurrentRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (t *Tx) Insert(ctx context.Context, key string, created, deleted bool, previousRevision, ttl int64, value []byte) (id int64, err error) {
	if t.d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = t.d.TranslateErr(err)
			}
		}()
	}

	cols := []any{previousRevision, key, boolToInt(created), boolToInt(deleted), previousRevision, ttl, value, key, previousRevision}

	if t.d.LastInsertID {
		res, err := t.execute(ctx, t.d.InsertSQL, cols...)
		if err != nil {
			return 0, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if rows == 0 {
			return 0, nil
		}
		return res.LastInsertId()
	}

	return retryInsert(ctx, t.d.queryRow, t.d.InsertRetry, t.d.InsertReturningSQL, cols...)
}

func (t *Tx) query(ctx context.Context, sql *query.Named, args ...any) (result *sql.Rows, err error) {
	query := sql.Fill(args)
	logrus.Tracef("TX QUERY: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, t.d.ErrCode(err), 0, query)
	}()
	return t.x.QueryContext(ctx, sql.Query, args...)
}

func (t *Tx) queryRow(ctx context.Context, sql *query.Named, args ...any) (result *sql.Row) {
	query := sql.Fill(args)
	logrus.Tracef("TX QUERY ROW: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, t.d.ErrCode(result.Err()), 0, query)
	}()
	return t.x.QueryRowContext(ctx, sql.Query, args...)
}

func (t *Tx) execute(ctx context.Context, sql *query.Named, args ...any) (result sql.Result, err error) {
	query := sql.Fill(args)
	logrus.Tracef("TX EXEC: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, t.d.ErrCode(err), 0, query)
	}()
	return t.x.ExecContext(ctx, sql.Query, args...)
}
