package generic

import (
	"context"
	"database/sql"

	"github.com/sirupsen/logrus"
)

type Tx struct {
	x *sql.Tx
	d *Generic
}

func (d *Generic) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	logrus.Tracef("TX BEGIN")
	x, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		x: x,
		d: d,
	}, nil
}

func (t *Tx) Commit() error {
	logrus.Tracef("TX COMMIT")
	return t.x.Commit()
}

func (t *Tx) Rollback() error {
	logrus.Tracef("TX ROLLBACK")
	return t.x.Rollback()
}

func (t *Tx) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := t.queryRow(ctx, compactRevSQL)
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

func (t *Tx) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return t.query(ctx, t.d.GetRevisionSQL, revision)
}

func (t *Tx) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("TX DELETEREVISION %v", revision)
	_, err := t.execute(ctx, t.d.DeleteSQL, revision)
	return err
}

func (t *Tx) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	logrus.Tracef("TX QUERY %v : %s", args, Stripped(sql))
	return t.x.QueryContext(ctx, sql, args...)
}

func (t *Tx) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	logrus.Tracef("TX QUERY ROW %v : %s", args, Stripped(sql))
	return t.x.QueryRowContext(ctx, sql, args...)
}

func (t *Tx) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	logrus.Tracef("TX EXEC %v : %s", args, Stripped(sql))
	return t.x.ExecContext(ctx, sql, args...)
}
