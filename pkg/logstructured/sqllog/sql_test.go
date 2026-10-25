package sqllog

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

type compactTestDialect struct {
	server.Dialect
	tx *compactTestTx
}

func (d *compactTestDialect) BeginTx(ctx context.Context, _ *sql.TxOptions) (server.Transaction, error) {
	d.tx.beginCtx = ctx
	return d.tx, nil
}

type compactTestTx struct {
	beginCtx   context.Context
	compact    func(context.Context) error
	committed  bool
	rolledBack bool
}

func (t *compactTestTx) Commit() error   { t.committed = true; return nil }
func (t *compactTestTx) MustCommit()     { t.committed = true }
func (t *compactTestTx) Rollback() error { t.rolledBack = true; return nil }
func (t *compactTestTx) MustRollback()   { t.rolledBack = true }
func (t *compactTestTx) checkContext(ctx context.Context) error {
	if ctx != t.beginCtx {
		return errors.New("SQL operation did not receive the transaction context")
	}
	return nil
}

func (t *compactTestTx) GetCompactRevision(ctx context.Context) (int64, error) {
	return 0, t.checkContext(ctx)
}
func (t *compactTestTx) SetCompactRevision(ctx context.Context, _ int64) error {
	return t.checkContext(ctx)
}
func (t *compactTestTx) Compact(ctx context.Context, _, _ int64) (int64, error) {
	if err := t.checkContext(ctx); err != nil {
		return 0, err
	}
	return 0, t.compact(ctx)
}
func (t *compactTestTx) DeleteRevision(context.Context, int64) error { return nil }
func (t *compactTestTx) CurrentRevision(ctx context.Context) (int64, error) {
	return 3000, t.checkContext(ctx)
}

func TestCompactCancelsSQLWithTransactionDeadline(t *testing.T) {
	tx := &compactTestTx{}
	d := &compactTestDialect{tx: tx}
	tx.compact = func(ctx context.Context) error {
		if err := tx.checkContext(ctx); err != nil {
			return err
		}
		beginDeadline, hasBeginDeadline := tx.beginCtx.Deadline()
		sqlDeadline, hasSQLDeadline := ctx.Deadline()
		if !hasBeginDeadline || !hasSQLDeadline || !beginDeadline.Equal(sqlDeadline) {
			t.Fatalf("transaction and SQL deadlines differ: begin=%v/%v sql=%v/%v", beginDeadline, hasBeginDeadline, sqlDeadline, hasSQLDeadline)
		}
		<-ctx.Done()
		return ctx.Err()
	}

	log := &SQLLog{d: d, ctx: context.Background(), compactTimeout: 20 * time.Millisecond, compactMinRetain: 1000}
	started := time.Now()
	_, _, err := log.compact(0, 2000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("compact error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("compact cancellation took %s", elapsed)
	}
	if tx.committed {
		t.Fatal("timed-out compaction committed")
	}
	if !tx.rolledBack {
		t.Fatal("timed-out compaction did not roll back")
	}
}

func TestCompactPreservesStorageError(t *testing.T) {
	storageErr := errors.New("sqlite write failed")
	tx := &compactTestTx{compact: func(context.Context) error { return storageErr }}
	d := &compactTestDialect{tx: tx}
	log := &SQLLog{d: d, ctx: context.Background(), compactTimeout: time.Second, compactMinRetain: 1000}

	_, _, err := log.compact(0, 2000)
	if !errors.Is(err, storageErr) {
		t.Fatalf("compact error = %v, want wrapped storage error", err)
	}
	if tx.committed {
		t.Fatal("failed compaction committed")
	}
}
