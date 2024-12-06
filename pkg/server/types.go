package server

import (
	"context"
	"database/sql"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotSupported = status.New(codes.InvalidArgument, "etcdserver: unsupported operations in txn request").Err()

	ErrKeyExists     = rpctypes.ErrGRPCDuplicateKey
	ErrCompacted     = rpctypes.ErrGRPCCompacted
	ErrFutureRev     = rpctypes.ErrGRPCFutureRev
	ErrGRPCUnhealthy = rpctypes.ErrGRPCUnhealthy
)

type Backend interface {
	Start(ctx context.Context) error
	Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *KeyValue, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)
	Watch(ctx context.Context, key string, revision int64) WatchResult
	DbSize(ctx context.Context) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	Compact(ctx context.Context, revision int64) (int64, error)
}

type Dialect interface {
	ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted bool) (*sql.Rows, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error)
	CountCurrent(ctx context.Context, prefix, startKey string) (int64, int64, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error)
	//nolint:revive
	Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, revision int64) (int64, error)
	PostCompact(ctx context.Context) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key string) bool
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	GetSize(ctx context.Context) (int64, error)
	FillRetryDelay(ctx context.Context)
}

type Transaction interface {
	Commit() error
	MustCommit()
	Rollback() error
	MustRollback()
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, revision int64) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	CurrentRevision(ctx context.Context) (int64, error)
}

type KeyValue struct {
	Key            string
	CreateRevision int64
	ModRevision    int64
	Value          []byte
	Lease          int64
}

type Event struct {
	Delete bool
	Create bool
	KV     *KeyValue
	PrevKV *KeyValue
}

type WatchResult struct {
	CurrentRevision int64
	CompactRevision int64
	Events          <-chan []*Event
	Errorc          <-chan error
}

func unsupported(field string) error {
	return status.New(codes.Unimplemented, field+" is not implemented by kine").Err()
}
