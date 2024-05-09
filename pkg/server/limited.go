package server

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	notifyInterval time.Duration
	backend        Backend
	scheme         string
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put)
	}
	if rev, key, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return l.compact()
	}
	return nil, ErrNotSupported
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
