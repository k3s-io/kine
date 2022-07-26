package server

import (
	"context"
	"fmt"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	backend Backend
	scheme  string
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return l.put(ctx, r)
}

func (l *LimitedServer) Delete(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	key := string(r.Key)
	if len(r.RangeEnd) == 0 && strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
	}

	rev, kv, _, err := l.backend.Delete(ctx, key, 0)
	if err != nil {
		return nil, err
	}
	deleted := int64(0)
	if kv != nil {
		deleted = 1
	}
	return &etcdserverpb.DeleteRangeResponse{
		Header:  txnHeader(rev),
		Deleted: deleted,
	}, nil
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put, txn)
	}
	if rev, key, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return l.compact(ctx)
	}
	return nil, fmt.Errorf("unsupported transaction: %v", txn)
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
