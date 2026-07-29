package server

import (
	"bytes"
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	}

	var kv *KeyValue
	// redirect apiserver get to the substitute compact revision key
	// response is fixed up in toKV()
	if bytes.Equal(r.Key, compactRevKey) {
		r.Key = compactRevAPI
	}

	key := string(r.Key)
	rev, err := l.backend.Create(ctx, key, r.Value, r.Lease)
	if err == ErrKeyExists {
		rev, kv, err = l.backend.Get(ctx, key, rev, false)
		if err != nil {
			return nil, err
		}
		if !r.PrevKv {
			kv = nil
		}
		rev, _, _, err = l.backend.Update(ctx, key, r.Value, rev, r.Lease)
	}

	return &etcdserverpb.PutResponse{
		Header: txnHeader(rev),
		PrevKv: toKV(kv),
	}, err
}
