package server

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	key := string(r.Key)
	value := r.Value
	lease := r.Lease
	var ok bool
	var rev int64

	_, kv, err := l.backend.Get(ctx, string(r.Key), 0)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		rev, err = l.backend.Create(ctx, key, value, lease)
		ok = true
	} else {
		rev, kv, ok, err = l.backend.Update(ctx, key, value, kv.ModRevision, lease)
	}
	logrus.Trace("PUT ", rev, kv, ok)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.PutResponse{
		Header: txnHeader(rev),
	}, nil
}
