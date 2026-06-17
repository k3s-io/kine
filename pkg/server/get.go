package server

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit > 1 {
		return nil, unsupported("limit")
	}

	key := string(r.Key)
	// redirect apiserver get to the substitute compact revision key
	// response is fixed up in toKV()
	if key == compactRevKey {
		key = compactRevAPI
	}

	rev, kv, err := l.backend.Get(ctx, key, r.Revision, r.KeysOnly)
	logrus.Tracef("GET key=%s, revision=%d, currentRev=%d, keysOnly=%v", key, r.Revision, rev, r.KeysOnly)
	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
		resp.Count = 1
	}
	return resp, err
}
