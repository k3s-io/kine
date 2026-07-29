package server

import (
	"bytes"
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit > 1 {
		return nil, unsupported("limit")
	}

	// redirect apiserver get to the substitute compact revision key
	// response is fixed up in toKV()
	if bytes.Equal(r.Key, compactRevKey) {
		r.Key = compactRevAPI
	}

	rev, kv, err := l.backend.Get(ctx, string(r.Key), r.Revision, r.KeysOnly)
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		logrus.Tracef("GET key=%s, revision=%d, currentRev=%d, keysOnly=%v", r.Key, r.Revision, rev, r.KeysOnly)
	}
	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
		resp.Count = 1
	}
	return resp, err
}
