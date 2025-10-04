package server

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		return nil, fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
	}

	key := string(r.Key)

	rev, kv, err := l.backend.Get(ctx, key, string(r.RangeEnd), r.Limit, r.Revision, r.KeysOnly)
	logrus.Tracef("GET key=%s, end=%s, revision=%d, currentRev=%d, limit=%d, keysOnly=%v", r.Key, r.RangeEnd, r.Revision, rev, r.Limit, r.KeysOnly)
	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
		resp.Count = 1
	}
	return resp, err
}
