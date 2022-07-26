package server

import (
	"context"
	"fmt"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit != 0 {
		return nil, fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
	}

	key := string(r.Key)
	if len(r.RangeEnd) == 0 && strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
	}
	rev, kv, err := l.backend.Get(ctx, key, r.Revision)
	if err != nil {
		return nil, err
	}

	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
	}
	return resp, nil
}
