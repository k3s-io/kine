package server

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) list(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return nil, errors.New("invalid range end length of 0")
	}

	key := string(r.Key)
	end := string(r.RangeEnd)
	revision := int64(0)
	if r.Revision > 0 {
		revision = r.Revision
	}

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, key, end, revision)
		resp := &RangeResponse{
			Header: txnHeader(rev),
			Count:  count,
		}
		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", key, end, revision, rev, count)
		return resp, err
	}

	limit := r.Limit
	if limit > 0 {
		limit++
	}

	rev, kvs, err := l.backend.List(ctx, key, end, limit, revision, r.KeysOnly)
	logrus.Tracef("LIST key=%s, end=%s, revision=%d, currentRev=%d count=%d, limit=%d, keysOnly=%v", key, end, revision, rev, len(kvs), r.Limit, r.KeysOnly)
	resp := &RangeResponse{
		Header: txnHeader(rev),
		Count:  int64(len(kvs)),
		Kvs:    kvs,
	}

	// if the number of items returned exceeds the limit, count the keys remaining that follow the start key
	if limit > 0 && resp.Count > r.Limit {
		resp.More = true
		resp.Kvs = kvs[0 : limit-1]

		if revision == 0 {
			revision = rev
		}

		rev, resp.Count, err = l.backend.Count(ctx, key, end, revision)
		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", key, end, revision, rev, resp.Count)
		resp.Header = txnHeader(rev)
	}

	return resp, err
}
