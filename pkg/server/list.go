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
	if key == "\x00" && end == "\x00" {
		key = ""
		end = ""
	}

	revision := int64(0)
	if r.Revision > 0 {
		revision = r.Revision
	}

	limit := r.Limit
	if limit > 0 {
		limit++
	}

	var currentRev int64
	var kvs []*KeyValue
	var err error
	if !r.CountOnly {
		currentRev, kvs, err = l.backend.List(ctx, key, end, limit, revision, r.KeysOnly)
		logrus.Tracef("LIST key=%s, end=%s, revision=%d, currentRev=%d count=%d, limit=%d, keysOnly=%v", key, end, revision, currentRev, len(kvs), r.Limit, r.KeysOnly)
		if err != nil {
			return nil, err
		}
	}

	var more bool
	var count = int64(len(kvs))
	if r.CountOnly || (limit > 0 && count > r.Limit) {
		if len(kvs) > 1 {
			kvs = kvs[:len(kvs)-1]
			more = true
		}

		if revision == 0 {
			revision = currentRev
		}

		currentRev, count, err = l.backend.Count(ctx, key, end, revision)
		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", key, end, revision, currentRev, count)
		if err != nil {
			return nil, err
		}
	}

	return &RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: currentRev},
		Kvs:    kvs,
		More:   more,
		Count:  count,
	}, nil
}
