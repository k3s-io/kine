package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

const maxItems = int64(10000)

func (l *LimitedServer) list(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return nil, fmt.Errorf("invalid range end length of 0")
	}

	prefix := string(append(r.RangeEnd[:len(r.RangeEnd)-1], r.RangeEnd[len(r.RangeEnd)-1]-1))
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	start := string(bytes.TrimRight(r.Key, "\x00"))
	revision := r.Revision

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, prefix, start, revision)
		resp := &RangeResponse{
			Header: txnHeader(rev),
			Count:  count,
		}
		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, count)
		return resp, err
	}

	limit := r.Limit
	if limit == 0 {
		limit = maxItems
	}
	limit++

	rev, kvs, err := l.backend.List(ctx, prefix, start, limit, revision)
	logrus.Tracef("LIST key=%s, end=%s, revision=%d, currentRev=%d count=%d, limit=%d", r.Key, r.RangeEnd, revision, rev, len(kvs), r.Limit)
	resp := &RangeResponse{
		Header: txnHeader(rev),
		Count:  int64(len(kvs)),
		Kvs:    kvs,
	}

	// if the number of items returned exceeds the limit, count the keys remaining that follow the start key
	if resp.Count > limit-1 {
		resp.More = true
		resp.Kvs = kvs[0 : limit-1]

		if revision == 0 {
			revision = rev
		}

		rev, resp.Count, err = l.backend.Count(ctx, prefix, start, revision)
		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, resp.Count)
		resp.Header = txnHeader(rev)

		if err == nil && resp.Count > maxItems && (r.Limit == 0 || r.Limit > maxItems) && prefix == start {
			return nil, errors.New("dataset size exceeds the allowed limit, please use limit or rage end to reduce the number of items")
		}
	}

	return resp, err
}
