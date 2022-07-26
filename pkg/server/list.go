package server

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) list(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return nil, fmt.Errorf("invalid range end length of 0")
	}

	format_range_end := string(append(r.RangeEnd[:len(r.RangeEnd)-1], r.RangeEnd[len(r.RangeEnd)-1]-1))
	start := string(bytes.TrimRight(r.Key, "\x00"))

	prefix := start
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, prefix)
		if err != nil {
			return nil, err
		}
		return &RangeResponse{
			Header: txnHeader(rev),
			Count:  count,
		}, nil
	}

	limit := r.Limit
	if limit > 0 {
		limit++
	}

	rev, kvs, err := l.backend.List(ctx, prefix, start, 0, r.Revision)
	if err != nil {
		return nil, err
	}

	sort.SliceStable(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	filteredKVS := make([]*KeyValue, 0, len(kvs))
	if format_range_end != string(r.Key) {
		for _, kv := range kvs {
			if kv.Key <= format_range_end {
				filteredKVS = append(filteredKVS, kv)
			}
		}
	} else {
		filteredKVS = kvs
	}

	if r.SortOrder == etcdserverpb.RangeRequest_DESCEND {
		sort.SliceStable(filteredKVS, func(i, j int) bool {
			return filteredKVS[i].Key > filteredKVS[j].Key
		})
	}

	resp := &RangeResponse{
		Header: txnHeader(rev),
		Count:  int64(len(filteredKVS)),
		Kvs:    filteredKVS,
	}

	if limit > 0 && resp.Count > r.Limit {
		resp.More = true
		resp.Kvs = filteredKVS[0 : limit-1]
	}

	return resp, nil
}
