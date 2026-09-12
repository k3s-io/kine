package server

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/proto"
)

const initialBatchSize = 10

// adjustBatchSize picks the next batch's size so each batch lands near
// the target size: too small wastes round-trips, too large produces
// oversized batches. Doubling/halving only outside [0.5x, 2x] avoids
// thrashing when responses sit near the boundary. Always returns >= 1,
// since we need to send SOMETHING.
func adjustBatchSize(lastBatch int64, lastSize, targetSize int) int64 {
	switch {
	case lastSize < targetSize/2:
		lastBatch *= 2
	case lastSize > targetSize*2:
		lastBatch /= 2
	}
	if lastBatch == 0 {
		lastBatch = 1
	}
	return lastBatch
}

func (l *LimitedServer) RangeStream(r *etcdserverpb.RangeRequest, rs etcdserverpb.KV_RangeStreamServer) error {
	if len(r.RangeEnd) == 0 {
		return errors.New("invalid range end length of 0")
	}

	ctx, cancel := context.WithCancel(rs.Context())
	defer cancel()

	key := string(r.Key)
	end := string(r.RangeEnd)
	revision := int64(0)
	if r.Revision > 0 {
		revision = r.Revision
	}

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, key, end, revision)
		if err != nil {
			return err
		}
		logrus.Tracef("RANGESTREAM COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", key, end, revision, rev, count)
		return rs.Send(&etcdserverpb.RangeStreamResponse{
			RangeResponse: &etcdserverpb.RangeResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: rev},
				Count:  count,
			}})
	}

	limit := r.Limit
	if limit > 0 {
		limit++
	}
	lr := l.backend.ListStream(ctx, key, end, limit, revision, r.KeysOnly)
	logrus.Tracef("RANGESTREAM key=%s, end=%s, revision=%d, currentRev=%d limit=%d, keysOnly=%v", key, end, revision, lr.CurrentRevision, r.Limit, r.KeysOnly)

	count := int64(0)
	batchSize := int64(initialBatchSize)
	kvs := []*mvccpb.KeyValue{}
	for {
		if kv, ok := <-lr.KVc; ok {
			if limit == 0 || count < limit {
				count++
				kvs = append(kvs, toKV(kv))
				if int64(len(kvs)) >= batchSize {
					resp := &etcdserverpb.RangeResponse{Kvs: kvs}
					batchSize = adjustBatchSize(batchSize, proto.Size(resp), embed.DefaultMaxRequestBytes)
					if err := rs.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: resp}); err != nil {
						return err
					}
					kvs = kvs[:0]
				}
			}
		} else {
			if len(kvs) > 0 {
				if err := rs.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Kvs: kvs}}); err != nil {
					return err
				}
			}
			break
		}
	}

	if err := <-lr.Errorc; err != nil {
		return err
	}

	var more bool
	if limit > 0 && count > r.Limit {
		more = true
		var err error
		_, count, err = l.backend.Count(ctx, key, end, lr.CurrentRevision)
		if err != nil {
			return err
		}
	}

	return rs.Send(&etcdserverpb.RangeStreamResponse{
		RangeResponse: &etcdserverpb.RangeResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: lr.CurrentRevision},
			More:   more,
			Count:  count,
		},
	})
}
