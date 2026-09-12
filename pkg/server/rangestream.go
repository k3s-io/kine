package server

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/proto"
)

const initialBatchSize = 10

// adjustBatchSize picks the next batch's size so each batch lands near
// the target size: too small wastes round-trips, too large produces
// oversized batches. Doubling/halving only outside [0.5x, 2x] avoids
// thrashing when responses sit near the boundary. Always returns >= 1,
// since we need to send SOMETHING.
func adjustBatchSize(lastBatch, lastSize, targetSize int) int {
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

	var count int64
	var currentRev int64
	var kvs []*KeyValue
	if !r.CountOnly {
		lr := l.backend.ListStream(ctx, key, end, limit, revision, r.KeysOnly)
		logrus.Tracef("RANGESTREAM key=%s, end=%s, revision=%d, currentRev=%d limit=%d, keysOnly=%v", key, end, revision, lr.CurrentRevision, r.Limit, r.KeysOnly)

		currentRev = lr.CurrentRevision
		batchSize := initialBatchSize
		for kv := range lr.KVc {
			if len(kvs) == batchSize {
				resp := &etcdserverpb.RangeResponse{Kvs: toKVs(kvs...)}
				batchSize = adjustBatchSize(batchSize, proto.Size(resp), embed.DefaultMaxRequestBytes)
				kvs = kvs[:0]
				if err := rs.Send(&etcdserverpb.RangeStreamResponse{RangeResponse: resp}); err != nil {
					return err
				}
			}
			kvs = append(kvs, kv)
			count++
		}

		if err := <-lr.Errorc; err != nil {
			return err
		}
	}

	var more bool
	if r.CountOnly || (limit > 0 && count > r.Limit) {
		if len(kvs) > 1 {
			kvs = kvs[:len(kvs)-1]
			more = true
		}
		var err error
		currentRev, count, err = l.backend.Count(ctx, key, end, revision)
		if err != nil {
			return err
		}
	}

	return rs.Send(&etcdserverpb.RangeStreamResponse{
		RangeResponse: &etcdserverpb.RangeResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: currentRev},
			Kvs:    toKVs(kvs...),
			More:   more,
			Count:  count,
		},
	})
}
