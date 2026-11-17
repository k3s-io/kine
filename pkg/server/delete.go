package server

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func isDelete(txn *etcdserverpb.TxnRequest) (int64, string, bool, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		return 0, string(rng.Key), rng.PrevKv, true
	}
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		return txn.Compare[0].GetModRevision(), string(txn.Success[0].GetRequestDeleteRange().Key), txn.Success[0].GetRequestDeleteRange().PrevKv, true
	}
	return 0, "", false, false
}

func (l *LimitedServer) delete(ctx context.Context, key string, revision int64, prevKV bool) (*etcdserverpb.TxnResponse, error) {
	rev, kv, ok, err := l.backend.Delete(ctx, key, revision)
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		logrus.Tracef("DELETE key=%s, revision=%d, currentRev=%d", key, revision, rev)
	}

	if err != nil {
		return nil, err
	}

	var kvs []*mvccpb.KeyValue
	if !ok {
		kvs = toKVs(kv)
		return &etcdserverpb.TxnResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: rev},
			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponseRange{
						ResponseRange: &etcdserverpb.RangeResponse{
							Header: &etcdserverpb.ResponseHeader{Revision: rev},
							Kvs:    kvs,
							Count:  int64(len(kvs)),
						},
					},
				},
			},
			Succeeded: false,
		}, nil
	}

	if prevKV {
		rev, kv, err = l.backend.Get(ctx, kv.Key, kv.ModRevision-1, false)
		if err != nil {
			return nil, err
		}
		kvs = toKVs(kv)
	}

	return &etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: rev},
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
						Header:  &etcdserverpb.ResponseHeader{Revision: rev},
						PrevKvs: kvs,
						Deleted: 1,
					},
				},
			},
		},
		Succeeded: true,
	}, nil
}
