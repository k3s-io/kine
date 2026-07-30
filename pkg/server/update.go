package server

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func isUpdate(txn *etcdserverpb.TxnRequest) (int64, string, []byte, int64, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil {
		return txn.Compare[0].GetModRevision(),
			string(txn.Compare[0].Key),
			txn.Success[0].GetRequestPut().Value,
			txn.Success[0].GetRequestPut().Lease,
			true
	}
	return 0, "", nil, 0, false
}

func (l *LimitedServer) update(ctx context.Context, revision int64, key string, value []byte, lease int64) (*etcdserverpb.TxnResponse, error) {
	var (
		kv  *KeyValue
		rev int64
		ok  bool
		err error
	)

	if revision == 0 {
		rev, err = l.backend.Create(ctx, key, value, lease)
		if err == ErrKeyExists {
			rev, kv, err = l.backend.Get(ctx, key, rev, false)
		} else {
			ok = true
		}
	} else {
		rev, kv, ok, err = l.backend.Update(ctx, key, value, revision, lease)
	}

	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		logrus.Tracef("UPDATE key=%s, revision=%d, currentRev=%d", key, revision, rev)
	}

	if err != nil {
		return nil, err
	}

	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(rev),
		Succeeded: ok,
	}

	if ok {
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: txnHeader(rev),
					},
				},
			},
		}
	} else {
		kvs := toKVs(kv)
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(rev),
						Kvs:    kvs,
						Count:  int64(len(kvs)),
					},
				},
			},
		}
	}

	return resp, nil
}
