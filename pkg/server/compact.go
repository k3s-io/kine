package server

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	compactRevKey = "compact_rev_key"           // key used by apiserver to track compaction, and also used internally by kine for the same purpose
	compactRevAPI = "compact_rev_key_apiserver" // key used by kine to store the apiserver's compact_rev_key value
)

func (l *LimitedServer) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	rev, err := l.backend.Compact(ctx, r.Revision)
	return &etcdserverpb.CompactionResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: rev,
		},
	}, err
}

func isCompact(txn *etcdserverpb.TxnRequest) (int64, []byte, bool) {
	// See https://github.com/kubernetes/kubernetes/blob/v1.34.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L219
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_VERSION &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		string(txn.Compare[0].Key) == compactRevKey {
		return txn.Compare[0].GetVersion(), txn.Success[0].GetRequestPut().Value, true
	}
	return 0, nil, false
}

// compact implements the version compare transaction that the apiserver uses
// to store the current compact rev to the compact_rev_key. Because kine
// uses this key internally, we instead operate on a substitute key.
func (l *LimitedServer) compact(ctx context.Context, compareVersion int64, value []byte) (*etcdserverpb.TxnResponse, error) {
	rev, kv, err := l.backend.Get(ctx, compactRevAPI, "", 1, 0, false)
	if err != nil {
		return nil, err
	}

	// kine does not actually track key versions, only revisions. Since the apiserver
	// compares the compact key version, we need to store the version alongside the
	// value, and increment it if the transaction succeeds. The apiserver watches this
	// key, and uses its value to prune internal caches.
	var modRev, version int64
	var curValue []byte
	if kv != nil {
		modRev = kv.ModRevision
		version, curValue = DecodeVersion(kv.Value)
	}

	if compareVersion == version {
		value := EncodeVersion(version+1, value)
		if version == 0 {
			rev, err = l.backend.Create(ctx, compactRevAPI, value, 0)
		} else {
			rev, _, _, err = l.backend.Update(ctx, compactRevAPI, value, modRev, 0)
		}

		if err != nil {
			// create or update failed, get the version from the current value
			rev, kv, err = l.backend.Get(ctx, compactRevAPI, "", 1, 0, false)
			if err != nil {
				return nil, err
			}
			if kv != nil {
				version, curValue = DecodeVersion(kv.Value)
			}
		} else {
			// on success, no response is required - just the revision header.
			return &etcdserverpb.TxnResponse{
				Header: &etcdserverpb.ResponseHeader{
					Revision: rev,
				},
				Succeeded: true,
			}, nil
		}
	}

	// on failure return a get response with the current key version and value
	return &etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: rev,
		},
		Succeeded: false,
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{},
						Kvs: []*mvccpb.KeyValue{
							{
								Key:     []byte(compactRevKey),
								Value:   curValue,
								Version: version,
							},
						},
						Count: 1,
					},
				},
			},
		},
	}, nil
}

// EncodeVersion encodes a version before the client-requested value. We do this
// because kine does not actually track an incrementing version for key
// revisions.
func EncodeVersion(version int64, value []byte) []byte {
	return fmt.Appendf(nil, "%d|%s", version, value)
}

// extractVersion returns the version and client-requested value encoded within
// the stored value. Older releases of kine stored only the revision, discarding
// the client-requested value as it was never read by the apiserver prior to
// Kubernetes 1.34.
func DecodeVersion(value []byte) (int64, []byte) {
	if ver, val, ok := bytes.Cut(value, []byte("|")); ok {
		version, _ := strconv.ParseInt(string(ver), 10, 64)
		return version, val
	}
	version, _ := strconv.ParseInt(string(value), 10, 64)
	return version, []byte("0")
}
