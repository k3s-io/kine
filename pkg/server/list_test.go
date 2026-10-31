package server

import (
	"context"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// listCountBackend answers List and Count at different revisions, as if a
// write committed between the two queries. Other Backend methods are not
// implemented.
type listCountBackend struct {
	Backend
	listRev  int64
	countRev int64
	kvs      []*KeyValue
}

func (b *listCountBackend) List(_ context.Context, _, _ string, limit, _ int64, _ bool) (int64, []*KeyValue, error) {
	kvs := b.kvs
	if limit > 0 && int64(len(kvs)) > limit {
		kvs = kvs[:limit]
	}
	return b.listRev, kvs, nil
}

func (b *listCountBackend) Count(_ context.Context, _, _ string, _ int64) (int64, int64, error) {
	return b.countRev, int64(len(b.kvs)), nil
}

func newListCountServer() *LimitedServer {
	return &LimitedServer{backend: &listCountBackend{
		listRev:  500,
		countRev: 505,
		kvs:      []*KeyValue{{Key: "/a/1"}, {Key: "/a/2"}, {Key: "/a/3"}},
	}}
}

// The first page of a paginated list must report the revision its data was
// read at. Clients read the remaining pages at that revision and watch from
// just after it, so reporting the later count revision would skip any write
// that committed in between.
func TestListPaginatedHeaderUsesListRevision(t *testing.T) {
	resp, err := newListCountServer().list(context.Background(), &etcdserverpb.RangeRequest{
		Key:      []byte("/a/"),
		RangeEnd: []byte("/a0"),
		Limit:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Revision != 500 {
		t.Errorf("header revision = %d, want 500 (the list's revision)", resp.Header.Revision)
	}
	if !resp.More || len(resp.Kvs) != 2 || resp.Count != 3 {
		t.Errorf("got more=%v kvs=%d count=%d, want more=true kvs=2 count=3", resp.More, len(resp.Kvs), resp.Count)
	}
}

// A count-only request has no list query, so it reports the count's revision.
func TestListCountOnlyHeaderUsesCountRevision(t *testing.T) {
	resp, err := newListCountServer().list(context.Background(), &etcdserverpb.RangeRequest{
		Key:       []byte("/a/"),
		RangeEnd:  []byte("/a0"),
		CountOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Revision != 505 {
		t.Errorf("header revision = %d, want 505 (the count's revision)", resp.Header.Revision)
	}
	if resp.Count != 3 {
		t.Errorf("count = %d, want 3", resp.Count)
	}
}
