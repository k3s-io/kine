package sqlite

import (
	"context"
	"testing"

	"github.com/rancher/kine/pkg"

	"github.com/stretchr/testify/assert"

	"github.com/sirupsen/logrus"

	_ "github.com/mattn/go-sqlite3"
)

var (
	ctx = context.Background()
)

func create(t *testing.T, d *pkg.Driver, key string, value string) (int64, []byte) {
	rev, err := d.Insert(ctx, key, true, false, 0, 0, []byte(value), nil)
	if err != nil {
		t.Fatal(err)
	}
	return rev, []byte(value)
}

func update(t *testing.T, d *pkg.Driver, key string, value string, oldId int64, oldValue []byte) (int64, []byte) {
	rev, err := d.Insert(ctx, key, false, false, oldId, 0, []byte(value), oldValue)
	if err != nil {
		t.Fatal(err)
	}
	return rev, []byte(value)
}

func insertData(d *pkg.Driver, t *testing.T) {
	id, val := create(t, d, "/foo", "foo1")
	update(t, d, "/foo", "foo2", id, val)
	id, val = create(t, d, "/bar", "bar1")
	update(t, d, "/bar", "bar2", id, val)
}

func TestInsert(t *testing.T) {
	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	rev, err := d.Insert(ctx, "/foo",
		true, false, 0, 0, []byte("hi"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatal("rev should be 1 got", rev)
	}

	rev, err = d.Insert(ctx, "/foo",
		false, false, 1, 0, []byte("bye"), []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if rev != 2 {
		t.Fatal("rev should be 2 got", rev)
	}
}

func TestCurrentRevision(t *testing.T) {
	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)
	id, err := d.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id != 4 {
		t.Fatal("expected 2 got", id)
	}
}

func TestListCurrent(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	rows, err := d.ListCurrent(ctx, "%", 0)
	if err != nil {
		t.Fatal(err)
	}

	rev, compacted, events, err := pkg.RowsToEvents(rows)
	if err != nil {
		t.Fatal(err)
	}

	if compacted != 0 {
		t.Fatal("expected 0 got", compacted)
	}

	if rev != 4 {
		t.Fatal("expected 4 got", rev)
	}

	if len(events) != 2 {
		t.Fatal("expected 2 got", len(events))
	}

	if events[0].KV.Key != "/foo" {
		t.Fatal("expect /foo got", events[0].KV.Key)
	}

	if string(events[0].KV.Value) != "foo2" {
		t.Fatal("expect foo2 got", string(events[0].KV.Value))
	}

	if events[1].KV.Key != "/bar" {
		t.Fatal("expect /bar got", events[0].KV.Key)
	}

	if string(events[1].KV.Value) != "bar2" {
		t.Fatal("expect bar2 got", string(events[0].KV.Value))
	}
}

func TestListRevision(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	rows, err := d.List(ctx, "%", "", 0, 3)
	if err != nil {
		t.Fatal(err)
	}

	rev, compacted, events, err := pkg.RowsToEvents(rows)
	if err != nil {
		t.Fatal(err)
	}

	if compacted != 0 {
		t.Fatal("expected 0 got", compacted)
	}

	if rev != 4 {
		t.Fatal("expected 4 got", rev)
	}

	if len(events) != 2 {
		t.Fatal("expected 2 got", len(events))
	}

	if events[0].KV.Key != "/foo" {
		t.Fatal("expect /foo got", events[0].KV.Key)
	}

	if string(events[0].KV.Value) != "foo2" {
		t.Fatal("expect foo2 got", string(events[0].KV.Value))
	}

	if events[1].KV.Key != "/bar" {
		t.Fatal("expect /bar got", events[0].KV.Key)
	}

	if string(events[1].KV.Value) != "bar1" {
		t.Fatal("expect bar1 got", string(events[0].KV.Value))
	}
}

func TestListRevisionAfter(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	rows, err := d.List(ctx, "%", "/bar", 0, 3)
	if err != nil {
		t.Fatal(err)
	}

	rev, compacted, events, err := pkg.RowsToEvents(rows)
	if err != nil {
		t.Fatal(err)
	}

	if compacted != 0 {
		t.Fatal("expected 0 got", compacted)
	}

	if rev != 4 {
		t.Fatal("expected 4 got", rev)
	}

	if len(events) != 1 {
		t.Fatal("expected 2 got", len(events))
	}

	if events[0].KV.Key != "/bar" {
		t.Fatal("expect /bar got", events[0].KV.Key)
	}

	if string(events[0].KV.Value) != "bar1" {
		t.Fatal("expect bar1 got", string(events[0].KV.Value))
	}
}

func TestCount(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	rev, count, err := d.Count(ctx, "%")
	if err != nil {
		t.Fatal(err)
	}

	if rev != 4 {
		t.Fatal("expected 4 got", rev)
	}

	if count != 2 {
		t.Fatal("expected 2 got", count)
	}

	rev, count, err = d.Count(ctx, "/foo")
	if err != nil {
		t.Fatal(err)
	}

	if rev != 4 {
		t.Fatal("expected 4 got", rev)
	}

	if count != 1 {
		t.Fatal("expected 1 got", count)
	}
}

func TestSince(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	rows, err := d.Since(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	_, _, events, err := pkg.RowsToEvents(rows)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(events))
	assert.Equal(t, "/bar", events[0].KV.Key)
}

func TestCompact(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	d, err := pkg.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	insertData(d, t)

	num, err := d.GetCompactRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(0), num)

	err = d.SetCompactRevision(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}

	num, err = d.GetCompactRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(42), num)

	rows, err := d.ListCurrent(ctx, "%", 0)
	if err != nil {
		t.Fatal(err)
	}

	rev, compact, _, err := pkg.RowsToEvents(rows)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int64(5), rev)
	assert.Equal(t, int64(42), compact)
}
