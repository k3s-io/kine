package msobjectstore

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/osclient"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/sirupsen/logrus"
)

const (
	MockRev = 1
)

type Driver struct {
	//conn *nats.Conn
	kv osclient.KV
}

func New(_ context.Context, _ string, _ tls.Config) (server.Backend, error) {
	logrus.Info("msobjectstore Driver.New()")
	return &Driver{}, nil
}

func (d *Driver) Start(_ context.Context) error {
	logrus.Info("msobjectstore Driver.Start()")
	return nil
}

func (d *Driver) Get(_ context.Context, key, _ string, _, revision int64) (int64, *server.KeyValue, error) {
	logrus.Infof("msobjectstore Driver.Get(key:%s, revision:%d)\n", key, revision)

	v, e := d.kv.Get(key)

	return 0, v, e
	//return 666, &server.KeyValue{
	//	Key:            "keyMock1",
	//	CreateRevision: 0,
	//	ModRevision:    0,
	//	Value:          []byte(msg),
	//	Lease:          0,
	//}, nil
}

func (d *Driver) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	logrus.Infof("msobjectstore Driver.Create(key:%s, value:%+v)", key, value)

	createValue := &server.KeyValue{
		Key:            key,
		CreateRevision: MockRev,
		ModRevision:    MockRev,
		Value:          value,
		Lease:          lease,
	}

	if e := d.kv.Add(key, createValue); e != nil {
		return MockRev, e
	}

	return MockRev, nil
}

func (d *Driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	logrus.Infof("msobjectstoreDriver.Update(key:%s, value:%+v, lease:%d, revision:%d)\n", key, value, lease, revision)

	updateValue := &server.KeyValue{
		Key:            key,
		CreateRevision: MockRev,
		ModRevision:    MockRev,
		Value:          value,
		Lease:          lease,
	}

	if e := d.kv.Update(key, updateValue); e != nil {
		return 0, nil, false, e
	}

	return 0, updateValue, true, nil

}

func (d *Driver) Delete(_ context.Context, key string, revision int64) (rev int64, val *server.KeyValue, success bool, e error) {
	logrus.Infof("msobjectstore Driver.Delete(key:%s, revision:%d)\n", key, revision)
	rev = 0
	success = false

	if val, e = d.kv.Get(key); e != nil {
		return 0, nil, false, e
	}

	if e = d.kv.Delete(key); e != nil {
		return
	}

	success = true

	return
}

func (d *Driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (int64, []*server.KeyValue, error) {
	//TODO implement me
	logrus.Infof("msobjectstoreDriver.List(prefix:%s, startKey:%s, limit:%d, revision:%d)\n", prefix, startKey, limit, revision)

	kvs := make([]*server.KeyValue, 0)
	kvs = d.kv.List(prefix)

	return MockRev, kvs, nil
}

func (d *Driver) Count(_ context.Context, prefix string) (rev int64, count int64, e error) {
	logrus.Infof("msobjectstoreDriver.Count(prefix:%s)\n", prefix)

	rev = MockRev
	count = 123

	return
}

func (d *Driver) Watch(_ context.Context, key string, revision int64) <-chan []*server.Event {
	logrus.Infof("msobjectstoreDriver.Watch(key:%s, value:%+v, revision:%d)\n", key, revision)

	watchChan := make(chan []*server.Event, 100)

	return watchChan
}

func (d *Driver) DbSize(_ context.Context) (int64, error) {
	fmt.Println("msobjectstore Driver.DbSize()")

	return int64(len(d.kv.Keys())), nil

}
