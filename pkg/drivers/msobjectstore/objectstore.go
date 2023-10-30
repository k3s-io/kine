package msobjectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	types "github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex"
	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex/stubs"
	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/osclient"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/sirupsen/logrus"
)

const (
	DefaultRevision   = 1
	defaultBucket     = "kine"
	defaultReplicas   = 1
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond

	// TODO: define store and partition correctly
	osStore     = "my-store"
	osPartition = "my-partition"

	healthCheckKey   = "health"
	healthCheckValue = "{\"health\":\"true\"}"

	//Get object parameters
	NotFoundIgnored    = true
	NotFoundNotIgnored = false
)

type driver struct {
	store         types.Storage
	slowThreshold time.Duration
}

func New(_ context.Context, _ string, _ tls.Config) (be server.Backend, err error) {
	logrus.Info("msobjectstore driver.New()")

	osDriver := &driver{
		store:         osclient.New(),
		slowThreshold: defaultSlowMethod,
	}

	// TODO: store can be already created by another replica
	if err = osDriver.store.UpsertStore(getStore()); err != nil {
		return
	}

	be = osDriver

	return
}

func (d *driver) Start(_ context.Context) (err error) {
	logrus.Info("MS-ObjectStore Driver is starting...")

	// TODO: remove sleep
	// waits flex os starts
	time.Sleep(time.Second * 5)

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.START: err=%v, duration=%s"
		d.logMethod(dur, fStr, err, dur)
	}()

	healthStoreCommand := types.NewStoreCmd(
		osStore,
		osPartition,
		healthCheckKey,
		stubs.NewObject(
			stubs.WithKey(healthCheckKey),
			stubs.WithBinaryValue(healthCheckValue),
		),
	)
	if err = d.store.Store(healthStoreCommand); err != nil {
		return
	}
	logrus.Info("msobjectstore driver.Start: health check entry successfully stored")

	_, err = d.HealthCheck()

	return
}

func (d *driver) HealthCheck() (ok bool, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.HealthCheck: ok:%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, ok, err, dur)
	}()

	var healthGetResult types.GetResult
	getHealthCommand := types.NewGetCommand(
		osStore,
		osPartition,
		healthCheckKey,
	)
	if healthGetResult, err = d.store.GetValue(getHealthCommand); err != nil {
		return
	}
	// logrus.Info("msobjectstore driver.Start: health check entry successfully retrieved")

	if healthGetResult.GetValue().BinaryValue != healthCheckValue {
		err = osclient.ErrHealthCheckFailed
	}

	ok = err == nil

	return
}

func (d *driver) Get(_ context.Context, key, rangeEnd string, limit, revision int64) (rev int64, val *server.KeyValue, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if val != nil {
			size = len(val.Value)
		}
		fStr := "msobjectstore driver.GET %s, rev=%d, ignored:rangeEnd=%s, ignored:limit=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rev, rangeEnd, limit, val != nil, size, err, dur)
	}()

	// TODO: implement revision logic
	rev = DefaultRevision
	key = formatKey(key)

	val, err = d.get(key, NotFoundIgnored)

	return
}

// TODO: will need to receive store and partition when not mocked
func (d *driver) get(key string, ignoreNotFound bool) (val *server.KeyValue, err error) {
	var (
		getResult types.GetResult
	)

	getCommand := types.NewGetCommand(
		osStore,
		osPartition,
		key,
	)
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		if err == osclient.ErrKeyNotFound && ignoreNotFound {
			err = nil
		}
		return
	}

	val, err = parseKeyValue(getResult)

	return
}

func parseKeyValue(gr types.GetResult) (kv *server.KeyValue, err error) {
	if gr.GetValue().ValueType != types.ObjectTypeBinary {
		err = osclient.ErrNotBinaryValue
		return
	}

	if gr.GetValue().BinaryValue == "" {
		err = osclient.ErrNullValue
		return
	}

	if err = json.Unmarshal([]byte(gr.GetValue().BinaryValue), &kv); err != nil {
		logrus.Errorf("error while unmarshalling binary value from store: %s", err)
		err = osclient.ErrKeyCastingFailed
		kv = nil
		return
	}

	if gr.GetValue().KeyID != kv.Key {
		err = osclient.ErrInvalidKey
	}

	return
}

func (d *driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (rev int64, kvs []*server.KeyValue, err error) { //TODO implement me
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.LIST req-prefix=%s, ignored:req-start=%s, req-limit=%d, ignored:req-rev=%d => mock:res-rev=%d, size-kvs=%d, res-err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, rev, len(kvs), err, dur)
	}()

	// TODO: implement revision logic
	rev = DefaultRevision

	var keys *types.Keys
	if keys, err = d.store.GetKeys(osStore, osPartition); err != nil {
		return
	}

	var kv *server.KeyValue
	for _, k := range keys.Values {
		if !strings.HasPrefix(k.KeyID, formatKey(prefix)) {
			continue
		}

		if kv, err = d.get(k.KeyID, NotFoundIgnored); err != nil {
			kvs = nil
			return
		}
		kvs = append(kvs, kv)

		if int64(len(kvs)) == limit {
			break
		}
	}

	return
}

func (d *driver) Create(_ context.Context, key string, value []byte, lease int64) (rev int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, err, dur)
	}()

	// TODO: add revision logic
	rev = DefaultRevision
	key = formatKey(key)

	if _, err = d.get(key, NotFoundIgnored); err != nil {
		return
	}

	var keyValueString string
	if keyValueString, err = buildServerKeyValueString(key, value, lease); err != nil {
		return
	}

	storeCommand := types.NewStoreCmd(
		osStore,
		osPartition,
		key,
		stubs.NewObject(
			stubs.WithKey(key),
			stubs.WithBinaryValue(keyValueString),
		))
	err = d.store.Store(storeCommand)

	return
}

func (d *driver) Delete(_ context.Context, key string, revision int64) (rev int64, kv *server.KeyValue, success bool, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.DELETE %s, ignored:revision=%d => rev=%d, kv=%d, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, revision, rev, err, dur)
	}()

	// TODO: add revision logic
	rev = DefaultRevision
	key = formatKey(key)

	if kv, err = d.get(key, NotFoundNotIgnored); err != nil || kv == nil {
		if err == osclient.ErrKeyNotFound || kv == nil {
			err = nil
			success = true
		}
		return
	}

	if err = d.store.DeleteValue(osStore, osPartition, key); err != nil {
		return
	}

	success = true

	return
}

func (d *driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (rev int64, val *server.KeyValue, success bool, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.UPDATE %s, size=%d, ignored:lease=%d => rev=%d, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, success, err, dur)
	}()

	rev = revision

	if _, err = d.get(formatKey(key), NotFoundNotIgnored); err != nil {
		return
	}

	formattedKey := formatKey(key)

	var keyValueString string
	if keyValueString, err = buildServerKeyValueString(formattedKey, value, lease); err != nil {
		return
	}

	storeCommand := types.NewStoreCmd(
		osStore,
		osPartition,
		formattedKey,
		stubs.NewObject(
			stubs.WithKey(formattedKey),
			stubs.WithBinaryValue(keyValueString),
		))
	if err = d.store.Store(storeCommand); err != nil {
		return
	}

	success = true

	return
}

func (d *driver) Count(ctx context.Context, prefix string) (rev int64, count int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.COUNT %s => rev=%d, count=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, rev, count, err, dur)
	}()

	// TODO: implement revision logic
	rev = DefaultRevision

	var keys *types.Keys
	if keys, err = d.store.GetKeys(osStore, osPartition); err != nil {
		return
	}

	if prefix == "" {
		count = int64(len(keys.Values))
		return
	}

	for _, k := range keys.Values {
		if !strings.HasPrefix(k.KeyID, prefix) {
			continue
		}

		count++
	}

	return
}

/*


func (d *driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (rev int64, val *server.KeyValue, success bool, err error) {
	logrus.Infof("msobjectstoreDriver.Update(key:%s, value:%+v, lease:%d, revision:%d)\n", key, value, lease, revision)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		kvRev := int64(0)
		if val != nil {
			kvRev = val.ModRevision
		}
		fStr := "msobjectstoreDriver.Update posta %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), revision, lease, rev, kvRev, nil, err, dur)
	}()

	success = false
	rev = revision
	val = buildServerKeyValueString(key, value, lease)

	if err = d.kv.Update(key, val); err != nil {
		if err == osclient.ErrKeyNotFound {
			err = nil
		}
		val = nil
		return
	}

	success = true

	return

}

func (d *driver) Delete(_ context.Context, key string, revision int64) (rev int64, val *server.KeyValue, success bool, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.Delete %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, rev, val != nil, success, err, dur)
	}()

	rev = 1
	success = false

	if err = d.kv.Delete(key); err != nil {
		if err == osclient.ErrKeyNotFound {
			success = true
			err = nil
		}
		return
	}

	success = true

	return
}

func (d *driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (rev int64, kvs []*server.KeyValue, err error) {
	//TODO implement me
	logrus.Infof("msobjectstoreDriver.List(prefix:%s, startKey:%s, limit:%d, revision:%d)\n", prefix, startKey, limit, revision)
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST req-prefix%s, req-start=%s, req-limit=%d, req-rev=%d => res-rev=%d, res-kvs=%d, res-err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, rev, len(kvs), err, dur)
	}()

	rev = 1
	kvs = d.kv.List(prefix)

	return
}

func (d *driver) Count(_ context.Context, prefix string) (rev int64, count int64, e error) {
	logrus.Infof("msobjectstoreDriver.Count(prefix:%s)\n", prefix)

	rev = DefaultRevision
	count = 123

	return
}

*/

func (d *driver) Watch(_ context.Context, key string, revision int64) <-chan []*server.Event {
	logrus.Infof("msobjectstoreDriver.Watch(key:%s, value:%+v, revision:%d)\n", key, revision)

	watchChan := make(chan []*server.Event, 100)

	return watchChan
}

func (d *driver) DbSize(_ context.Context) (size int64, err error) {
	fmt.Println("msobjectstore driver.DbSize()")

	var keys *types.Keys
	if keys, err = d.store.GetKeys(osStore, osPartition); err != nil {
		return 0, err
	}

	size = int64(len(keys.Values))

	return
}

func (d *driver) logMethod(dur time.Duration, str string, args ...any) {
	//if dur > d.slowThreshold {
	//	logrus.Warnf(str, args...)
	//} else {
	//	logrus.Tracef(str, args...)
	//}
	logrus.Infof(str, args...)
}

func buildServerKeyValueString(key string, value []byte, lease int64) (kvs string, err error) {
	kv := server.KeyValue{
		Key:            key,
		CreateRevision: DefaultRevision,
		ModRevision:    DefaultRevision,
		Value:          value,
		Lease:          lease,
	}

	var kvb []byte
	if kvb, err = json.Marshal(kv); err != nil {
		logrus.Errorf("msobjectstore driver.buildServerKeyValueString: error marshalling value into server key value: %q", err)
		err = osclient.ErrMarshallFailed
		return
	}

	kvs = string(kvb)

	return
}

func getStore() *types.Store {
	return &types.Store{
		StoreID: osStore,
	}
}

// TODO: store and partition (resource type) must be obtained from this key as well
func formatKey(key string) string {
	return strings.ReplaceAll(key, "/", "_")
}
