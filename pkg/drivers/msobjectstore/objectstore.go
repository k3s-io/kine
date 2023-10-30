package msobjectstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	DefaultLease      = int64(1)
	defaultBucket     = "kine"
	defaultReplicas   = 1
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond

	// TODO: define store and partition correctly
	controlNodeStore           = "control-node-store"
	controlNodePartition       = "control-node-partition"
	apiServerResourceKeyPrefix = "/registry/sample-apiserver/gateway.mulesoft.com/"

	healthCheckKey   = "health"
	healthCheckValue = "{\"health\":\"true\"}"

	//Get object parameters
	NotFoundIgnored    = true
	NotFoundNotIgnored = false
)

type driver struct {
	store         types.Storage
	slowThreshold time.Duration
	mut           sync.RWMutex
}

func New(_ context.Context, _ string, _ tls.Config) (be server.Backend, err error) {
	logrus.Info("msobjectstore driver.New()")

	osDriver := &driver{
		store:         osclient.New(),
		slowThreshold: defaultSlowMethod,
	}

	// TODO: store can be already created by another replica
	for {
		err = osDriver.store.UpsertStore(getStore())
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}

	be = osDriver

	return
}

func (d *driver) Start(_ context.Context) (err error) {
	logrus.Info("MS-ObjectStore Driver is starting...")

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.START: err=%v, duration=%s"
		d.logMethod(dur, fStr, err, dur)
	}()

	metadata := map[string]string{
		"test": "qwerty",
	}

	healthStoreCommand := types.NewStoreCmd(
		controlNodeStore,
		controlNodePartition,
		healthCheckKey,
		stubs.NewObject(
			stubs.WithKey(healthCheckKey),
			stubs.WithBinaryValue(healthCheckValue),
			stubs.WithMetadata(metadata),
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
		controlNodeStore,
		controlNodePartition,
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
	d.mut.RLock()
	defer d.mut.RUnlock()

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

	store, partition, resourceType, resourceKey, err := parseKey(key)
	if err != nil {
		return
	}

	getCommand := types.NewGetCommand(
		store,
		partition,
		resourceType,
	)

	var getResult types.GetResult
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		if err == osclient.ErrKeyNotFound {
			err = nil
		}
		return
	}

	val, err = parseKeyValue(getResult, resourceKey)
	if err == osclient.ErrKeyNotFound {
		err = nil
	}

	return
}

func (d *driver) List(_ context.Context, prefix, startKey string, limit, revision int64) (rev int64, kvs []*server.KeyValue, err error) {
	start := time.Now()
	d.mut.RLock()
	defer d.mut.RUnlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.LIST prefix=%s, ignored:req-start=%s, req-limit=%d, ignored:req-rev=%d => mock:res-rev=%d, size-kvs=%d, res-err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, rev, len(kvs), err, dur)
	}()

	// TODO: implement revision logic
	rev = DefaultRevision

	auxStr := strings.Replace(prefix, apiServerResourceKeyPrefix, "", 1)
	auxSlc := strings.SplitN(auxStr, "/", 2)

	var resourceNamePrefix string
	resourceBundleKey := auxSlc[0]
	if len(auxSlc) == 2 {
		resourceNamePrefix = formatKey(auxSlc[1])
	}

	var getResult types.GetResult
	getCommand := types.NewGetCommand(
		controlNodeStore,
		controlNodePartition,
		resourceBundleKey,
	)
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		logrus.Error("msobjectstore driver.LIST failed when store.GetValue(): ", err.Error())
		if err == osclient.ErrKeyNotFound {
			err = nil
		}
		return
	}

	if getResult.GetValue() == nil {
		logrus.Error("msobjectstore driver.LIST failed when res.GetValue() was nil")
		return
	}

	var resBundle osclient.Bundled
	if resBundle, err = parseBundleFromObjectValue(getResult.GetValue().BinaryValue); err != nil {
		logrus.Error("msobjectstore driver.LIST failed when parseBundleFromObjectValue: ", err.Error())
		return
	}

	logrus.Error("msobjectstore driver.LIST executing list over res: %+v ", resBundle)
	kvs = resBundle.List(resourceNamePrefix, limit)

	return
}

func (d *driver) Create(_ context.Context, key string, value []byte, lease int64) (rev int64, err error) {
	start := time.Now()
	d.mut.Lock()
	defer d.mut.Unlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, err, dur)
	}()

	// TODO: add revision logic
	rev = DefaultRevision
	_, _, resourceType, resourceKey, err := parseKey(key)
	if err != nil {
		return
	}

	var getResult types.GetResult
	resourceBundle := osclient.NewBundle()
	getCommand := types.NewGetCommand(
		controlNodeStore,
		controlNodePartition,
		resourceType,
	)
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		if err != osclient.ErrKeyNotFound {
			return
		}
		err = nil
	} else {
		if _, ok := getResult.GetValue().Metadata[resourceKey]; ok {
			err = osclient.ErrKeyAlreadyExists
			return
		}
		if resourceBundle, err = parseBundleFromObjectValue(getResult.GetValue().BinaryValue); err != nil {
			return
		}
	}

	resourceBundle.Upsert(resourceKey, newKeyValue(key, value))

	var data string
	if data, err = resourceBundle.Encode(); err != nil {
		return
	}

	metadata := resourceBundle.Index()
	currentTime := time.Now().String()
	metadata["created"] = currentTime
	metadata["updated"] = currentTime

	storeCommand := types.NewStoreCmd(
		controlNodeStore,
		controlNodePartition,
		resourceType,
		stubs.NewObject(
			stubs.WithKey(resourceType),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
	err = d.store.Store(storeCommand)

	return
}

func (d *driver) Delete(_ context.Context, key string, revision int64) (rev int64, kv *server.KeyValue, success bool, err error) {
	start := time.Now()
	d.mut.Lock()
	defer d.mut.Unlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.DELETE %s, ignored:revision=%d => rev=%d, kv=%d, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, revision, rev, err, dur)
	}()

	// TODO: add revision logic
	rev = DefaultRevision

	store, partition, resourceType, resourceKey, err := parseKey(key)
	if err != nil {
		return
	}

	var getResult types.GetResult
	getCommand := types.NewGetCommand(
		store,
		partition,
		resourceType,
	)
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		if err != osclient.ErrKeyNotFound {
			err = nil
			success = true
		}
		return
	} else {
		if _, ok := getResult.GetValue().Metadata[resourceKey]; !ok {
			success = true
			return
		}
	}

	var resBundle osclient.Bundled
	if resBundle, err = parseBundleFromObjectValue(getResult.GetValue().BinaryValue); err != nil {
		return
	}
	resBundle.Remove(resourceKey)
	data, err := resBundle.Encode()
	if err != nil {
		return
	}

	metadata := resBundle.Index()
	currentTime := time.Now().String()
	metadata["created"] = getResult.GetValue().Metadata["created"]
	metadata["updated"] = currentTime

	storeCommand := types.NewStoreCmd(
		controlNodeStore,
		controlNodePartition,
		resourceType,
		stubs.NewObject(
			stubs.WithKey(resourceType),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
	err = d.store.Store(storeCommand)

	success = true

	return
}

func (d *driver) Update(_ context.Context, key string, value []byte, revision, lease int64) (rev int64, val *server.KeyValue, success bool, err error) {
	start := time.Now()
	d.mut.Lock()
	defer d.mut.Unlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.UPDATE %s, size=%d, ignored:lease=%d => rev=%d, success=%t, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, rev, success, err, dur)
	}()

	// TODO: add revision logic
	rev = DefaultRevision

	store, partition, resourceType, resourceKey, err := parseKey(key)
	if err != nil {
		return
	}

	var getResult types.GetResult
	getCommand := types.NewGetCommand(
		store,
		partition,
		resourceType,
	)
	if getResult, err = d.store.GetValue(getCommand); err != nil {
		if err != osclient.ErrKeyNotFound {
			err = nil
			success = false
		}
		return
	} else {
		if _, ok := getResult.GetValue().Metadata[resourceKey]; !ok {
			success = false
			return
		}
	}

	var resBundle osclient.Bundled
	if resBundle, err = parseBundleFromObjectValue(getResult.GetValue().BinaryValue); err != nil {
		return
	}
	resBundle.Upsert(resourceKey, newKeyValue(key, value))

	var data string
	if data, err = resBundle.Encode(); err != nil {
		return
	}

	metadata := resBundle.Index()
	currentTime := time.Now().String()
	metadata["created"] = getResult.GetValue().Metadata["created"]
	metadata["updated"] = currentTime

	storeCommand := types.NewStoreCmd(
		controlNodeStore,
		controlNodePartition,
		resourceType,
		stubs.NewObject(
			stubs.WithKey(resourceType),
			stubs.WithBinaryValue(data),
			stubs.WithMetadata(metadata),
		))
	err = d.store.Store(storeCommand)

	success = true

	return
}

func (d *driver) Count(ctx context.Context, prefix string) (rev int64, count int64, err error) {
	start := time.Now()
	d.mut.RLock()
	defer d.mut.RUnlock()

	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.COUNT %s => rev=%d, count=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, rev, count, err, dur)
	}()

	// TODO: implement revision logic
	rev = DefaultRevision

	var keys *types.Keys
	if keys, err = d.store.GetKeys(controlNodeStore, controlNodePartition); err != nil {
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

func (d *driver) Watch(_ context.Context, key string, revision int64) <-chan []*server.Event {
	logrus.Infof("msobjectstoreDriver.Watch(key:%s, value:%+v, revision:%d)\n", key, revision)

	watchChan := make(chan []*server.Event, 100)

	return watchChan
}

func (d *driver) DbSize(_ context.Context) (size int64, err error) {
	d.mut.RLock()
	defer d.mut.RUnlock()

	fmt.Println("msobjectstore driver.DbSize()")

	var keys *types.Keys
	if keys, err = d.store.GetKeys(controlNodeStore, controlNodePartition); err != nil {
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

func getStore() *types.Store {
	return &types.Store{
		StoreID: controlNodeStore,
	}
}

func formatKey(s string) (key string) {
	key = strings.TrimPrefix(s, "/")
	key = strings.TrimSuffix(key, "/")
	return strings.ReplaceAll(key, "/", "_")
}

func newKeyValue(key string, value []byte) *server.KeyValue {
	return &server.KeyValue{
		Key:            key,
		CreateRevision: DefaultRevision,
		ModRevision:    DefaultRevision,
		Value:          value,
		Lease:          DefaultLease,
	}
}
