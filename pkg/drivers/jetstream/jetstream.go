package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/jetstream/kv"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	defaultBucket     = "kine"
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond
)

var (
	toplevelKeyMatch = regexp.MustCompile(`(/[^/]*/[^/]*)(/.*)?`)
)

type Config struct {
	natsURL    string
	options    []nats.Option
	revHistory uint8
	bucket     string
	slowMethod time.Duration
}

type JetStream struct {
	kvBucket         nats.KeyValue
	kvBucketMutex    *sync.RWMutex
	kvDirectoryMutex *sync.RWMutex
	kvDirectoryMuxes map[string]*sync.RWMutex
	jetStream        nats.JetStreamContext
	slowMethod       time.Duration
	server.Backend
}

type JSValue struct {
	KV           *server.KeyValue
	PrevRevision int64
	Create       bool
	Delete       bool
}

// New get the JetStream Backend, establish connection to NATS JetStream. At the moment nats.go does not have
// connection string support so kine will use:
//		nats://(token|username:password)hostname:port?bucket=bucketName&contextFile=nats-context&slowMethod=<duration>&revHistory=<revCount>`.
//
// If contextFile is provided then do not provide a hostname:port in the endpoint URL, instead use the context file to
// provide the NATS server url(s).
//
//		bucket: specifies the bucket on the nats server for the k8s key/values for this cluster (optional)
//		contextFile: specifies the nats context file to load e.g. /etc/nats/context.json
//		revHistory: controls the rev history for JetStream defaults to 10 must be > 2 and <= 64
//		slowMethod: used to log methods slower than provided duration default 500ms
//
// Multiple urls can be passed in a comma separated format - only the first in the list will be evaluated for query
// parameters. While auth is valid in the url, the preferred way to pass auth is through a context file. If user/pass or
// token are provided in the url only the first one will be used for all urls.
///
// If no bucket query parameter is provided it will default to kine
//
// https://docs.nats.io/using-nats/nats-tools/nats_cli#configuration-contexts
//
// example nats-context.json:
/*
{
  "description": "optional context description",
  "url": "nats://127.0.0.1:4222",
  "token": "",
  "user": "",
  "password": "",
  "creds": "",
  "nkey": "",
  "cert": "",
  "key": "",
  "ca": "",
  "nsc": "",
  "jetstream_domain": "",
  "jetstream_api_prefix": "",
  "jetstream_event_prefix": ""
}
*/
func New(ctx context.Context, connection string, tlsInfo tls.Config) (server.Backend, error) {
	config, err := parseNatsConnection(connection, tlsInfo)
	if err != nil {
		return nil, err
	}

	logrus.Infof("using bucket: %s", config.bucket)
	logrus.Infof("connecting to %s", config.natsURL)

	nopts := append(config.options, nats.Name("k3s-server using bucket: "+config.bucket))

	conn, err := nats.Connect(config.natsURL, nopts...)
	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()

	if err != nil {
		return nil, err
	}

	bucket, err := js.KeyValue(config.bucket)
	if err != nil && err == nats.ErrBucketNotFound {
		bucket, err = js.CreateKeyValue(
			&nats.KeyValueConfig{
				Bucket:      config.bucket,
				Description: "Holds kine key/values",
				History:     config.revHistory,
			})
	}

	kvB := kv.NewEncodedKV(bucket, &kv.EtcdKeyCodec{}, &kv.S2ValueCodec{})

	if err != nil {
		return nil, err
	}

	return &JetStream{
		kvBucket:         kvB,
		kvBucketMutex:    &sync.RWMutex{},
		kvDirectoryMutex: &sync.RWMutex{},
		kvDirectoryMuxes: make(map[string]*sync.RWMutex),
		jetStream:        js,
		slowMethod:       config.slowMethod,
	}, nil
}

// parseNatsConnection returns nats connection url, bucketName and []nats.Option, error
func parseNatsConnection(dsn string, tlsInfo tls.Config) (*Config, error) {

	jsConfig := &Config{
		slowMethod: defaultSlowMethod,
		revHistory: defaultRevHistory,
	}
	connections := strings.Split(dsn, ",")
	jsConfig.bucket = defaultBucket

	jsConfig.options = make([]nats.Option, 0)

	u, err := url.Parse(connections[0])
	if err != nil {
		return nil, err
	}

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	if b, ok := queryMap["bucket"]; ok {
		jsConfig.bucket = b[0]
	}

	if r, ok := queryMap["slowMethod"]; ok {
		if dur, err := time.ParseDuration(r[0]); err == nil {
			jsConfig.slowMethod = dur
		} else {
			return nil, err
		}
	}

	if r, ok := queryMap["revHistory"]; ok {
		if revs, err := strconv.ParseUint(r[0], 10, 8); err == nil {
			if revs >= 2 && revs <= 64 {
				jsConfig.revHistory = uint8(revs)
			} else {
				return nil, fmt.Errorf("invalid revHistory, must be => 2 and <= 64")
			}
		}
	}

	contextFile, hasContext := queryMap["contextFile"]
	if hasContext && u.Host != "" {
		return jsConfig, fmt.Errorf("when using context endpoint no host should be provided")
	}

	if tlsInfo.KeyFile != "" && tlsInfo.CertFile != "" {
		jsConfig.options = append(jsConfig.options, nats.ClientCert(tlsInfo.CertFile, tlsInfo.KeyFile))
	}

	if tlsInfo.CAFile != "" {
		jsConfig.options = append(jsConfig.options, nats.RootCAs(tlsInfo.CAFile))
	}

	if hasContext {
		logrus.Infof("loading nats contextFile=%s", contextFile[0])

		natsContext, err := natscontext.NewFromFile(contextFile[0])
		if err != nil {
			return nil, err
		}

		connections = strings.Split(natsContext.ServerURL(), ",")

		// command line options provided to kine will override the file
		// https://github.com/nats-io/jsm.go/blob/v0.0.29/natscontext/context.go#L257
		// allows for user, creds, nke, token, certifcate, ca, inboxprefix from the context.json
		natsClientOpts, err := natsContext.NATSOptions(jsConfig.options...)
		if err != nil {
			return nil, err
		}
		jsConfig.options = natsClientOpts
	}

	connBuilder := strings.Builder{}
	for idx, c := range connections {
		if idx > 0 {
			connBuilder.WriteString(",")
		}

		u, err := url.Parse(c)
		if err != nil {
			return nil, err
		}

		if u.Scheme != "nats" {
			return nil, fmt.Errorf("invalid connection string=%s", c)
		}

		connBuilder.WriteString("nats://")

		if u.User != nil && idx == 0 {
			userInfo := strings.Split(u.User.String(), ":")
			if len(userInfo) > 1 {
				jsConfig.options = append(jsConfig.options, nats.UserInfo(userInfo[0], userInfo[1]))
			} else {
				jsConfig.options = append(jsConfig.options, nats.Token(userInfo[0]))
			}
		}
		connBuilder.WriteString(u.Host)
	}
	jsConfig.natsURL = connBuilder.String()

	logrus.Infof("using config %v", jsConfig)

	return jsConfig, nil
}

func (j *JetStream) Start(ctx context.Context) error {
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := j.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	return nil
}

func (j *JetStream) isKeyExpired(_ context.Context, createTime time.Time, value *JSValue) bool {

	requestTime := time.Now()
	expired := false
	if value.KV.Lease > 0 {
		if requestTime.After(createTime.Add(time.Second * time.Duration(value.KV.Lease))) {
			expired = true
			if err := j.kvBucket.Delete(value.KV.Key); err != nil {
				logrus.Warnf("problem deleting expired key=%s, error=%v", value.KV.Key, err)
			}
		}
	}

	return expired
}

// Get returns the associated server.KeyValue
func (j *JetStream) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	//logrus.Tracef("GET %s, rev=%d", key, revision)
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		size := 0
		if kvRet != nil {
			size = len(kvRet.Value)
		}
		fStr := "GET %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, key, revision, revRet, kvRet != nil, size, errRet, duration.String())
		} else {
			logrus.Tracef(fStr, key, revision, revRet, kvRet != nil, size, errRet, duration.String())
		}
	}()

	currentRev, err := j.currentRevision()
	if err != nil {
		return currentRev, nil, err
	}

	if rev, kv, err := j.get(ctx, key, revision, false); err == nil {
		if kv == nil {
			return currentRev, nil, nil
		}
		return rev, kv.KV, nil
	} else if err == nats.ErrKeyNotFound {
		return currentRev, nil, nil
	} else {
		return rev, nil, err
	}
}

func (j *JetStream) get(ctx context.Context, key string, revision int64, includeDeletes bool) (int64, *JSValue, error) {

	compactRev, err := j.compactRevision()
	if err != nil {
		return 0, nil, err
	}

	// Get latest revision
	if revision <= 0 {
		if entry, err := j.kvBucket.Get(key); err == nil {

			val, err := decode(entry)
			if err != nil {
				return 0, nil, err
			}

			if val.Delete && !includeDeletes {
				return 0, nil, nats.ErrKeyNotFound
			}

			if j.isKeyExpired(ctx, entry.Created(), &val) {
				return 0, nil, nats.ErrKeyNotFound
			}
			return val.KV.ModRevision, &val, nil
		} else if err == nats.ErrKeyNotFound {
			return 0, nil, err
		} else {
			return 0, nil, err
		}
	} else {
		if revision < compactRev {
			logrus.Warnf("requested revision that has been compacted")
		}
		if entry, err := j.kvBucket.GetRevision(key, uint64(revision)); err == nil {
			val, err := decode(entry)
			if err != nil {
				return 0, nil, err
			}

			if val.Delete && !includeDeletes {
				return 0, nil, nats.ErrKeyNotFound
			}

			if j.isKeyExpired(ctx, entry.Created(), &val) {
				return 0, nil, nats.ErrKeyNotFound
			}
			return val.KV.ModRevision, &val, nil
		} else if err == nats.ErrKeyNotFound {
			return 0, nil, err
		} else {
			return 0, nil, err
		}
	}
}

// Create
func (j *JetStream) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		fStr := "CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, key, len(value), lease, revRet, errRet, duration.String())
		} else {
			logrus.Tracef(fStr, key, len(value), lease, revRet, errRet, duration.String())
		}
	}()

	lockFolder := getTopLevelKey(key)
	if lockFolder != "" {
		j.kvDirectoryMutex.Lock()
		if _, ok := j.kvDirectoryMuxes[lockFolder]; !ok {
			j.kvDirectoryMuxes[lockFolder] = &sync.RWMutex{}
		}
		j.kvDirectoryMutex.Unlock()
		j.kvDirectoryMuxes[lockFolder].Lock()
		defer j.kvDirectoryMuxes[lockFolder].Unlock()
	}

	// check if key exists already
	rev, prevKV, err := j.get(ctx, key, 0, true)
	if err != nil && err != nats.ErrKeyNotFound {
		return 0, err
	}

	createValue := JSValue{
		Delete:       false,
		Create:       true,
		PrevRevision: rev,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: 0,
			ModRevision:    0,
			Value:          value,
			Lease:          lease,
		},
	}

	if prevKV != nil {
		if !prevKV.Delete {
			return 0, server.ErrKeyExists
		}
		createValue.PrevRevision = prevKV.KV.ModRevision
	}

	event, err := encode(createValue)
	if err != nil {
		return 0, err
	}

	if prevKV != nil {
		seq, err := j.kvBucket.Put(key, event)
		if err != nil {
			return 0, err
		}
		return int64(seq), nil
	}
	seq, err := j.kvBucket.Create(key, event)
	if err != nil {
		return 0, err
	}
	return int64(seq), nil
}

func (j *JetStream) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		fStr := "DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, duration.String())
		} else {
			logrus.Tracef(fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, duration.String())
		}
	}()
	lockFolder := getTopLevelKey(key)
	if lockFolder != "" {
		j.kvDirectoryMutex.Lock()
		if _, ok := j.kvDirectoryMuxes[lockFolder]; !ok {
			j.kvDirectoryMuxes[lockFolder] = &sync.RWMutex{}
		}
		j.kvDirectoryMutex.Unlock()
		j.kvDirectoryMuxes[lockFolder].Lock()
		defer j.kvDirectoryMuxes[lockFolder].Unlock()
	}

	rev, value, err := j.get(ctx, key, 0, true)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return rev, nil, true, nil
		}
		return rev, nil, false, err
	}

	if value == nil {
		return rev, nil, true, nil
	}

	if value.Delete {
		return rev, value.KV, true, nil
	}

	if revision != 0 && value.KV.ModRevision != revision {
		return rev, value.KV, false, nil
	}

	deleteEvent := JSValue{
		Delete:       true,
		PrevRevision: rev,
		KV:           value.KV,
	}
	deleteEventBytes, err := encode(deleteEvent)
	if err != nil {
		return rev, nil, false, err
	}

	deleteRev, err := j.kvBucket.Put(key, deleteEventBytes)
	if err != nil {
		return rev, value.KV, false, nil
	}

	err = j.kvBucket.Delete(key)
	if err != nil {
		return rev, value.KV, false, nil
	}

	return int64(deleteRev), value.KV, true, nil
}

func (j *JetStream) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		fStr := "LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, duration.String())
		} else {
			logrus.Tracef(fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, duration.String())
		}
	}()

	// its assumed that when there is a start key that that key exists.
	if strings.HasSuffix(prefix, "/") {
		if prefix == startKey || strings.HasPrefix(prefix, startKey) {
			startKey = ""
		}
	}

	rev, err := j.currentRevision()
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, 0)
	var count int64 = 0

	// startkey provided so get max revision after the startKey matching the prefix
	if startKey != "" {
		histories := make(map[string][]nats.KeyValueEntry)
		var minRev int64 = 0
		//var innerEntry nats.KeyValueEntry
		if entries, err := j.kvBucket.History(startKey, nats.Context(ctx)); err == nil {
			histories[startKey] = entries
			for i := len(entries) - 1; i >= 0; i-- {
				// find the matching startKey
				if int64(entries[i].Revision()) <= revision {
					minRev = int64(entries[i].Revision())
					logrus.Debugf("Found min revision=%d for key=%s", minRev, startKey)
					break
				}
			}
		} else {
			return 0, nil, err
		}

		keys, err := j.getKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}

		for _, key := range keys {
			if key != startKey {
				if history, err := j.kvBucket.History(key, nats.Context(ctx)); err == nil {
					histories[key] = history
				} else {
					// should not happen
					logrus.Warnf("no history for %s", key)
				}
			}
		}
		var nextRevID = minRev
		var nextRevision nats.KeyValueEntry
		for k, v := range histories {
			logrus.Debugf("Checking %s history", k)
			for i := len(v) - 1; i >= 0; i-- {
				if int64(v[i].Revision()) > nextRevID && int64(v[i].Revision()) <= revision {
					nextRevID = int64(v[i].Revision())
					nextRevision = v[i]
					logrus.Debugf("found next rev=%d", nextRevID)
					break
				} else if int64(v[i].Revision()) <= nextRevID {
					break
				}
			}
		}
		if nextRevision != nil {
			entry, err := decode(nextRevision)
			if err != nil {
				return 0, nil, err
			}
			kvs = append(kvs, entry.KV)
		}

		return rev, kvs, nil
	}

	current := true

	if revision != 0 {
		rev = revision
		current = false
	}

	if current {

		entries, err := j.getKeyValues(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}
		for _, e := range entries {
			if count < limit || limit == 0 {
				kv, err := decode(e)
				if !j.isKeyExpired(ctx, e.Created(), &kv) && err == nil {
					kvs = append(kvs, kv.KV)
					count++
				}
			} else {
				break
			}
		}

	} else {
		keys, err := j.getKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}
		if revision == 0 && len(keys) == 0 {
			return rev, nil, nil
		}

		for _, key := range keys {
			if count < limit || limit == 0 {
				if history, err := j.kvBucket.History(key, nats.Context(ctx)); err == nil {
					for i := len(history) - 1; i >= 0; i-- {
						if int64(history[i].Revision()) <= revision {
							if entry, err := decode(history[i]); err == nil {
								kvs = append(kvs, entry.KV)
								count++
							} else {
								logrus.Warnf("Could not decode %s rev=> %d", key, history[i].Revision())
							}
							break
						}
					}
				} else {
					// should not happen
					logrus.Warnf("no history for %s", key)
				}
			}
		}

	}
	return rev, kvs, nil
}

func (j *JetStream) listAfter(ctx context.Context, prefix string, revision int64) (revRet int64, eventRet []*server.Event, errRet error) {

	entries, err := j.getKeyValues(ctx, prefix, false)

	if err != nil {
		return 0, nil, err
	}

	rev, err := j.currentRevision()
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	events := make([]*server.Event, 0)
	for _, e := range entries {
		kv, err := decode(e)
		if err == nil && int64(e.Revision()) > revision {
			event := server.Event{
				Delete: kv.Delete,
				Create: kv.Create,
				KV:     kv.KV,
				PrevKV: &server.KeyValue{},
			}
			if _, prevKV, err := j.Get(ctx, kv.KV.Key, "", 1, kv.PrevRevision); err == nil && prevKV != nil {
				event.PrevKV = prevKV
			}

			events = append(events, &event)
		}
	}
	return rev, events, nil
}

// Count returns an exact count of the number of matching keys and the current revision of the database
func (j *JetStream) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		fStr := "COUNT %s => rev=%d, count=%d, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, prefix, revRet, count, err, duration.String())
		} else {
			logrus.Tracef(fStr, prefix, revRet, count, err, duration.String())
		}
	}()

	entries, err := j.getKeys(ctx, prefix, false)
	if err != nil {
		return 0, 0, err
	}
	// current revision
	currentRev, err := j.currentRevision()
	if err != nil {
		return 0, 0, err
	}
	return currentRev, int64(len(entries)), nil
}

func (j *JetStream) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	start := time.Now()
	defer func() {
		duration := time.Duration(time.Now().Nanosecond() - start.Nanosecond())
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		fStr := "UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v, duration=%s"
		if duration > j.slowMethod {
			logrus.Warnf(fStr, key, len(value), revision, lease, revRet, kvRev, updateRet, errRet, duration.String())
		} else {
			logrus.Tracef(fStr, key, len(value), revision, lease, revRet, kvRev, updateRet, errRet, duration.String())
		}
	}()

	lockFolder := getTopLevelKey(key)
	if lockFolder != "" {
		j.kvDirectoryMutex.Lock()
		if _, ok := j.kvDirectoryMuxes[lockFolder]; !ok {
			j.kvDirectoryMuxes[lockFolder] = &sync.RWMutex{}
		}
		j.kvDirectoryMutex.Unlock()
		j.kvDirectoryMuxes[lockFolder].Lock()
		defer j.kvDirectoryMuxes[lockFolder].Unlock()
	}

	rev, prevKV, err := j.get(ctx, key, 0, false)

	if err != nil {
		if err == nats.ErrKeyNotFound {
			return rev, nil, false, nil
		}
		return rev, nil, false, err
	}

	if prevKV == nil {
		return 0, nil, false, nil
	}

	if prevKV.KV.ModRevision != revision {
		return rev, prevKV.KV, false, nil
	}

	updateValue := JSValue{
		Delete:       false,
		Create:       false,
		PrevRevision: prevKV.KV.ModRevision,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: prevKV.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
	}
	if prevKV.KV.CreateRevision == 0 {
		updateValue.KV.CreateRevision = rev
	}

	valueBytes, err := encode(updateValue)
	if err != nil {
		return 0, nil, false, err
	}

	seq, err := j.kvBucket.Put(key, valueBytes)
	if err != nil {
		return 0, nil, false, err
	}

	updateValue.KV.ModRevision = int64(seq)

	return int64(seq), updateValue.KV, true, err

}

func (j *JetStream) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {

	watcher, err := j.kvBucket.(*kv.EncodedKV).Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))

	if revision > 0 {
		revision--
	}
	_, events, err := j.listAfter(ctx, prefix, revision)

	if err != nil {
		logrus.Errorf("failed to create watcher %s for revision %d", prefix, revision)
	}

	result := make(chan []*server.Event, 100)

	go func() {

		if len(events) > 0 {
			result <- events
			revision = events[len(events)-1].KV.ModRevision
		}

		for {
			select {
			case i := <-watcher.Updates():
				if i != nil {
					if int64(i.Revision()) > revision {
						events := make([]*server.Event, 1)
						var err error
						value := JSValue{
							KV:           &server.KeyValue{},
							PrevRevision: 0,
							Create:       false,
							Delete:       false,
						}
						prevValue := JSValue{
							KV:           &server.KeyValue{},
							PrevRevision: 0,
							Create:       false,
							Delete:       false,
						}
						lastEntry := &i

						value, err = decode(*lastEntry)
						if err != nil {
							logrus.Warnf("watch event: could not decode %s seq %d", i.Key(), i.Revision())
						}
						if _, prevEntry, prevErr := j.get(ctx, i.Key(), value.PrevRevision, false); prevErr == nil {
							if prevEntry != nil {
								prevValue = *prevEntry
							}
						}
						if err == nil {
							event := &server.Event{
								Create: value.Create,
								Delete: value.Delete,
								KV:     value.KV,
								PrevKV: prevValue.KV,
							}
							events[0] = event
							result <- events
						} else {
							logrus.Warnf("error decoding %s event %v", i.Key(), err)
							continue
						}
					}
				}
			case <-ctx.Done():
				logrus.Infof("watcher: %s context cancelled", prefix)
				if err := watcher.Stop(); err != nil && err != nats.ErrBadSubscription {
					logrus.Warnf("error stopping %s watcher: %v", prefix, err)
				}
				return
			}
		}
	}()
	return result
}

// getPreviousEntry returns the nats.KeyValueEntry previous to the one provided, if the previous entry is a nats.KeyValuePut
// operation. If it is not a KeyValuePut then it will return nil.
func (j *JetStream) getPreviousEntry(ctx context.Context, entry nats.KeyValueEntry) (result *nats.KeyValueEntry, e error) {
	defer func() {
		if result != nil {
			logrus.Debugf("getPreviousEntry %s:%d found=true %d", entry.Key(), entry.Revision(), (*result).Revision())
		} else {
			logrus.Debugf("getPreviousEntry %s:%d found=false", entry.Key(), entry.Revision())
		}
	}()
	found := false
	entries, err := j.kvBucket.History(entry.Key(), nats.Context(ctx))
	if err == nil {
		for idx := len(entries) - 1; idx >= 0; idx-- {
			if found {
				if entries[idx].Operation() == nats.KeyValuePut {
					return &entries[idx], nil
				}
				return nil, nil
			}
			if entries[idx].Revision() == entry.Revision() {
				found = true
			}
		}
	}

	return nil, nil
}

// DbSize get the kineBucket size from JetStream.
func (j *JetStream) DbSize(ctx context.Context) (int64, error) {
	keySize, err := j.bucketSize(ctx, j.kvBucket.Bucket())
	if err != nil {
		return -1, err
	}
	return keySize, nil
}

func (j *JetStream) bucketSize(ctx context.Context, bucket string) (int64, error) {
	os, err := j.jetStream.ObjectStore(bucket)
	if err != nil {
		return -1, err
	}
	s, err := os.Status()
	if err != nil {
		return -1, err
	}
	return int64(s.Size()), nil
}

func encode(v JSValue) ([]byte, error) {
	buf, err := json.Marshal(v)
	return buf, err
}

func decode(e nats.KeyValueEntry) (JSValue, error) {
	v := JSValue{}
	if e.Value() != nil {
		err := json.Unmarshal(e.Value(), &v)
		if err != nil {
			logrus.Debugf("key: %s", e.Key())
			logrus.Debugf("sequence number: %d", e.Revision())
			logrus.Debugf("bytes returned: %v", len(e.Value()))
			return v, err
		}
		v.KV.ModRevision = int64(e.Revision())
	}
	return v, nil
}

func (j *JetStream) currentRevision() (int64, error) {
	status, err := j.kvBucket.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.LastSeq), nil
}

func (j *JetStream) compactRevision() (int64, error) {
	status, err := j.kvBucket.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.FirstSeq), nil
}

// getKeyValues returns a []nats.KeyValueEntry matching prefix
func (j *JetStream) getKeyValues(ctx context.Context, prefix string, sortResults bool) ([]nats.KeyValueEntry, error) {
	watcher, err := j.kvBucket.Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := watcher.Stop()
		if err != nil {
			logrus.Warnf("failed to stop %s getKeyValues watcher", prefix)
		}
	}()

	var entries []nats.KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}

	if sortResults {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Key() < entries[j].Key()
		})
	}

	return entries, nil
}

// getKeys returns a list of keys matching a prefix
func (j *JetStream) getKeys(ctx context.Context, prefix string, sortResults bool) ([]string, error) {
	watcher, err := j.kvBucket.Watch(prefix, nats.MetaOnly(), nats.IgnoreDeletes(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := watcher.Stop()
		if err != nil {
			logrus.Warnf("failed to stop %s getKeys watcher", prefix)
		}
	}()

	var keys []string
	// grab all matching keys immediately
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		keys = append(keys, entry.Key())
	}

	if sortResults {
		sort.Strings(keys)
	}

	return keys, nil
}

func getTopLevelKey(key string) string {
	if toplevelKeyMatch.MatchString(key) {
		matches := toplevelKeyMatch.FindStringSubmatch(key)
		return matches[1]
	}
	return ""
}
