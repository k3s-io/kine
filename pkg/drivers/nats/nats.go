package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/nats/kv"
	natsserver "github.com/k3s-io/kine/pkg/drivers/nats/server"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	defaultBucket     = "kine"
	defaultReplicas   = 1
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond
)

var (
	toplevelKeyMatch = regexp.MustCompile(`(/[^/]*/[^/]*)(/.*)?`)
)

type Config struct {
	// Client URL which could be a list of comma separated URLs.
	clientURL string
	// Client connection options.
	clientOptions []nats.Option
	// Number of revisions to keep in history. Defaults to 10.
	revHistory uint8
	// Name of the bucket. Defaults to "kine".
	bucket string
	// Number of replicas for the bucket. Defaults to 1
	replicas int
	// Indicates the duration of a method before it is considered slow. Defaults to 500ms.
	slowThreshold time.Duration
	// If true, an embedded server will not be used.
	noEmbed bool
	// If true, use a socket for the embedded server.
	dontListen bool
	// Path to a server configuration file when embedded.
	serverConfig string
	// If true, the embedded server will log to stdout.
	stdoutLogging bool
	// The explicit host to listen on when embedded.
	host string
	// The explicit port to listen on when embedded.
	port int
}

type Driver struct {
	nc *nats.Conn
	js nats.JetStreamContext
	kv nats.KeyValue

	dirMu  *sync.RWMutex
	subMus map[string]*sync.RWMutex

	slowThreshold time.Duration
}

func (d *Driver) logMethod(dur time.Duration, str string, args ...any) {
	if dur > d.slowThreshold {
		logrus.Warnf(str, args...)
	} else {
		logrus.Tracef(str, args...)
	}
}

func getTopLevelKey(key string) string {
	if toplevelKeyMatch.MatchString(key) {
		matches := toplevelKeyMatch.FindStringSubmatch(key)
		return matches[1]
	}
	return ""
}

func (d *Driver) lockFolder(key string) (unlock func()) {
	lockFolder := getTopLevelKey(key)
	if lockFolder == "" {
		return func() {}
	}

	d.dirMu.Lock()
	mu, ok := d.subMus[lockFolder]
	if !ok {
		mu = &sync.RWMutex{}
		d.subMus[lockFolder] = mu
	}
	d.dirMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

type JSValue struct {
	KV           *server.KeyValue
	PrevRevision int64
	Create       bool
	Delete       bool
}

// New return an implementation of server.Backend using NATS + JetStream.
// See the `examples/nats.md` file for examples of connection strings.
func New(ctx context.Context, connection string, tlsInfo tls.Config) (server.Backend, error) {
	return newBackend(ctx, connection, tlsInfo, false)
}

// NewLegacy return an implementation of server.Backend using NATS + JetStream
// with legacy jetstream:// behavior, ignoring the embedded server.
func NewLegacy(ctx context.Context, connection string, tlsInfo tls.Config) (server.Backend, error) {
	return newBackend(ctx, connection, tlsInfo, true)
}

func newBackend(ctx context.Context, connection string, tlsInfo tls.Config, legacy bool) (server.Backend, error) {
	config, err := parseConnection(connection, tlsInfo)
	if err != nil {
		return nil, err
	}

	nopts := append(config.clientOptions, nats.Name("kine using bucket: "+config.bucket))

	// Run an embedded server if available and not disabled.
	if !legacy && natsserver.Embedded && !config.noEmbed {
		logrus.Infof("using an embedded NATS server")

		ns, err := natsserver.New(&natsserver.Config{
			Host:          config.host,
			Port:          config.port,
			ConfigFile:    config.serverConfig,
			DontListen:    config.dontListen,
			StdoutLogging: config.stdoutLogging,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create embedded NATS server: %w", err)
		}

		if config.dontListen {
			nopts = append(nopts, nats.InProcessServer(ns))
		}

		// Start the server.
		go ns.Start()
		logrus.Infof("started embedded NATS server")

		// Wait for the server to be ready.
		// TODO: limit the number of retries?
		for {
			if ns.ReadyForConnections(5 * time.Second) {
				break
			}
		}

		// TODO: No method on backend.Driver exists to indicate a shutdown.
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, os.Interrupt)
		go func() {
			<-sigch
			ns.Shutdown()
			logrus.Infof("embedded NATS server shutdown")
		}()

		// Use the local server's client URL.
		config.clientURL = ns.ClientURL()
	}

	if !config.dontListen {
		logrus.Infof("connecting to %s", config.clientURL)
	}

	logrus.Infof("using bucket: %s", config.bucket)

	conn, err := nats.Connect(config.clientURL, nopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	bucket, err := js.KeyValue(config.bucket)
	if err != nil && err == nats.ErrBucketNotFound {
		bucket, err = js.CreateKeyValue(
			&nats.KeyValueConfig{
				Bucket:      config.bucket,
				Description: "Holds kine key/values",
				History:     config.revHistory,
				Replicas:    config.replicas,
			})
	}

	kvB := kv.NewEncodedKV(bucket, &kv.EtcdKeyCodec{}, &kv.S2ValueCodec{})

	if err != nil {
		return nil, err
	}

	return &Driver{
		kv:            kvB,
		dirMu:         &sync.RWMutex{},
		subMus:        make(map[string]*sync.RWMutex),
		js:            js,
		slowThreshold: config.slowThreshold,
	}, nil
}

// parseConnection returns nats connection url, bucketName and []nats.Option, error
func parseConnection(dsn string, tlsInfo tls.Config) (*Config, error) {
	config := &Config{
		slowThreshold: defaultSlowMethod,
		revHistory:    defaultRevHistory,
		bucket:        defaultBucket,
		replicas:      defaultReplicas,
	}

	// Parse the first URL in the connection string which contains the
	// query parameters.
	connections := strings.Split(dsn, ",")
	u, err := url.Parse(connections[0])
	if err != nil {
		return nil, err
	}

	// Extract the host and port if embedded server is used.
	config.host = u.Hostname()
	if u.Port() != "" {
		config.port, _ = strconv.Atoi(u.Port())
	}

	// Extract the query parameters to build configuration.
	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	if v := queryMap.Get("bucket"); v != "" {
		config.bucket = v
	}

	if v := queryMap.Get("replicas"); v != "" {
		if r, err := strconv.ParseUint(v, 10, 8); err == nil {
			if r >= 1 && r <= 5 {
				config.replicas = int(r)
			} else {
				return nil, fmt.Errorf("invalid replicas, must be >= 1 and <= 5")
			}
		}
	}

	if d := queryMap.Get("slowMethod"); d != "" {
		if dur, err := time.ParseDuration(d); err == nil {
			config.slowThreshold = dur
		} else {
			return nil, fmt.Errorf("invalid slowMethod duration: %w", err)
		}
	}

	if r := queryMap.Get("revHistory"); r != "" {
		if revs, err := strconv.ParseUint(r, 10, 8); err == nil {
			if revs >= 2 && revs <= 64 {
				config.revHistory = uint8(revs)
			} else {
				return nil, fmt.Errorf("invalid revHistory, must be >= 2 and <= 64")
			}
		}
	}

	if tlsInfo.KeyFile != "" && tlsInfo.CertFile != "" {
		config.clientOptions = append(config.clientOptions, nats.ClientCert(tlsInfo.CertFile, tlsInfo.KeyFile))
	}

	if tlsInfo.CAFile != "" {
		config.clientOptions = append(config.clientOptions, nats.RootCAs(tlsInfo.CAFile))
	}

	if f := queryMap.Get("contextFile"); f != "" {
		if u.Host != "" {
			return config, fmt.Errorf("when using context endpoint no host should be provided")
		}

		logrus.Debugf("loading nats context file: %s", f)

		natsContext, err := natscontext.NewFromFile(f)
		if err != nil {
			return nil, err
		}

		connections = strings.Split(natsContext.ServerURL(), ",")

		// command line options provided to kine will override the file
		// https://github.com/nats-io/jsm.go/blob/v0.0.29/natscontext/context.go#L257
		// allows for user, creds, nke, token, certifcate, ca, inboxprefix from the context.json
		natsClientOpts, err := natsContext.NATSOptions(config.clientOptions...)
		if err != nil {
			return nil, err
		}
		config.clientOptions = natsClientOpts
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
				config.clientOptions = append(config.clientOptions, nats.UserInfo(userInfo[0], userInfo[1]))
			} else {
				config.clientOptions = append(config.clientOptions, nats.Token(userInfo[0]))
			}
		}
		connBuilder.WriteString(u.Host)
	}

	config.clientURL = connBuilder.String()

	// Config options only relevant if built with embedded NATS.
	if natsserver.Embedded {
		config.noEmbed = queryMap.Has("noEmbed")
		config.serverConfig = queryMap.Get("serverConfig")
		config.stdoutLogging = queryMap.Has("stdoutLogging")
		config.dontListen = queryMap.Has("dontListen")
	}

	logrus.Debugf("using config %#v", config)

	return config, nil
}

func (d *Driver) Start(ctx context.Context) error {
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := d.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	return nil
}

func (d *Driver) isKeyExpired(_ context.Context, createTime time.Time, value *JSValue) bool {

	requestTime := time.Now()
	expired := false
	if value.KV.Lease > 0 {
		if requestTime.After(createTime.Add(time.Second * time.Duration(value.KV.Lease))) {
			expired = true
			if err := d.kv.Delete(value.KV.Key); err != nil {
				logrus.Warnf("problem deleting expired key=%s, error=%v", value.KV.Key, err)
			}
		}
	}

	return expired
}

// Get returns the associated server.KeyValue
func (d *Driver) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if kvRet != nil {
			size = len(kvRet.Value)
		}
		fStr := "GET %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, size, errRet, dur)
	}()

	currentRev, err := d.currentRevision()
	if err != nil {
		return currentRev, nil, err
	}

	rev, kv, err := d.get(ctx, key, revision, false)
	if err == nil {
		if kv == nil {
			return currentRev, nil, nil
		}
		return rev, kv.KV, nil
	}

	if err == nats.ErrKeyNotFound {
		return currentRev, nil, nil
	}

	return rev, nil, err
}

func (d *Driver) get(ctx context.Context, key string, revision int64, includeDeletes bool) (int64, *JSValue, error) {
	compactRev, err := d.compactRevision()
	if err != nil {
		return 0, nil, err
	}

	// Get latest revision
	if revision <= 0 {
		entry, err := d.kv.Get(key)
		if err == nil {
			val, err := decode(entry)
			if err != nil {
				return 0, nil, err
			}

			if val.Delete && !includeDeletes {
				return 0, nil, nats.ErrKeyNotFound
			}

			if d.isKeyExpired(ctx, entry.Created(), &val) {
				return 0, nil, nats.ErrKeyNotFound
			}
			return val.KV.ModRevision, &val, nil
		}
		if err == nats.ErrKeyNotFound {
			return 0, nil, err
		}
		return 0, nil, err
	}

	if revision < compactRev {
		logrus.Warnf("requested revision has been compacted")
	}

	entry, err := d.kv.GetRevision(key, uint64(revision))
	if err == nil {
		val, err := decode(entry)
		if err != nil {
			return 0, nil, err
		}

		if val.Delete && !includeDeletes {
			return 0, nil, nats.ErrKeyNotFound
		}

		if d.isKeyExpired(ctx, entry.Created(), &val) {
			return 0, nil, nats.ErrKeyNotFound
		}
		return val.KV.ModRevision, &val, nil
	}

	if err == nats.ErrKeyNotFound {
		return 0, nil, err
	}

	return 0, nil, err
}

// Create
func (d *Driver) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), lease, revRet, errRet, dur)
	}()

	// Lock the folder containing this key.
	defer d.lockFolder(key)()

	// check if key exists already
	rev, prevKV, err := d.get(ctx, key, 0, true)
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
		seq, err := d.kv.Put(key, event)
		if err != nil {
			return 0, err
		}
		return int64(seq), nil
	}
	seq, err := d.kv.Create(key, event)
	if err != nil {
		return 0, err
	}
	return int64(seq), nil
}

func (d *Driver) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, dur)
	}()

	// Lock the folder containing this key.
	defer d.lockFolder(key)()

	rev, value, err := d.get(ctx, key, 0, true)
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

	deleteRev, err := d.kv.Put(key, deleteEventBytes)
	if err != nil {
		return rev, value.KV, false, nil
	}

	err = d.kv.Delete(key)
	if err != nil {
		return rev, value.KV, false, nil
	}

	return int64(deleteRev), value.KV, true, nil
}

func (d *Driver) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, dur)
	}()

	// its assumed that when there is a start key that that key exists.
	if strings.HasSuffix(prefix, "/") {
		if prefix == startKey || strings.HasPrefix(prefix, startKey) {
			startKey = ""
		}
	}

	rev, err := d.currentRevision()
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, 0)
	var count int64

	// startkey provided so get max revision after the startKey matching the prefix
	if startKey != "" {
		histories := make(map[string][]nats.KeyValueEntry)
		var minRev int64
		//var innerEntry nats.KeyValueEntry
		if entries, err := d.kv.History(startKey, nats.Context(ctx)); err == nil {
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

		keys, err := d.getKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}

		for _, key := range keys {
			if key != startKey {
				if history, err := d.kv.History(key, nats.Context(ctx)); err == nil {
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

		entries, err := d.getKeyValues(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}
		for _, e := range entries {
			if count < limit || limit == 0 {
				kv, err := decode(e)
				if !d.isKeyExpired(ctx, e.Created(), &kv) && err == nil {
					kvs = append(kvs, kv.KV)
					count++
				}
			} else {
				break
			}
		}

	} else {
		keys, err := d.getKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}
		if revision == 0 && len(keys) == 0 {
			return rev, nil, nil
		}

		for _, key := range keys {
			if count < limit || limit == 0 {
				if history, err := d.kv.History(key, nats.Context(ctx)); err == nil {
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

func (d *Driver) listAfter(ctx context.Context, prefix string, revision int64) (revRet int64, eventRet []*server.Event, errRet error) {

	entries, err := d.getKeyValues(ctx, prefix, false)

	if err != nil {
		return 0, nil, err
	}

	rev, err := d.currentRevision()
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
			if _, prevKV, err := d.Get(ctx, kv.KV.Key, "", 1, kv.PrevRevision); err == nil && prevKV != nil {
				event.PrevKV = prevKV
			}

			events = append(events, &event)
		}
	}
	return rev, events, nil
}

// Count returns an exact count of the number of matching keys and the current revision of the database
func (d *Driver) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "COUNT %s => rev=%d, count=%d, err=%v, duration=%s"
		d.logMethod(dur, fStr, prefix, revRet, count, err, dur)
	}()

	entries, err := d.getKeys(ctx, prefix, false)
	if err != nil {
		return 0, 0, err
	}
	// current revision
	currentRev, err := d.currentRevision()
	if err != nil {
		return 0, 0, err
	}
	return currentRev, int64(len(entries)), nil
}

func (d *Driver) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		fStr := "UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v, duration=%s"
		d.logMethod(dur, fStr, key, len(value), revision, lease, revRet, kvRev, updateRet, errRet, dur)
	}()

	// Lock the folder containing the key.
	defer d.lockFolder(key)()

	rev, prevKV, err := d.get(ctx, key, 0, false)

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

	seq, err := d.kv.Put(key, valueBytes)
	if err != nil {
		return 0, nil, false, err
	}

	updateValue.KV.ModRevision = int64(seq)

	return int64(seq), updateValue.KV, true, err

}

func (d *Driver) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {

	watcher, err := d.kv.(*kv.EncodedKV).Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))

	if revision > 0 {
		revision--
	}
	_, events, err := d.listAfter(ctx, prefix, revision)

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
						if _, prevEntry, prevErr := d.get(ctx, i.Key(), value.PrevRevision, false); prevErr == nil {
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
func (d *Driver) getPreviousEntry(ctx context.Context, entry nats.KeyValueEntry) (result *nats.KeyValueEntry, e error) {
	defer func() {
		if result != nil {
			logrus.Debugf("getPreviousEntry %s:%d found=true %d", entry.Key(), entry.Revision(), (*result).Revision())
		} else {
			logrus.Debugf("getPreviousEntry %s:%d found=false", entry.Key(), entry.Revision())
		}
	}()
	found := false
	entries, err := d.kv.History(entry.Key(), nats.Context(ctx))
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
func (d *Driver) DbSize(context.Context) (int64, error) {
	status, err := d.kv.Status()
	if err != nil {
		return -1, err
	}
	return int64(status.Bytes()), nil
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

func (d *Driver) currentRevision() (int64, error) {
	status, err := d.kv.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.LastSeq), nil
}

func (d *Driver) compactRevision() (int64, error) {
	status, err := d.kv.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.FirstSeq), nil
}

// getKeyValues returns a []nats.KeyValueEntry matching prefix
func (d *Driver) getKeyValues(ctx context.Context, prefix string, sortResults bool) ([]nats.KeyValueEntry, error) {
	watcher, err := d.kv.Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))
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
func (d *Driver) getKeys(ctx context.Context, prefix string, sortResults bool) ([]string, error) {
	watcher, err := d.kv.Watch(prefix, nats.MetaOnly(), nats.IgnoreDeletes(), nats.Context(ctx))
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
