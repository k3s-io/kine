package nats

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

type entry struct {
	kc    *keyCodec
	vc    *valueCodec
	entry jetstream.KeyValueEntry
}

func (e *entry) Key() string {
	dk, err := e.kc.Decode(e.entry.Key())
	// should not happen
	if err != nil {
		// should not happen
		logrus.Warnf("could not decode key %s: %v", e.entry.Key(), err)
		return ""
	}

	return dk
}

func (e *entry) Bucket() string { return e.entry.Bucket() }
func (e *entry) Value() []byte {
	buf := new(bytes.Buffer)
	if err := e.vc.Decode(bytes.NewBuffer(e.entry.Value()), buf); err != nil {
		// should not happen
		logrus.Warnf("could not decode value for %s: %v", e.Key(), err)
	}
	return buf.Bytes()
}
func (e *entry) Revision() uint64                { return e.entry.Revision() }
func (e *entry) Created() time.Time              { return e.entry.Created() }
func (e *entry) Delta() uint64                   { return e.entry.Delta() }
func (e *entry) Operation() jetstream.KeyValueOp { return e.entry.Operation() }

type seqOp struct {
	seq uint64
	op  jetstream.KeyValueOp
	ex  time.Time
}

type streamWatcher struct {
	con        jetstream.Consumer
	cctx       jetstream.ConsumeContext
	keyCodec   *keyCodec
	valueCodec *valueCodec
	updates    chan jetstream.KeyValueEntry
	keyPrefix  string
	ctx        context.Context
	cancel     context.CancelFunc
}

func (w *streamWatcher) Context() context.Context {
	if w == nil {
		return nil
	}
	return w.ctx
}

func (w *streamWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return w.updates
}

func (w *streamWatcher) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	if w.cctx != nil {
		w.cctx.Stop()
	}
	return nil
}

type kvEntry struct {
	key       string
	bucket    string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation jetstream.KeyValueOp
}

func (e *kvEntry) Key() string {
	return e.key
}

func (e *kvEntry) Bucket() string { return e.bucket }
func (e *kvEntry) Value() []byte {
	return e.value
}
func (e *kvEntry) Revision() uint64                { return e.revision }
func (e *kvEntry) Created() time.Time              { return e.created }
func (e *kvEntry) Delta() uint64                   { return e.delta }
func (e *kvEntry) Operation() jetstream.KeyValueOp { return e.operation }

type KeyValue struct {
	nkv     jetstream.KeyValue
	js      jetstream.JetStream
	kc      *keyCodec
	vc      *valueCodec
	bt      *btree.Map[string, []*seqOp]
	btm     sync.RWMutex
	lastSeq uint64
}

func (e *KeyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.nkv.Get(ctx, ek)
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) GetRevision(ctx context.Context, key string, revision uint64) (jetstream.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.nkv.GetRevision(ctx, ek, revision)
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.nkv.Create(ctx, ek, buf.Bytes())
}

func (e *KeyValue) Update(ctx context.Context, key string, value []byte, last uint64) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.nkv.Update(ctx, ek, buf.Bytes(), last)
}

func (e *KeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return err
	}

	return e.nkv.Delete(ctx, ek, opts...)
}

func (e *KeyValue) Watch(ctx context.Context, keys string, startRev int64) (jetstream.KeyWatcher, error) {
	// Everything but the last token will be treated as a filter
	// on the watcher. The last token will used as a deliver-time filter.
	filter := keys
	if !strings.HasSuffix(filter, "/") {
		idx := strings.LastIndexByte(filter, '/')
		if idx > -1 {
			filter = keys[:idx+1]
		}
	}

	if filter != "" {
		p, err := e.kc.EncodeRange(filter)
		if err != nil {
			return nil, err
		}
		filter = fmt.Sprintf("$KV.%s.%s", e.nkv.Bucket(), p)
	}

	wctx, cancel := context.WithCancel(ctx)

	updates := make(chan jetstream.KeyValueEntry, 100)
	subjectPrefix := fmt.Sprintf("$KV.%s.", e.nkv.Bucket())

	handler := func(msg jetstream.Msg) {
		md, _ := msg.Metadata()
		key := strings.TrimPrefix(msg.Subject(), subjectPrefix)

		if keys != "" {
			dkey, err := e.kc.Decode(strings.TrimPrefix(key, "."))
			if err != nil || !strings.HasPrefix(dkey, keys) {
				return
			}
		}

		// Default is PUT
		var op jetstream.KeyValueOp
		switch msg.Headers().Get("KV-Operation") {
		case "DEL":
			op = jetstream.KeyValueDelete
		case "PURGE":
			op = jetstream.KeyValuePurge
		}
		// Not currently used...
		delta := 0

		updates <- &entry{
			kc: e.kc,
			vc: e.vc,
			entry: &kvEntry{
				key:       key,
				bucket:    e.nkv.Bucket(),
				value:     msg.Data(),
				revision:  md.Sequence.Stream,
				created:   md.Timestamp,
				delta:     uint64(delta),
				operation: op,
			},
		}
	}

	var dp jetstream.DeliverPolicy
	var cfg jetstream.OrderedConsumerConfig
	if startRev <= 0 {
		dp = jetstream.DeliverAllPolicy
	} else {
		dp = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = uint64(startRev)
	}
	cfg.DeliverPolicy = dp

	con, err := e.js.OrderedConsumer(ctx, fmt.Sprintf("KV_%s", e.nkv.Bucket()), cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	ci := con.CachedInfo()
	cctx, err := con.Consume(handler,
		jetstream.ConsumeErrHandler(func(cctx jetstream.ConsumeContext, err error) {
			if !strings.Contains(err.Error(), "Server Shutdown") {
				logrus.Warnf("error consuming from %s: %v", ci.Name, err)
			}
		}),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	w := &streamWatcher{
		con:        con,
		cctx:       cctx,
		keyCodec:   e.kc,
		valueCodec: e.vc,
		updates:    updates,
		ctx:        wctx,
		cancel:     cancel,
	}

	return w, nil
}

// BucketSize returns the size of the bucket in bytes.
func (e *KeyValue) BucketSize(ctx context.Context) (int64, error) {
	status, err := e.nkv.Status(ctx)
	if err != nil {
		return 0, err
	}
	return int64(status.Bytes()), nil
}

// BucketRevision returns the latest revision of the bucket.
func (e *KeyValue) BucketRevision() int64 {
	e.btm.RLock()
	s := e.lastSeq
	e.btm.RUnlock()
	return int64(s)
}

func (e *KeyValue) btreeWatcher(ctx context.Context) error {
	w, err := e.Watch(ctx, "/", int64(e.lastSeq))
	if err != nil {
		return err
	}
	defer w.Stop()

	status, _ := e.nkv.Status(ctx)
	hsize := status.History()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case x := <-w.Updates():
			if x == nil {
				continue
			}

			seq := x.Revision()
			op := x.Operation()

			key := x.Key()

			var ex time.Time
			if op == jetstream.KeyValuePut {
				var nd natsData
				err = nd.Decode(x)
				if err != nil {
					continue
				}
				if nd.KV.Lease > 0 {
					ex = nd.CreateTime.Add(time.Second * time.Duration(nd.KV.Lease))
				}
			}

			e.btm.Lock()
			e.lastSeq = seq
			val, ok := e.bt.Get(key)
			if !ok {
				val = make([]*seqOp, 0, hsize)
			}
			// Remove the oldest entry.
			if len(val) == cap(val) {
				val = append(val[:0], val[1:]...)
			}
			val = append(val, &seqOp{
				seq: seq,
				op:  op,
				ex:  ex,
			})
			e.bt.Set(key, val)
			e.btm.Unlock()
		}
	}
}

type keySeq struct {
	key string
	seq uint64
}

func (e *KeyValue) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, error) {
	seekKey := prefix
	if startKey != "" {
		seekKey = strings.TrimSuffix(seekKey, "/")
		seekKey = fmt.Sprintf("%s/%s", seekKey, startKey)
	}

	it := e.bt.Iter()
	if seekKey != "" {
		if ok := it.Seek(seekKey); !ok {
			return 0, nil
		}
	}

	var count int64
	now := time.Now()

	e.btm.RLock()
	for {
		k := it.Key()
		if !strings.HasPrefix(k, prefix) {
			break
		}
		v := it.Value()

		// Get the latest update for the key.
		if revision <= 0 {
			so := v[len(v)-1]
			if so.op == jetstream.KeyValuePut {
				if so.ex.IsZero() || so.ex.After(now) {
					count++
				}
			}
		} else {
			// Find the latest update below the given revision.
			for i := len(v) - 1; i >= 0; i-- {
				so := v[i]
				if so.seq <= uint64(revision) {
					if so.op == jetstream.KeyValuePut {
						if so.ex.IsZero() || so.ex.After(now) {
							count++
						}
					}
					break
				}
			}
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	return count, nil
}

func (e *KeyValue) List(ctx context.Context, prefix, startKey string, limit, revision int64) ([]jetstream.KeyValueEntry, error) {
	seekKey := prefix
	if startKey != "" {
		seekKey = strings.TrimSuffix(seekKey, "/")
		seekKey = fmt.Sprintf("%s/%s", seekKey, startKey)
	}

	it := e.bt.Iter()
	if seekKey != "" {
		if ok := it.Seek(seekKey); !ok {
			return nil, nil
		}
	}

	var matches []*keySeq
	now := time.Now()

	e.btm.RLock()

	for {
		if limit > 0 && len(matches) == int(limit) {
			break
		}

		k := it.Key()
		if !strings.HasPrefix(k, prefix) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if revision <= 0 {
			so := v[len(v)-1]
			if so.op == jetstream.KeyValuePut {
				if so.ex.IsZero() || so.ex.After(now) {
					matches = append(matches, &keySeq{key: k, seq: so.seq})
				}
			}
		} else {
			// Find the latest update below the given revision.
			for i := len(v) - 1; i >= 0; i-- {
				so := v[i]
				if so.seq <= uint64(revision) {
					if so.op == jetstream.KeyValuePut {
						if so.ex.IsZero() || so.ex.After(now) {
							matches = append(matches, &keySeq{key: k, seq: so.seq})
						}
					}
					break
				}
			}
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	logrus.Debugf("kv: list: got %d matches from btree", len(matches))

	var entries []jetstream.KeyValueEntry
	for _, m := range matches {
		e, err := e.GetRevision(ctx, m.key, m.seq)
		if err != nil {
			logrus.Errorf("get revision in list error: %s @ %d: %v", m.key, m.seq, err)
			continue
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func NewKeyValue(ctx context.Context, bucket jetstream.KeyValue, js jetstream.JetStream) *KeyValue {
	kv := &KeyValue{
		nkv: bucket,
		js:  js,
		kc:  &keyCodec{},
		vc:  &valueCodec{},
		bt:  btree.NewMap[string, []*seqOp](0),
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			err := kv.btreeWatcher(ctx)
			if err != nil {
				logrus.Errorf("btree watcher error: %v", err)
			}
		}
	}()

	return kv
}
