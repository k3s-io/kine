package nats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

var errStopKeyValue = errors.New("stopping key value")

type keySeq struct {
	key string
	seq uint64
}

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
	ctx        context.Context
	errch      chan error
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

func (w *streamWatcher) Err() <-chan error {
	return w.errch
}

func (w *streamWatcher) Stop() error {
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
	name       string
	revHistory int
	nkv        jetstream.KeyValue
	js         jetstream.JetStream
	kc         *keyCodec
	vc         *valueCodec
	bt         *btree.Map[string, []*seqOp]
	ew         *ExpireWatcher
	btm        sync.RWMutex
	lastSeq    atomic.Uint64
	compactRev atomic.Int64
	readyCh    chan struct{}
	ready      atomic.Bool
	seqCond    *sync.Cond // Broadcasts on btree update for read-after-write consistency
	ctx        context.Context
	cancel     context.CancelCauseFunc
	wg         *sync.WaitGroup
}

func NewKeyValue(name string, wg *sync.WaitGroup, bucket jetstream.KeyValue, js jetstream.JetStream, revHistory int, deleteFn DeleteFn) *KeyValue {
	kv := &KeyValue{
		name:       name,
		revHistory: revHistory,
		nkv:        bucket,
		js:         js,
		kc:         &keyCodec{},
		vc:         &valueCodec{},
		bt:         btree.NewMap[string, []*seqOp](0),
		ew:         NewExpireWatcher(deleteFn),
		seqCond:    sync.NewCond(&sync.Mutex{}),
		readyCh:    make(chan struct{}),
		wg:         wg,
	}

	return kv
}

func (e *KeyValue) Start(ctx context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)

	e.ctx = ctx
	e.cancel = cancel

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		for {
			errch := make(chan error, 1)

			go func() {
				errch <- e.btreeWatcher(ctx, e.revHistory)
			}()

			var err error
			select {
			case err = <-errch:
				logrus.Errorf("btree watcher: error: %v", err)

			case <-ctx.Done():
				logrus.Infof("%s: stopping key value store", e.name)
				err = context.Cause(ctx)
				// Parent context was canceled
				if err != errStopKeyValue {
					e.js.Conn().Close()
				}
				return
			}

			logrus.Debugf("%s: btree watcher: %v", e.name, err)

			if errors.Is(err, errStopKeyValue) {
				return
			}

			if errors.Is(err, nats.ErrConnectionClosed) {
				return
			}

			jitterSleep(time.Second)
		}
	}()
}

func (e *KeyValue) GetRevision(ctx context.Context, key string, revision int64, checkRevision bool) (jetstream.KeyValueEntry, error) {
	if revision < 0 {
		logrus.Warnf("getRevision: key=%s, revision=%d is less than 0, setting to 0", key, revision)
		revision = 0
	}

	if checkRevision {
		err := e.checkRevision(key, revision)
		if err != nil {
			return nil, err
		}
	}

	op, err := e.getRevisionOp(key, revision, false)
	if err != nil {
		return nil, err
	}

	if op == nil {
		return nil, jetstream.ErrKeyNotFound
	}

	if revision > 0 {
		revision = int64(op.seq)
	}

	return e.getRevision(ctx, key, revision)
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

	rev, err := e.nkv.Create(ctx, ek, buf.Bytes())
	if err != nil {
		return rev, err
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := e.waitForSequence(ctx, rev, waitForSeqTimeout); err != nil {
		logrus.Warnf("create: btree watcher lag: key=%s, seq=%d, err=%v", key, rev, err)
		// Continue anyway - data is in NATS, btree is lagging
	}

	return rev, nil
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

	rev, err := e.nkv.Update(ctx, ek, buf.Bytes(), last)
	if err != nil {
		return rev, err
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := e.waitForSequence(ctx, rev, waitForSeqTimeout); err != nil {
		logrus.Warnf("create: btree watcher lag: key=%s, seq=%d, err=%v", key, rev, err)
		// Continue anyway - data is in NATS, btree is lagging
	}

	return rev, nil
}

func (e *KeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return err
	}

	err = e.nkv.Delete(ctx, ek, opts...)
	if err != nil {
		return err
	}

	kvs, err := e.nkv.History(ctx, ek, jetstream.MetaOnly())
	if err != nil {
		logrus.Errorf("delete: error getting history for key %s: %v", key, err)
	}

	if len(kvs) > 0 {
		last := kvs[len(kvs)-1]
		if last.Operation() == jetstream.KeyValueDelete {
			if err := e.waitForSequence(ctx, last.Revision(), waitForSeqTimeout); err != nil {
				logrus.Warnf("delete: btree watcher lag: key=%s, seq=%d, err=%v", key, last.Revision(), err)
				// Continue anyway - data is in NATS, btree is lagging
			}
		} else {
			logrus.Warnf("delete: key was not deleted, last operation was key=%s, op=%s, seq=%d", key, last.Operation(), last.Revision())
		}
	}

	return nil
}

type KeyWatcher interface {
	Updates() <-chan jetstream.KeyValueEntry
	Stop() error
	Err() <-chan error
}

func (e *KeyValue) Watch(ctx context.Context, keys string, startRev int64) (KeyWatcher, error) {
	// Everything but the last token will be treated as a filter
	// on the watcher. The last token will used as a deliver-time filter.

	filter := keys

	if !strings.HasSuffix(filter, "/") {
		idx := strings.LastIndexByte(filter, '/')
		if idx > -1 {
			filter = keys[:idx+1]
		} else {
			// No '/' prefix in key and no '/' within key.
			// We should subscribe on the meta subject
			filter = noRootPrefix
		}
	}

	if filter != "" {
		p, err := e.kc.EncodeRange(filter)
		if err != nil {
			return nil, err
		}

		filter = fmt.Sprintf("$KV.%s.%s", e.nkv.Bucket(), p)
	}

	updates := make(chan jetstream.KeyValueEntry, 100)
	subjectPrefix := fmt.Sprintf("$KV.%s.", e.nkv.Bucket())

	handler := func(msg jetstream.Msg) {
		md, _ := msg.Metadata()

		if md.Sequence.Stream < uint64(e.compactRev.Load()) {
			return
		}

		key := strings.TrimPrefix(msg.Subject(), subjectPrefix)

		if keys != "" {
			dkey, err := e.kc.Decode(strings.TrimPrefix(key, "."))
			if err != nil || (keys != "/" && !strings.HasPrefix(dkey, keys)) {
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
	cfg.FilterSubjects = append(cfg.FilterSubjects, filter)

	con, err := e.js.OrderedConsumer(ctx, fmt.Sprintf("KV_%s", e.nkv.Bucket()), cfg)
	if err != nil {
		return nil, errors.Join(errors.New("watch: creating ordered consumer"), err)
	}

	errch := make(chan error, 1)
	ci := con.CachedInfo()
	cctx, err := con.Consume(handler,
		jetstream.ConsumeErrHandler(func(cctx jetstream.ConsumeContext, err error) {
			errch <- fmt.Errorf("watch: error consuming from %s: %v", ci.Name, err)
		}),
	)
	if err != nil {
		return nil, errors.Join(errors.New("watch: starting consuming"), err)
	}

	w := &streamWatcher{
		con:        con,
		cctx:       cctx,
		keyCodec:   e.kc,
		valueCodec: e.vc,
		updates:    updates,
		ctx:        ctx,
		errch:      errch,
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
	return int64(e.lastSeq.Load())
}

func (e *KeyValue) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) ([]jetstream.KeyValueEntry, error) {
	err := e.checkRevision("", revision)
	if err != nil {
		return nil, err
	}

	seekKey, exact := seekKey(prefix, startKey)

	it := e.bt.Iter()
	if seekKey != "" {
		if ok := it.Seek(seekKey); !ok {
			return nil, nil
		}
	}

	var matches []*keySeq

	e.btm.RLock()
	for {
		// KINE pagination logic relies on checking if response is > limit
		// if limit > 0 && len(matches) == int(limit) {
		// break
		// }

		k := it.Key()

		if exact && k != seekKey {
			break
		}

		if !strings.HasPrefix(k, prefix) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if op := getSeqOp(v, revision, false); op != nil {
			matches = append(matches, &keySeq{key: k, seq: op.seq})
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	var entries []jetstream.KeyValueEntry
	for _, m := range matches {
		valueEntry, err := e.getRevision(ctx, m.key, int64(m.seq))
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}

		entries = append(entries, valueEntry)
	}

	return entries, nil
}

func (e *KeyValue) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, error) {
	matches, err := e.getListOps(prefix, startKey, revision)
	if err != nil {
		return 0, err
	}

	return int64(len(matches)), nil
}

func (e *KeyValue) Stop() {
	e.cancel(errStopKeyValue)
	<-e.ctx.Done()
}

// waitForSequence waits for the btree watcher to consume up to the given sequence number.
// This ensures read-after-write consistency by blocking until the async watcher has
// updated the btree with the written data.
func (e *KeyValue) waitForSequence(ctx context.Context, seq uint64, timeout time.Duration) error {
	// Fast path: check if already processed
	if e.lastSeq.Load() >= seq {
		return nil
	}

	done := make(chan struct{}) // Closed when condition is met
	stop := make(chan struct{}) // Closed when we should abort waiting

	go func() {
		e.seqCond.L.Lock()
		defer e.seqCond.L.Unlock()

		for e.lastSeq.Load() < seq {
			select {
			case <-stop:
				return
			default:
			}

			e.seqCond.Wait() // Releases lock, waits for broadcast, reacquires lock
		}
		close(done)
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case <-done:
		return nil
	case <-t.C:
		close(stop)           // Signal goroutine to exit
		e.seqCond.Broadcast() // Wake up Wait() so it can check stop
		return fmt.Errorf("timeout waiting for sequence %d", seq)
	case <-ctx.Done():
		close(stop)           // Signal goroutine to exit
		e.seqCond.Broadcast() // Wake up Wait() so it can check stop
		return ctx.Err()
	}
}

// waitReady waits for the btree watcher to finish replaying history on startup.
// This prevents reads from seeing incomplete/inconsistent state during cold start.
func (e *KeyValue) waitReady(ctx context.Context) error {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return errors.New("timeout waiting for btree to be ready")
		case <-ctx.Done():
			return ctx.Err()
		case <-e.readyCh:
			logrus.Infof("%s: stream replay complete, ready for operations", e.name)
			return nil
		}
	}
}

// getRevision returns the NATS key value entry for the given key and revision.
// Expects valid NATS KV revision for key or zero for latest
func (e *KeyValue) getRevision(ctx context.Context, key string, revision int64) (jetstream.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	var ent jetstream.KeyValueEntry

	if revision <= 0 {
		ent, err = e.nkv.Get(ctx, ek)
	} else {
		ent, err = e.nkv.GetRevision(ctx, ek, uint64(revision))
	}
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) checkRevision(key string, revision int64) error {
	if key != "" {
		e.btm.RLock()
		_, ok := e.bt.Get(key)
		if !ok {
			e.btm.RUnlock()
			return jetstream.ErrKeyNotFound
		}
		e.btm.RUnlock()
	}

	if revision > 0 {
		currRev := e.BucketRevision()
		if revision > currRev {
			return server.ErrFutureRev
		}

		compactRev := e.compactRev.Load()

		if revision < compactRev {
			return server.ErrCompacted
		}
	}

	return nil
}

func (e *KeyValue) btreeWatcher(ctx context.Context, hsize int) error {
	br := e.BucketRevision()

	s, err := e.js.Stream(ctx, fmt.Sprintf("KV_%s", e.nkv.Bucket()))
	if err != nil {
		return fmt.Errorf("failed to get stream info: %w", err)
	}
	targetSeq := s.CachedInfo().State.LastSeq

	isStartupReplay := br == 0

	if isStartupReplay {
		if targetSeq == 0 {
			e.ready.Store(true)
			close(e.readyCh)
			isStartupReplay = false
		} else {
			logrus.Infof("%s: starting initial replay from 0 to %d", e.name, targetSeq)
		}
	}

	now := time.Now()
	w, err := e.Watch(ctx, "/", br)
	if err != nil {
		return fmt.Errorf("init: %s after %s", err, time.Since(now))
	}
	defer w.Stop()

	for {
		select {
		case err := <-w.Err():
			return fmt.Errorf("error: %s", err)

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

				if nd.Delete {
					op = jetstream.KeyValueDelete
				} else if nd.KV.Lease > 0 {
					ex = nd.CreateTime.Add(time.Second * time.Duration(nd.KV.Lease))
				}
			}

			e.btm.Lock()

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

			e.lastSeq.Store(seq)

			e.btm.Unlock()

			// Broadcast to all waiters that sequence has advanced
			e.seqCond.Broadcast()

			// Check if startup replay is complete
			if isStartupReplay && seq >= targetSeq && !e.ready.Load() {
				e.ready.Store(true)
				close(e.readyCh)
				logrus.Infof("%s: btree replay complete at seq=%d (target was %d, took %s)",
					e.name, seq, targetSeq, time.Since(now))
			}
		}
	}
}

func (e *KeyValue) getListOps(prefix, startKey string, revision int64) ([]*keySeq, error) {
	err := e.checkRevision("", revision)
	if err != nil {
		return nil, err
	}

	seekKey, exact := seekKey(prefix, startKey)

	it := e.bt.Iter()
	if seekKey != "" {
		if ok := it.Seek(seekKey); !ok {
			return nil, nil
		}
	}

	var matches []*keySeq

	e.btm.RLock()
	for {
		// KINE pagination logic relies on checking if response is > limit
		// if limit > 0 && len(matches) == int(limit) {
		// break
		// }

		k := it.Key()

		if exact && k != seekKey {
			break
		}

		if !strings.HasPrefix(k, prefix) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if op := getSeqOp(v, revision, false); op != nil {
			matches = append(matches, &keySeq{key: k, seq: op.seq})
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	return matches, nil
}

// getRevisionOp returns the latest btree operation for the requested revision
func (e *KeyValue) getRevisionOp(key string, revision int64, allowDeleted bool) (*seqOp, error) {
	e.btm.RLock()
	val, ok := e.bt.Get(key)
	if !ok {
		e.btm.RUnlock()
		return nil, jetstream.ErrKeyNotFound
	}
	e.btm.RUnlock()

	op := getSeqOp(val, revision, allowDeleted)

	if op == nil {
		return nil, jetstream.ErrKeyNotFound
	}

	return op, nil
}

// getSeqOp returns the latest sequence operation for the given key and global revision
func getSeqOp(val []*seqOp, revision int64, allowDeleted bool) *seqOp {
	for i := len(val) - 1; i >= 0; i-- {
		op := val[i]
		if revision <= 0 || op.seq <= uint64(revision) {
			if op.op != jetstream.KeyValuePut && !allowDeleted {
				return nil
			}

			return op
		}
	}

	return nil
}

func seekKey(prefix, startKey string) (string, bool) {
	exact := !strings.HasSuffix(prefix, "/") && startKey == ""

	if startKey == "" || startKey == prefix {
		return prefix, exact
	}

	seekKey := strings.TrimSuffix(prefix, "/")

	startKey = strings.TrimPrefix(startKey, seekKey)
	startKey = strings.TrimPrefix(startKey, "/")

	var k string
	if startKey != "" {
		k = fmt.Sprintf("%s/%s", seekKey, startKey)
	} else {
		k = prefix
	}

	return k, exact
}
