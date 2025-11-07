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
	nkv        jetstream.KeyValue
	js         jetstream.JetStream
	kc         *keyCodec
	vc         *valueCodec
	bt         *btree.Map[string, []*seqOp]
	btm        sync.RWMutex
	lastSeq    atomic.Uint64
	compactRev atomic.Int64
	ready      atomic.Bool
	seqCond    *sync.Cond // Broadcasts on btree update for read-after-write consistency
	ctx        context.Context
	cancel     context.CancelCauseFunc
}

func NewKeyValue(ctx context.Context, name string, bucket jetstream.KeyValue, js jetstream.JetStream, minRetain int64, hsize int) *KeyValue {
	ctx, cancel := context.WithCancelCause(ctx)

	kv := &KeyValue{
		name:    name,
		nkv:     bucket,
		js:      js,
		kc:      &keyCodec{},
		vc:      &valueCodec{},
		bt:      btree.NewMap[string, []*seqOp](0),
		seqCond: sync.NewCond(&sync.Mutex{}),
		ctx:     ctx,
		cancel:  cancel,
	}

	go func() {
		for {
			errch := make(chan error, 1)

			go func() {
				errch <- kv.btreeWatcher(ctx, hsize)
			}()

			var err error
			select {
			case err = <-errch:

			case <-ctx.Done():
				err = context.Cause(ctx)
			}

			logrus.Debugf("%s: btree watcher: %v", name, err)

			if errors.Is(err, errStopKeyValue) {
				return
			}

			if errors.Is(err, nats.ErrConnectionClosed) {
				return
			}

			jitterSleep(time.Second)
		}
	}()

	return kv
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

	op, err := e.getRevisionOp(key, revision, true, false)
	if err != nil {
		return nil, err
	}

	if !op.ex.IsZero() && op.ex.Before(time.Now()) {
		err := e.Delete(ctx, key, jetstream.LastRevision(op.seq))
		if err != nil {
			logrus.Errorf("kv: GetRevision: error deleting expired key %s: %v", key, err)
		}
		return nil, jetstream.ErrKeyNotFound
	}

	if op == nil {
		logrus.Warnf("no revision op found in btree for key: %s", key)
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
		return nil, errors.Join(fmt.Errorf("watch: creating ordered consumer"), err)
	}

	errch := make(chan error, 1)
	ci := con.CachedInfo()
	cctx, err := con.Consume(handler,
		jetstream.ConsumeErrHandler(func(cctx jetstream.ConsumeContext, err error) {
			errch <- fmt.Errorf("watch: error consuming from %s: %v", ci.Name, err)
		}),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("watch: starting consuming"), err)
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
	seekKey, exact := seekKey(prefix, startKey)

	err := e.checkRevision("", revision)
	if err != nil {
		return nil, err
	}

	it := e.bt.Iter()
	if seekKey != "" {
		if ok := it.Seek(seekKey); !ok {
			return nil, nil
		}
	}

	var matches []*keySeq
	var expired []*keySeq

	now := time.Now()

	e.btm.RLock()
	for {

		// KINE pagination logic relies on checking if response is > limit
		/**
		if limit > 0 && len(matches) == int(limit) {
			break
		}
		**/

		k := it.Key()

		if exact && k != seekKey {
			break
		}

		if !strings.HasPrefix(k, prefix) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if op := getSeqOp(v, revision, true, false); op != nil {
			if !op.ex.IsZero() && op.ex.Before(now) {
				expired = append(expired, &keySeq{key: k, seq: op.seq})
			} else {
				matches = append(matches, &keySeq{key: k, seq: op.seq})
			}
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	for _, ex := range expired {
		err = e.Delete(ctx, ex.key, jetstream.LastRevision(ex.seq))
		if err != nil {
			logrus.Errorf("kv: list: error deleting expired key %s: %v", ex.key, err)
		}
	}

	var entries []jetstream.KeyValueEntry
	for _, m := range matches {
		valueEntry, err := e.getRevision(ctx, m.key, int64(m.seq))
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			logrus.Debugf("%s: get revision in list error: %s @ %d: %v", e.name, m.key, m.seq, err)
			return nil, err
		}

		entries = append(entries, valueEntry)
	}

	return entries, nil
}

func (e *KeyValue) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, error) {
	err := e.checkRevision("", revision)
	if err != nil {
		return 0, err
	}

	seekKey, exact := seekKey(prefix, startKey)

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

		if exact && k != seekKey {
			break
		}

		if !strings.HasPrefix(k, prefix) {
			break
		}
		v := it.Value()

		// Get the latest update for the key.
		if revision <= 0 {
			so := v[len(v)-1]
			if so.ex.IsZero() || so.ex.After(now) {
				count++
			}
		} else {
			// Find the latest update below the given revision.
			for i := len(v) - 1; i >= 0; i-- {
				so := v[i]
				if so.seq <= uint64(revision) {
					if so.ex.IsZero() || so.ex.After(now) {
						count++
						break
					}
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

	logrus.Debugf("waitForSequence: waiting for seq=%d, current=%d", seq, e.lastSeq.Load())

	done := make(chan struct{}) // Closed when condition is met
	stop := make(chan struct{}) // Closed when we should abort waiting

	go func() {
		e.seqCond.L.Lock()
		defer e.seqCond.L.Unlock()

		for e.lastSeq.Load() < seq {
			// Check if we should stop waiting before calling Wait()
			select {
			case <-stop:
				return
			default:
			}

			e.seqCond.Wait() // Releases lock, waits for broadcast, reacquires lock
		}
		close(done)
	}()

	// Wait for either completion, timeout, or context cancellation
	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case <-done:
		logrus.Debugf("waitForSequence: seq=%d reached", seq)
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
	// Fast path: check if already ready
	if e.ready.Load() {
		return nil
	}

	timeout := time.NewTimer(5 * time.Minute)
	defer timeout.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			return fmt.Errorf("timeout waiting for btree to be ready")
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if e.ready.Load() {
				logrus.Infof("%s: stream replay complete, ready for operations", e.name)
				return nil
			}
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

	isInitialReplay := br == 0 && targetSeq > 0

	if isInitialReplay {
		logrus.Infof("%s: btree watcher: starting initial replay from 0 to %d", e.name, targetSeq)
	} else {
		logrus.Debugf("%s: btree watcher: resuming at %d (targetSeq=%d)", e.name, br, targetSeq)
		e.ready.Store(true)
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
			if isInitialReplay && !e.ready.Load() && seq >= targetSeq {
				e.ready.Store(true)
				logrus.Infof("%s: btree replay complete at seq=%d (target was %d, took %s)",
					e.name, seq, targetSeq, time.Since(now))
			}
		}
	}
}

// getRevisionOp returns the latest btree operation for the requested revision
func (e *KeyValue) getRevisionOp(key string, revision int64, allowExpired, allowDeleted bool) (*seqOp, error) {
	e.btm.RLock()
	val, ok := e.bt.Get(key)
	if !ok {
		e.btm.RUnlock()
		return nil, jetstream.ErrKeyNotFound
	}
	e.btm.RUnlock()

	op := getSeqOp(val, revision, allowExpired, allowDeleted)

	if op == nil {
		return nil, jetstream.ErrKeyNotFound
	}

	return op, nil
}

// getSeqOp returns the latest sequence operation for the given key and global revision
func getSeqOp(val []*seqOp, revision int64, allowExpired, allowDeleted bool) *seqOp {
	now := time.Now()

	for i := len(val) - 1; i >= 0; i-- {
		op := val[i]
		if revision <= 0 || op.seq <= uint64(revision) {
			if op.op != jetstream.KeyValuePut && !allowDeleted {
				return nil
			}

			if !allowExpired && !op.ex.IsZero() && op.ex.Before(now) {
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

	if startKey != "" {
		return fmt.Sprintf("%s/%s", seekKey, startKey), exact
	}

	return prefix, exact
}
