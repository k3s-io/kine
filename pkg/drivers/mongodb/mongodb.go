package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func init() {
	drivers.Register("mongodb", New)
}

const (
	defaultConnectionURI = "mongodb://localhost:27017"
	defaultDatabase      = "kine"
)

// KineReg is the document stored in the kine collection.
// Each document represents one revision event (create, update, or delete) for a key.
type KineReg struct {
	ID             bson.ObjectID `bson:"_id,omitempty"`
	Revision       int64         `bson:"revision"`
	Key            string        `bson:"key"`
	Created        int64         `bson:"created"`
	Deleted        int64         `bson:"deleted"`
	CreateRevision int64         `bson:"createRevision"`
	PrevRevision   int64         `bson:"prevRevision"`
	Lease          int64         `bson:"lease"`
	Value          []byte        `bson:"value,omitempty"`
	PrevValue      []byte        `bson:"prevValue,omitempty"`
	Version        int64         `bson:"version"`
}

// RevisionReg is the single document in the revision collection used as an atomic counter.
type RevisionReg struct {
	ID       bson.ObjectID `bson:"_id,omitempty"`
	Revision int64         `bson:"revision"`
}

// CollStats holds size information for a collection.
type CollStats struct {
	Size int64 `bson:"size"`
}

func (kr *KineReg) toKeyValue(keysOnly bool) *server.KeyValue {
	kv := &server.KeyValue{
		Key:            kr.Key,
		CreateRevision: kr.CreateRevision,
		ModRevision:    kr.Revision,
		Version:        kr.Version,
		Lease:          kr.Lease,
	}
	if !keysOnly {
		kv.Value = kr.Value
	}
	return kv
}

// MongoBackend implements server.Backend using MongoDB.
type MongoBackend struct {
	mu           sync.Mutex
	client       *mongo.Client
	db           *mongo.Database
	coll         *mongo.Collection
	collRevision *mongo.Collection
	// watchMu and watchCh implement a broadcast-on-write mechanism for Watch subscribers.
	// After every append, the current watchCh is closed and replaced with a new one,
	// waking all goroutines waiting on the old channel.
	watchMu sync.RWMutex
	watchCh chan struct{}
}

// New creates a MongoBackend and registers it under the "mongodb" scheme.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	uri := cfg.DataSourceName
	if uri == "" {
		uri = defaultConnectionURI
	}
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		uri = "mongodb://" + uri
	}

	// Extract database name from URI path (e.g. mongodb://host/mydb → mydb).
	dbName := defaultDatabase
	if u, err := url.Parse(uri); err == nil {
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			dbName = db
		}
	}

	clientOpts := options.Client().ApplyURI(uri)

	if tlsCfg, err := cfg.BackendTLSConfig.ClientConfig(); err != nil {
		return false, nil, fmt.Errorf("building TLS config for MongoDB: %w", err)
	} else if tlsCfg != nil {
		clientOpts.SetTLSConfig(tlsCfg)
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return false, nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}

	b := &MongoBackend{
		client:  client,
		watchCh: make(chan struct{}),
	}
	b.db = client.Database(dbName)
	b.coll = b.db.Collection("kine")
	b.collRevision = b.db.Collection("revision")

	// leaderElect = true: multiple kine instances can safely share the same MongoDB.
	return true, b, nil
}

// Start pings MongoDB, creates indexes, and ensures a health-check key exists.
func (b *MongoBackend) Start(ctx context.Context) error {
	if err := b.client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("pinging MongoDB: %w", err)
	}
	if err := setupIndexes(ctx, b.coll); err != nil {
		return fmt.Errorf("setting up MongoDB indexes: %w", err)
	}
	if _, err := b.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if !errors.Is(err, server.ErrKeyExists) {
			logrus.Warnf("mongodb: failed to create health check key: %v", err)
		}
	}
	logrus.Infof("MongoDB backend started (db=%s)", b.db.Name())
	return nil
}

// CurrentRevision returns the current global revision.
func (b *MongoBackend) CurrentRevision(ctx context.Context) (int64, error) {
	var rev RevisionReg
	err := b.collRevision.FindOne(ctx, bson.M{}).Decode(&rev)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return rev.Revision, nil
}

// nextRevision atomically increments the global revision counter and returns the new value.
func (b *MongoBackend) nextRevision(ctx context.Context) (int64, error) {
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)
	var rev RevisionReg
	err := b.collRevision.FindOneAndUpdate(
		ctx,
		bson.M{},
		bson.M{"$inc": bson.M{"revision": int64(1)}},
		opts,
	).Decode(&rev)
	if err != nil {
		return 0, err
	}
	return rev.Revision, nil
}

// Get returns the most recent non-deleted value for key at or before revision.
// If revision is 0, the latest is returned. rangeEnd and limit are unused for single-key gets.
func (b *MongoBackend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	filter := bson.M{"key": key, "deleted": int64(0)}
	if revision > 0 {
		filter["revision"] = bson.M{"$lte": revision}
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "revision", Value: -1}})
	if keysOnly {
		opts.SetProjection(bson.M{"value": 0, "prevValue": 0})
	}

	var doc KineReg
	err := b.coll.FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		curRev, _ := b.CurrentRevision(ctx)
		return curRev, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}

	curRev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if revision > 0 {
		return revision, doc.toKeyValue(keysOnly), nil
	}
	return curRev, doc.toKeyValue(keysOnly), nil
}

// Create inserts a new key. Returns ErrKeyExists if the key already exists and is not deleted.
func (b *MongoBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	existing, err := b.findLatestForKey(ctx, key, 0, true)
	if err != nil {
		return 0, err
	}
	if existing != nil && existing.Deleted == 0 {
		curRev, _ := b.CurrentRevision(ctx)
		return curRev, server.ErrKeyExists
	}

	prevKV := &server.KeyValue{}
	if existing != nil {
		prevKV = existing.toKeyValue(false)
	}

	return b.appendEvent(ctx, &server.Event{
		Create: true,
		KV: &server.KeyValue{
			Key:     key,
			Value:   value,
			Lease:   lease,
			Version: 1,
		},
		PrevKV: prevKV,
	})
}

// Update modifies the value of key at the exact given revision.
// Returns (rev, kv, false, nil) when the key has already been modified past revision.
func (b *MongoBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	existing, err := b.findLatestForKey(ctx, key, revision, false)
	if err != nil {
		return 0, nil, false, err
	}
	if existing == nil {
		return 0, nil, false, nil
	}
	if existing.Revision != revision {
		return existing.Revision, existing.toKeyValue(false), false, nil
	}

	newKV := &server.KeyValue{
		Key:            key,
		Value:          value,
		Lease:          lease,
		Version:        existing.Version + 1,
		CreateRevision: existing.CreateRevision,
	}
	rev, err := b.appendEvent(ctx, &server.Event{
		KV:     newKV,
		PrevKV: existing.toKeyValue(false),
	})
	if err != nil {
		return 0, nil, false, err
	}
	newKV.ModRevision = rev
	return rev, newKV, true, nil
}

// Delete marks key as deleted at the given revision.
// Returns (rev, kv, false, nil) when the key has been modified past revision.
func (b *MongoBackend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	existing, err := b.findLatestForKey(ctx, key, 0, true)
	if err != nil {
		return 0, nil, false, err
	}
	if existing == nil {
		return 0, nil, true, nil
	}
	if existing.Deleted == 1 {
		return existing.Revision, existing.toKeyValue(false), true, nil
	}
	if revision != 0 && existing.Revision != revision {
		return existing.Revision, existing.toKeyValue(false), false, nil
	}

	kv := existing.toKeyValue(false)
	rev, err := b.appendEvent(ctx, &server.Event{
		Delete: true,
		KV:     kv,
		PrevKV: kv,
	})
	if err != nil {
		return 0, nil, false, err
	}
	return rev, kv, true, nil
}

// List returns the latest non-deleted version of all keys with the given prefix,
// at or before revision. startKey acts as a lower bound for key range queries.
func (b *MongoBackend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	curRev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	returnRev := curRev

	matchFilter := buildPrefixFilter(prefix, startKey)
	if revision > 0 {
		matchFilter["revision"] = bson.M{"$lte": revision}
		returnRev = revision
	}

	// Aggregate to get the latest revision per key (equivalent to DISTINCT ON in SQL).
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchFilter}},
		{{Key: "$sort", Value: bson.D{{Key: "key", Value: 1}, {Key: "revision", Value: -1}}}},
		{{Key: "$group", Value: bson.M{
			"_id": "$key",
			"doc": bson.M{"$first": "$$ROOT"},
		}}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$doc"}}},
		{{Key: "$match", Value: bson.M{"deleted": int64(0)}}},
		{{Key: "$sort", Value: bson.D{{Key: "key", Value: 1}}}},
	}
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
	}
	if keysOnly {
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.M{"value": 0, "prevValue": 0}}})
	}

	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var result []*server.KeyValue
	for cursor.Next(ctx) {
		var doc KineReg
		if err := cursor.Decode(&doc); err != nil {
			return 0, nil, err
		}
		result = append(result, doc.toKeyValue(keysOnly))
	}
	return returnRev, result, cursor.Err()
}

// Count returns the current revision and the number of live keys matching the given prefix,
// at or before revision. startKey acts as a lower bound for the key range.
func (b *MongoBackend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	matchFilter := buildPrefixFilter(prefix, startKey)
	if revision > 0 {
		matchFilter["revision"] = bson.M{"$lte": revision}
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchFilter}},
		{{Key: "$sort", Value: bson.D{{Key: "revision", Value: -1}}}},
		{{Key: "$group", Value: bson.M{
			"_id":     "$key",
			"deleted": bson.M{"$first": "$deleted"},
		}}},
		{{Key: "$match", Value: bson.M{"deleted": int64(0)}}},
		{{Key: "$count", Value: "total"}},
	}

	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(ctx)

	var countResult struct {
		Total int64 `bson:"total"`
	}
	if cursor.Next(ctx) {
		if err := cursor.Decode(&countResult); err != nil {
			return 0, 0, err
		}
	}

	curRev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	return curRev, countResult.Total, nil
}

// Watch returns a WatchResult that emits Events for keys matching the key prefix
// with revision greater than the given revision.
// Uses a polling approach: a goroutine waits for a broadcast signal from appendEvent,
// then queries for new documents since the last known revision.
func (b *MongoBackend) Watch(ctx context.Context, key string, revision int64) server.WatchResult {
	curRev, _ := b.CurrentRevision(ctx)

	eventC := make(chan []*server.Event, 100)
	errC := make(chan error, 1)

	go func() {
		defer close(eventC)
		lastRev := revision
		if lastRev == 0 {
			lastRev = curRev
		}

		for {
			// Capture current notification channel before waiting, so we never miss
			// a signal that arrives between the fetch and the next Wait call.
			b.watchMu.RLock()
			notifyCh := b.watchCh
			b.watchMu.RUnlock()

			select {
			case <-ctx.Done():
				return
			case <-notifyCh:
			}

			events, err := b.fetchEventsSince(ctx, key, lastRev)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errC <- err
				}
				return
			}
			if len(events) > 0 {
				lastRev = events[len(events)-1].KV.ModRevision
				select {
				case eventC <- events:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return server.WatchResult{
		CurrentRevision: curRev,
		Events:          eventC,
		Errorc:          errC,
	}
}

// DbSize returns the storage size of the kine collection in bytes.
func (b *MongoBackend) DbSize(ctx context.Context) (int64, error) {
	result := b.db.RunCommand(ctx, bson.M{"collStats": b.coll.Name()})
	var stats CollStats
	if err := result.Decode(&stats); err != nil {
		return 0, err
	}
	return stats.Size, nil
}

// Compact deletes superseded and tombstone documents with revision <= the given revision.
// It mirrors the SQL compaction logic:
//  1. Collect all prevRevision values referenced by documents at or before compactRevision
//     (these are versions superseded by a later update).
//  2. Delete all documents whose revision appears in that set.
//  3. Delete all tombstone documents (deleted=1) at or before compactRevision.
func (b *MongoBackend) Compact(ctx context.Context, revision int64) (int64, error) {
	// Step 1: find all prevRevision values that have been superseded.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"revision":     bson.M{"$lte": revision},
			"prevRevision": bson.M{"$gt": int64(0)},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":           nil,
			"prevRevisions": bson.M{"$addToSet": "$prevRevision"},
		}}},
	}
	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var agg struct {
		PrevRevisions []int64 `bson:"prevRevisions"`
	}
	if cursor.Next(ctx) {
		if err := cursor.Decode(&agg); err != nil {
			return 0, err
		}
	}
	cursor.Close(ctx)

	var deleted int64

	// Step 2: delete superseded revisions.
	if len(agg.PrevRevisions) > 0 {
		res, err := b.coll.DeleteMany(ctx, bson.M{"revision": bson.M{"$in": agg.PrevRevisions}})
		if err != nil {
			return deleted, err
		}
		deleted += res.DeletedCount
	}

	// Step 3: delete tombstone documents at or before the compact revision.
	res, err := b.coll.DeleteMany(ctx, bson.M{
		"revision": bson.M{"$lte": revision},
		"deleted":  bson.M{"$gt": int64(0)},
	})
	if err != nil {
		return deleted, err
	}
	deleted += res.DeletedCount

	return deleted, nil
}

// WaitForSyncTo blocks until the current global revision reaches the target.
func (b *MongoBackend) WaitForSyncTo(revision int64) {
	for {
		curRev, err := b.CurrentRevision(context.Background())
		if err == nil && curRev >= revision {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// appendEvent inserts a new event document and broadcasts to all active Watch goroutines.
func (b *MongoBackend) appendEvent(ctx context.Context, event *server.Event) (int64, error) {
	if event.KV == nil {
		event.KV = &server.KeyValue{}
	}
	if event.PrevKV == nil {
		event.PrevKV = &server.KeyValue{}
	}

	nextRev, err := b.nextRevision(ctx)
	if err != nil {
		return 0, err
	}

	doc := KineReg{
		Revision:       nextRev,
		Key:            event.KV.Key,
		Lease:          event.KV.Lease,
		Value:          event.KV.Value,
		PrevValue:      event.PrevKV.Value,
		PrevRevision:   event.PrevKV.ModRevision,
		Version:        event.KV.Version,
		CreateRevision: event.KV.CreateRevision,
	}
	if event.Create {
		doc.Created = 1
		doc.CreateRevision = nextRev
	}
	if event.Delete {
		doc.Deleted = 1
	}

	if _, err := b.coll.InsertOne(ctx, doc); err != nil {
		return 0, err
	}

	event.KV.ModRevision = nextRev
	b.notifyWatchers()
	return nextRev, nil
}

// notifyWatchers closes the current watchCh and replaces it with a new one,
// waking all goroutines that are select-waiting on the old channel.
func (b *MongoBackend) notifyWatchers() {
	b.watchMu.Lock()
	old := b.watchCh
	b.watchCh = make(chan struct{})
	b.watchMu.Unlock()
	close(old)
}

// fetchEventsSince returns all events for keys matching key (as prefix) with
// revision greater than lastRev, ordered by ascending revision.
func (b *MongoBackend) fetchEventsSince(ctx context.Context, key string, lastRev int64) ([]*server.Event, error) {
	filter := bson.M{"revision": bson.M{"$gt": lastRev}}
	if key != "" {
		filter["key"] = bson.M{"$regex": "^" + regexEscape(key)}
	}

	opts := options.Find().SetSort(bson.D{{Key: "revision", Value: 1}})
	cursor, err := b.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var events []*server.Event
	for cursor.Next(ctx) {
		var doc KineReg
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		event := &server.Event{
			Create: doc.Created == 1,
			Delete: doc.Deleted == 1,
			KV:     doc.toKeyValue(false),
		}
		if doc.PrevRevision > 0 {
			event.PrevKV = &server.KeyValue{
				Key:         doc.Key,
				Value:       doc.PrevValue,
				ModRevision: doc.PrevRevision,
			}
		}
		events = append(events, event)
	}
	return events, cursor.Err()
}

// findLatestForKey returns the most recent document for key with revision <= maxRevision.
// If maxRevision is 0, no upper bound is applied.
// If includeDeleted is false, deleted documents are excluded.
func (b *MongoBackend) findLatestForKey(ctx context.Context, key string, maxRevision int64, includeDeleted bool) (*KineReg, error) {
	filter := bson.M{"key": key}
	if maxRevision > 0 {
		filter["revision"] = bson.M{"$lte": maxRevision}
	}
	if !includeDeleted {
		filter["deleted"] = int64(0)
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "revision", Value: -1}})
	var doc KineReg
	err := b.coll.FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// setupIndexes creates the required indexes on the kine collection.
func setupIndexes(ctx context.Context, coll *mongo.Collection) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "key", Value: 1}},
			Options: options.Index().SetName("idx_key"),
		},
		{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("idx_key_id"),
		},
		{
			Keys:    bson.D{{Key: "_id", Value: 1}, {Key: "deleted", Value: 1}},
			Options: options.Index().SetName("idx_id_deleted"),
		},
		{
			Keys:    bson.D{{Key: "prevRevision", Value: 1}},
			Options: options.Index().SetName("idx_prev_revision"),
		},
		{
			Keys:    bson.D{{Key: "revision", Value: 1}},
			Options: options.Index().SetName("idx_revision_unique").SetUnique(true),
		},
	}
	_, err := coll.Indexes().CreateMany(ctx, models)
	return err
}

// buildPrefixFilter constructs a bson.M that matches all keys with the given prefix
// and with key >= startKey (for range queries / pagination).
func buildPrefixFilter(prefix, startKey string) bson.M {
	filter := bson.M{}
	keyFilter := bson.M{"$regex": "^" + regexEscape(prefix)}
	if startKey != "" {
		keyFilter["$gte"] = startKey
	}
	filter["key"] = keyFilter
	return filter
}

// regexEscape escapes special regex metacharacters in a string.
func regexEscape(s string) string {
	return regexp.QuoteMeta(s)
}
