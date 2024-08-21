package metrics

import (
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	ResultSuccess = "success"
	ResultError   = "error"
)

// kine sql and compaction metrics
var (
	SQLTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kine",
		Subsystem: "sql",
		Name:      "total",
		Help:      "Total number of SQL operations",
	}, []string{"error_code"})

	SQLTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kine",
		Subsystem: "sql",
		Name:      "time_seconds",
		Help:      "Length of time per SQL operation",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30},
	}, []string{"error_code"})

	CompactTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kine",
		Subsystem: "compact",
		Name:      "total",
		Help:      "Total number of compactions",
	}, []string{"result"})
)

// mvcc metrics cribbed from https://github.com/etcd-io/etcd/blob/v3.5.11/server/mvcc/metrics.go
var (
	RangeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "mvcc",
			Name:      "range_total",
			Help:      "Total number of ranges seen by this member.",
		})

	PutCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "mvcc",
			Name:      "put_total",
			Help:      "Total number of puts seen by this member.",
		})

	DeleteCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "mvcc",
			Name:      "delete_total",
			Help:      "Total number of deletes seen by this member.",
		})

	TxnCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "mvcc",
			Name:      "txn_total",
			Help:      "Total number of txns seen by this member.",
		})

	KeysGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "keys_total",
			Help:      "Total number of keys.",
		})

	WatchStreamGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "watch_stream_total",
			Help:      "Total number of watch streams.",
		})

	WatcherGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "watcher_total",
			Help:      "Total number of watchers.",
		})

	SlowWatcherGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "slow_watcher_total",
			Help:      "Total number of unsynced slow watchers.",
		})

	TotalEventsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "events_total",
			Help:      "Total number of events sent by this member.",
		})

	DbCompactionTotalMs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "db_compaction_total_duration_milliseconds",
			Help:      "Bucketed histogram of db compaction total duration.",

			// lowest bucket start of upper bound 100 ms with factor 2
			// highest bucket start of 100 ms * 2^13 == 8.192 sec
			Buckets: prometheus.ExponentialBuckets(100, 2, 14),
		})

	DbCompactionLast = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "db_compaction_last",
			Help:      "The unix time of the last db compaction. Resets to 0 on start.",
		})

	DbCompactionKeysCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "db_compaction_keys_total",
			Help:      "Total number of db keys compacted.",
		})

	DbTotalSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "mvcc",
		Name:      "db_total_size_in_bytes",
		Help:      "Total size of the underlying database physically allocated in bytes.",
	},
		func() float64 {
			ReportDbTotalSizeInBytesMu.RLock()
			defer ReportDbTotalSizeInBytesMu.RUnlock()
			return ReportDbTotalSizeInBytes()
		},
	)
	// overridden by mvcc initialization
	ReportDbTotalSizeInBytesMu sync.RWMutex
	ReportDbTotalSizeInBytes   = func() float64 { return 0 }

	DbTotalSizeInUse = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "mvcc",
		Name:      "db_total_size_in_use_in_bytes",
		Help:      "Total size of the underlying database logically in use in bytes.",
	},
		func() float64 {
			ReportDbTotalSizeInUseInBytesMu.RLock()
			defer ReportDbTotalSizeInUseInBytesMu.RUnlock()
			return ReportDbTotalSizeInUseInBytes()
		},
	)
	// overridden by mvcc initialization
	ReportDbTotalSizeInUseInBytesMu sync.RWMutex
	ReportDbTotalSizeInUseInBytes   = func() float64 { return 0 }

	CurrentRev = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "etcd_debugging",
		Subsystem: "mvcc",
		Name:      "current_revision",
		Help:      "The current revision of store.",
	},
		func() float64 {
			ReportCurrentRevMu.RLock()
			defer ReportCurrentRevMu.RUnlock()
			return ReportCurrentRev()
		},
	)
	// overridden by mvcc initialization
	ReportCurrentRevMu sync.RWMutex
	ReportCurrentRev   = func() float64 { return 0 }

	CompactRev = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "etcd_debugging",
		Subsystem: "mvcc",
		Name:      "compact_revision",
		Help:      "The revision of the last compaction in store.",
	},
		func() float64 {
			ReportCompactRevMu.RLock()
			defer ReportCompactRevMu.RUnlock()
			return ReportCompactRev()
		},
	)
	// overridden by mvcc initialization
	ReportCompactRevMu sync.RWMutex
	ReportCompactRev   = func() float64 { return 0 }

	TotalPutSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "etcd_debugging",
			Subsystem: "mvcc",
			Name:      "total_put_size_in_bytes",
			Help:      "The total size of put kv pairs seen by this member.",
		})
)

// server metrics cribbed from https://github.com/etcd-io/etcd/blob/v3.5.11/server/etcdserver/metrics.go
var (
	HasLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "has_leader",
		Help:      "Whether or not a leader exists. 1 is existence, 0 is not.",
	})

	IsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "is_leader",
		Help:      "Whether or not this member is a leader. 1 if is, 0 otherwise.",
	})

	IsLearner = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "is_learner",
		Help:      "Whether or not this member is a learner. 1 if is, 0 otherwise.",
	})

	CurrentVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version",
		Help:      "Which version is running. 1 for 'server_version' label with current version.",
	},
		[]string{"server_version"})

	CurrentGoVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "go_version",
		Help:      "Which Go version server is running with. 1 for 'server_go_version' label with current version.",
	},
		[]string{"server_go_version"})

	ServerID = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "id",
		Help:      "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.",
	},
		[]string{"server_id"})
)

var (
	// SlowSQLThreshold is a duration which SQL executed longer than will be logged.
	// This can be directly modified to override the default value when kine is used as a library.
	SlowSQLThreshold = time.Second
)

func ObserveSQL(start time.Time, errCode string, sql util.Stripped, args ...interface{}) {
	SQLTotal.WithLabelValues(errCode).Inc()
	duration := time.Since(start)
	SQLTime.WithLabelValues(errCode).Observe(duration.Seconds())
	if SlowSQLThreshold > 0 && duration >= SlowSQLThreshold {
		if logrus.GetLevel() == logrus.TraceLevel {
			logrus.Tracef("Slow SQL (started: %v) (total time: %v): %s : %v", start, duration, sql, args)
		} else {
			logrus.Infof("Slow SQL (started: %v) (total time: %v): %s", start, duration, sql)
		}
	}
}
