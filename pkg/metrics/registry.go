package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// DefaultRegisterer and DefaultGatherer are the implementations of the
	// prometheus Registerer and Gatherer interfaces that all metrics operations
	// will use. They are variables so that packages that embed this library can
	// replace them at runtime, instead of having to pass around specific
	// registries.
	defaultRegistry                         = prometheus.NewRegistry()
	DefaultRegisterer prometheus.Registerer = defaultRegistry
	DefaultGatherer   prometheus.Gatherer   = defaultRegistry
)

func RegisterAll() {
	DefaultRegisterer.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	DefaultRegisterer.MustRegister(prometheus.NewGoCollector())
	Register()
}

func Register() {
	DefaultRegisterer.MustRegister(
		// kine sql metrics
		SQLTotal,
		SQLTime,
		CompactTotal,
		// etcd mvcc metrics
		RangeCounter,
		PutCounter,
		DeleteCounter,
		TxnCounter,
		KeysGauge,
		WatchStreamGauge,
		WatcherGauge,
		SlowWatcherGauge,
		TotalEventsCounter,
		DbCompactionTotalMs,
		DbCompactionLast,
		DbCompactionKeysCounter,
		DbTotalSize,
		DbTotalSizeInUse,
		CurrentRev,
		CompactRev,
		TotalPutSizeGauge,
		// etcd server metrics
		HasLeader,
		IsLeader,
		IsLearner,
		CurrentVersion,
		CurrentGoVersion,
		ServerID,
	)
}
