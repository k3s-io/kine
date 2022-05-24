package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type RegistererGatherer interface {
	prometheus.Registerer
	prometheus.Gatherer
}

var Registry RegistererGatherer = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		// expose process metrics like CPU, Memory, file descriptor usage etc.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		// expose Go runtime metrics like GC stats, memory stats etc.
		collectors.NewGoCollector(),
	)
}
