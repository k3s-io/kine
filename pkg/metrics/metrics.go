package metrics

import (
	"time"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	ResultSuccess = "success"
	ResultError   = "error"
)

var (
	SQLTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_sql_total",
		Help: "Total number of SQL operations",
	}, []string{"name", "error_code"})

	SQLTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kine_sql_time_seconds",
		Help: "Length of time per SQL operation",
		// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
		// highest bucket start of 0.001 sec * 2^13 == 8.192 sec
		// keep consistent with etcd backend latencies in server/storage/backend/metrics.go
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"name", "error_code"})

	CompactTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_compact_total",
		Help: "Total number of compactions",
	}, []string{"result"})

	InsertErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_insert_errors_total",
		Help: "Total number of insert retries due to unique constraint violations",
	}, []string{"retriable"})
)

var (
	// SlowSQLThreshold is a duration which SQL executed longer than will be logged.
	// This can be directly modified to override the default value when kine is used as a library.
	SlowSQLThreshold        = time.Second
	SlowSQLWarningThreshold = 5 * time.Second
)

func ObserveSQL(start time.Time, errCode string, retries int, sql *query.Filled) {
	if sql.Name != "" {
		SQLTotal.WithLabelValues(sql.Name, errCode).Inc()
		duration := time.Since(start)
		SQLTime.WithLabelValues(sql.Name, errCode).Observe(duration.Seconds())
		if SlowSQLThreshold > 0 && duration >= SlowSQLThreshold {
			instrumentedLogger := logrus.WithFields(logrus.Fields{
				"name":     sql.Name,
				"duration": duration.String(),
				"started":  start.Format(time.RFC3339Nano),
			})
			if retries > 0 {
				instrumentedLogger = instrumentedLogger.WithField("retries", retries)
			}
			if duration < SlowSQLWarningThreshold {
				instrumentedLogger.Infof("Slow SQL: %s", sql.QueryString())
			} else {
				instrumentedLogger.Warnf("Slow SQL: %s", sql.QueryString())
			}
		}
	}
}
