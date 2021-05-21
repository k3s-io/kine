package generic

import (
	"time"

	"github.com/sirupsen/logrus"
)

const (
	slowSQLThreshold = time.Second
)

type Trace struct {
	startTime time.Time
}

func NewTrace() *Trace {
	return &Trace{startTime: time.Now()}
}

func (t *Trace) LogSlowSQL(sql Stripped, args ...interface{}) {
	totalTime := time.Since(t.startTime)
	if totalTime >= slowSQLThreshold {
		logrus.Infof("Slow SQL (started: %v) (total time: %v): %s : %v", t.startTime, totalTime, sql, args)
	}
}
