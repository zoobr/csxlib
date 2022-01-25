package metrics

import (
	"time"

	kitmetrics "github.com/go-kit/kit/metrics"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

// prometheusMetrics is a struct for Prometheus metrics
type prometheusMetrics struct {
	reqCountMetric    kitmetrics.Counter   // requests count metric
	reqDurationMetric kitmetrics.Histogram // requests duration metric
}

// common labels for metrics: method - method name, res - result of method execution (success/error)
var labelNames = []string{"method", "res"}

// getMetricLabelValues returns array of label names & values for metrics
func getMetricLabelValues(methodName string, err error) []string {
	res := "success"
	if err != nil {
		res = "error"
	}
	return []string{"method", methodName, "res", res}
}

// init initializes Prometheus metrics using namespace & subsystem
func (pm *prometheusMetrics) init(namespace, subsystem string) {
	pm.reqCountMetric = kitprometheus.NewCounterFrom(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_count",
		Help:      "Count of requests",
	}, labelNames)

	pm.reqDurationMetric = kitprometheus.NewSummaryFrom(prometheus.SummaryOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "request_duration_ms",
		Help:      "Requests execution time in milliseconds",
	}, labelNames)
}

// initNop initializes unregistered Prometheus metrics. Useful for tests
func (pm *prometheusMetrics) initNop() {
	pm.reqCountMetric = kitprometheus.NewCounter(prometheus.NewCounterVec(prometheus.CounterOpts{}, labelNames))
	pm.reqDurationMetric = kitprometheus.NewSummary(prometheus.NewSummaryVec(prometheus.SummaryOpts{}, labelNames))
}

// collect collects Prometheus metrics by executed method
func (pm *prometheusMetrics) collect(name string, method func() error) {
	var err error
	defer func(begin time.Time) {
		lvs := getMetricLabelValues(name, err)
		pm.reqCountMetric.With(lvs...).Add(1)
		pm.reqDurationMetric.With(lvs...).Observe(float64(time.Since(begin).Milliseconds()))
	}(time.Now())

	err = method()
}
