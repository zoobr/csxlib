package metrics

import (
	"context"
	"net/http"

	"github.com/go-kit/kit/endpoint"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// global metrics instance
var pm = prometheusMetrics{}

// Init initializes Prometheus metrics using namespace & subsystem
func Init(namespace, subsystem string) {
	pm.init(namespace, subsystem)
}

// InitNop initializes unregistered Prometheus metrics. Useful for tests
func InitNop() {
	pm.initNop()
}

// Collect collects Prometheus metrics by executed method
func Collect(name string, method func() error) {
	pm.collect(name, method)
}

// HTTPHandler returns Prometheus HTTP handler.
// See https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/promhttp#Handler for details
func HTTPHandler() http.Handler {
	return promhttp.Handler()
}

// MetricsEndpointMiddleware returns an endpoint middleware which collects Prometheus metrics.
// It is analogue of Collect() used as go-kit middleware
func MetricsEndpointMiddleware(name string) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			pm.collect(name, func() error {
				response, err = next(ctx, request)
				return err
			})

			return response, err
		}
	}
}
