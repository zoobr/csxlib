package tracer

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"go.opentelemetry.io/contrib/instrumentation/github.com/go-kit/kit/otelkit"
	"go.opentelemetry.io/otel/trace"
)

var ot = otelTracer{} // global tracer instance

// Init initializes tracer
func Init(jaegerURL, serviceNamespace, serviceName string) (func(context.Context), error) {
	return ot.initialize(jaegerURL, serviceNamespace, serviceName)
}

// InitNop initializes No-op tracer whick doesn't make tracing. Useful for tests
func InitNop() {
	ot.initNop()
}

// Span creates tracing span, then exec callback & write result to span
func Span(ctx context.Context, name string, cb func(ctx context.Context) error) context.Context {
	return ot.span(ctx, name, cb)
}

// SpatContext returns span and context
func SpanContext(ctx context.Context, name string) (context.Context, trace.Span) {
	return ot.tracer.Start(ctx, name)
}

// TracerEndpointMiddleware returns tracing midleware
func TracerEndpointMiddleware(name string) endpoint.Middleware {
	epName := "endpoint." + name
	return otelkit.EndpointMiddleware(otelkit.WithOperation(epName))
}
