package tracer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

// otelTracer is a struct for tracer & provder instances
type otelTracer struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
}

// initProvider initializes Jaeger provider
func (ot *otelTracer) initProvider(jaegerURL, serviceNamespace, serviceName string) error {
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerURL)))
	if err != nil {
		return err
	}

	ot.provider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNamespaceKey.String(serviceNamespace),
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	return nil
}

// initialize initializes OpenTelemetry tracer
func (ot *otelTracer) initialize(jaegerURL, serviceNamespace, serviceName string) (func(context.Context), error) {
	err := ot.initProvider(jaegerURL, serviceNamespace, serviceName)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(ot.provider)
	ot.tracer = ot.provider.Tracer(serviceName)

	return ot.finish, nil
}

// initNop initializes No-op OpenTelemetry tracer whick doesn't make tracing. Useful for tests
func (ot *otelTracer) initNop() {
	ot.provider = sdktrace.NewTracerProvider()
	ot.tracer = ot.provider.Tracer("")
}

// span creates tracing span, then exec callback & write result to span
func (ot *otelTracer) span(ctx context.Context, name string, cb func(ctx context.Context) error) context.Context {
	var sp trace.Span
	ctx, sp = ot.tracer.Start(ctx, name)
	defer sp.End()

	err := cb(ctx)
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
	} else {
		sp.SetStatus(codes.Ok, "success")
	}

	return ctx
}

// finish is finalizer. It shuts down the span processors in the order they were registered
func (ot *otelTracer) finish(ctx context.Context) {
	err := ot.provider.Shutdown(ctx)
	if err != nil {
		panic(err)
	}
}
