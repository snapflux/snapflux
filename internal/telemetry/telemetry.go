package telemetry

import (
	"context"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup initializes the global OTEL metric and trace providers.
// Metrics are exported via the returned Prometheus HTTP handler at /metrics.
// Traces are exported via OTLP HTTP if OTEL_EXPORTER_OTLP_ENDPOINT is set.
func Setup(ctx context.Context) (shutdown func(context.Context) error, metricsHandler http.Handler, err error) {
	registry := prometheus.NewRegistry()
	promExp, err := promexporter.New(promexporter.WithRegisterer(registry))
	if err != nil {
		return nil, nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(promExp))
	otel.SetMeterProvider(mp)

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, expErr := otlptracehttp.New(ctx)
		if expErr != nil {
			_ = mp.Shutdown(ctx)
			return nil, nil, expErr
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(shutCtx context.Context) error {
		_ = mp.Shutdown(shutCtx)
		return tp.Shutdown(shutCtx)
	}, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), nil
}
