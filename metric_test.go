package observesdk_test

import (
	"context"
	"errors"
	"testing"

	"goark.dev/observe"
	observesdk "goark.dev/observe-sdk"
)

func TestMetricCardinalityIsBounded(t *testing.T) {
	t.Parallel()
	exporter := &captureExporter{}
	provider, err := observesdk.NewProvider(observesdk.WithExporters(exporter), observesdk.WithMetricCardinalityLimit(2))
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	counter, err := provider.Meter("test").Int64Counter("requests", observe.WithAttributeKeys("route"))
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	for _, route := range []string{"/a", "/b", "/c"} {
		counter.Add(t.Context(), 1, observe.String("route", route))
	}
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.metrics) != 1 {
		t.Fatalf("metrics = %d, want 1", len(exporter.metrics))
	}
	sum, ok := exporter.metrics[0].Aggregation.(observe.SumData)
	if !ok {
		t.Fatalf("aggregation = %T, want SumData", exporter.metrics[0].Aggregation)
	}
	if len(sum.Points) != 2 {
		t.Fatalf("series = %d, want 2", len(sum.Points))
	}
}

func TestFailedMetricExportRemainsPending(t *testing.T) {
	t.Parallel()
	exporter := &retryMetricExporter{captureExporter: captureExporter{}, fail: true}
	provider, _ := observesdk.NewProvider(observesdk.WithExporters(exporter))
	counter, _ := provider.Meter("test").Int64Counter("requests")
	counter.Add(t.Context(), 1)
	if err := provider.ForceFlush(t.Context()); err == nil {
		t.Fatal("first ForceFlush must fail")
	}
	exporter.fail = false
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("retry ForceFlush: %v", err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.metrics) != 2 {
		t.Fatalf("export attempts = %d, want 2", len(exporter.metrics))
	}
}

type retryMetricExporter struct {
	captureExporter
	fail bool
}

func (e *retryMetricExporter) ExportMetrics(ctx context.Context, values []observe.MetricData) error {
	_ = e.captureExporter.ExportMetrics(ctx, values)
	if e.fail {
		return errors.New("export failed")
	}
	return nil
}

func TestObservableGaugeIsCollected(t *testing.T) {
	t.Parallel()
	exporter := &captureExporter{}
	provider, err := observesdk.NewProvider(observesdk.WithExporters(exporter))
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	meter := provider.Meter("test")
	gauge, err := meter.Int64ObservableGauge("workers")
	if err != nil {
		t.Fatalf("Int64ObservableGauge: %v", err)
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, observer observe.CallbackObserver) error {
		observer.ObserveInt64(gauge, 4)
		return nil
	}, gauge)
	if err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}
	defer registration.Unregister()
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.metrics) != 1 {
		t.Fatalf("metrics = %d, want 1", len(exporter.metrics))
	}
}

func TestUnchangedSynchronousMetricsAreNotExportedTwice(t *testing.T) {
	t.Parallel()
	exporter := &captureExporter{}
	provider, _ := observesdk.NewProvider(observesdk.WithExporters(exporter))
	counter, _ := provider.Meter("test").Int64Counter("requests")
	counter.Add(t.Context(), 1)
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("first ForceFlush: %v", err)
	}
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("second ForceFlush: %v", err)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.metrics) != 1 {
		t.Fatalf("metrics = %d, want 1", len(exporter.metrics))
	}
}
