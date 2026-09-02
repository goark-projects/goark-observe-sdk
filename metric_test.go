package observesdk_test

import (
	"context"
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
