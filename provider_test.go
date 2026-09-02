package observesdk_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"goark.dev/observe"
	observesdk "goark.dev/observe-sdk"
)

func TestProviderExportsAllSignalsAndShutsDownOnce(t *testing.T) {
	t.Parallel()
	exporter := &captureExporter{}
	provider, err := observesdk.NewProvider(
		observesdk.WithResource(observe.NewResource("test-service")),
		observesdk.WithExporters(exporter),
	)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	ctx, span := provider.Tracer("test").Start(t.Context(), "request")
	provider.Logger("test").Log(ctx, observe.LevelInfo, "handled")
	provider.Eventer("test").Emit(ctx, "ready")
	counter, err := provider.Meter("test").Int64Counter("requests", observe.WithAttributeKeys("method"))
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	counter.Add(ctx, 1, observe.String("method", "GET"))
	span.End()

	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if err := provider.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := provider.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}

	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.spans) != 1 || len(exporter.logs) != 1 || len(exporter.events) != 1 || len(exporter.metrics) == 0 {
		t.Fatalf("export counts = spans:%d logs:%d events:%d metrics:%d", len(exporter.spans), len(exporter.logs), len(exporter.events), len(exporter.metrics))
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("shutdown calls = %d, want 1", exporter.shutdowns.Load())
	}
	if exporter.logs[0].SpanContext.TraceID != exporter.spans[0].SpanContext.TraceID {
		t.Fatal("log and span trace IDs differ")
	}
}

func TestProviderSupportsConcurrentSpanEnd(t *testing.T) {
	t.Parallel()
	exporter := &captureExporter{}
	provider, err := observesdk.NewProvider(observesdk.WithExporters(exporter))
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	_, span := provider.Tracer("test").Start(t.Context(), "concurrent")
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() { defer group.Done(); span.SetAttrs(observe.Int("worker", 1)); span.End() }()
	}
	group.Wait()
	exporter.mu.Lock()
	count := len(exporter.spans)
	exporter.mu.Unlock()
	if count != 1 {
		t.Fatalf("exported spans = %d, want 1", count)
	}
}

type captureExporter struct {
	mu        sync.Mutex
	spans     []observe.SpanSnapshot
	metrics   []observe.MetricData
	logs      []observe.LogRecord
	events    []observe.EventRecord
	shutdowns atomic.Int32
}

func (*captureExporter) Descriptor() observe.ExporterDescriptor {
	return observe.ExporterDescriptor{Name: "capture", Signals: observe.SignalAll, Stability: observe.StabilityExperimental, Capabilities: observe.ExporterCapabilities{Push: true, CumulativeTemporality: true, Histogram: true}}
}
func (*captureExporter) ForceFlush(context.Context) error { return nil }
func (e *captureExporter) Shutdown(context.Context) error { e.shutdowns.Add(1); return nil }
func (e *captureExporter) ExportSpans(_ context.Context, v []observe.SpanSnapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, item := range v {
		e.spans = append(e.spans, item.Clone())
	}
	return nil
}
func (e *captureExporter) ExportMetrics(_ context.Context, v []observe.MetricData) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, item := range v {
		e.metrics = append(e.metrics, item.Clone())
	}
	return nil
}
func (e *captureExporter) ExportLogs(_ context.Context, v []observe.LogRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, item := range v {
		e.logs = append(e.logs, item.Clone())
	}
	return nil
}
func (e *captureExporter) ExportEvents(_ context.Context, v []observe.EventRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, item := range v {
		e.events = append(e.events, item.Clone())
	}
	return nil
}
