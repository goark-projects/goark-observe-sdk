package processor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goark.dev/observe"
	"goark.dev/observe-sdk/processor"
)

func TestBatchSpanProcessorFlushesAndShutsDown(t *testing.T) {
	t.Parallel()
	exporter := &spanExporter{}
	batch, err := processor.NewBatchSpanProcessor(exporter, processor.WithQueueSize(8), processor.WithBatchSize(4), processor.WithScheduleDelay(time.Hour))
	if err != nil {
		t.Fatalf("NewBatchSpanProcessor: %v", err)
	}
	for index := range 3 {
		batch.OnEnd(t.Context(), observe.SpanSnapshot{Name: string(rune('a' + index))})
	}
	if err := batch.ForceFlush(t.Context()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if exporter.count() != 3 {
		t.Fatalf("exported spans = %d, want 3", exporter.count())
	}
	if err := batch.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := batch.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("shutdowns = %d, want 1", exporter.shutdowns.Load())
	}
}

func TestBatchSpanProcessorReportsQueueOverflow(t *testing.T) {
	t.Parallel()
	exporter := &spanExporter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	errorsSeen := make(chan error, 1)
	batch, err := processor.NewBatchSpanProcessor(exporter, processor.WithQueueSize(1), processor.WithBatchSize(1), processor.WithScheduleDelay(time.Hour), processor.WithErrorHandler(observe.ErrorHandlerFunc(func(_ context.Context, err error) {
		select {
		case errorsSeen <- err:
		default:
		}
	})))
	if err != nil {
		t.Fatalf("NewBatchSpanProcessor: %v", err)
	}
	batch.OnEnd(t.Context(), observe.SpanSnapshot{Name: "first"})
	select {
	case <-exporter.entered:
	case <-time.After(time.Second):
		t.Fatal("exporter was not entered")
	}
	batch.OnEnd(t.Context(), observe.SpanSnapshot{Name: "second"})
	batch.OnEnd(t.Context(), observe.SpanSnapshot{Name: "overflow"})
	select {
	case err := <-errorsSeen:
		if !errors.Is(err, processor.ErrQueueFull) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queue overflow was not reported")
	}
	close(exporter.release)
	if err := batch.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

type spanExporter struct {
	mu        sync.Mutex
	spans     []observe.SpanSnapshot
	shutdowns atomic.Int32
	entered   chan struct{}
	release   chan struct{}
}

func (*spanExporter) Descriptor() observe.ExporterDescriptor {
	return observe.ExporterDescriptor{Name: "batch-test", Signals: observe.SignalTraces, Stability: observe.StabilityExperimental, Capabilities: observe.ExporterCapabilities{Push: true}}
}
func (*spanExporter) ForceFlush(context.Context) error { return nil }
func (e *spanExporter) Shutdown(context.Context) error { e.shutdowns.Add(1); return nil }
func (e *spanExporter) ExportSpans(ctx context.Context, spans []observe.SpanSnapshot) error {
	if e.entered != nil {
		select {
		case e.entered <- struct{}{}:
		default:
		}
		select {
		case <-e.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, span := range spans {
		e.spans = append(e.spans, span.Clone())
	}
	return nil
}
func (e *spanExporter) count() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.spans) }
