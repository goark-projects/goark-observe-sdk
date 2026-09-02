package observesdk

import (
	"context"
	"time"

	"goark.dev/observe"
)

type eventer struct {
	provider *Provider
	scope    observe.Scope
}

func (e *eventer) Enabled(context.Context, string) bool {
	return e != nil && e.provider.state.Load() == providerActive && (len(e.provider.eventProcessors)+len(e.provider.eventExporters) > 0)
}
func (e *eventer) Emit(ctx context.Context, name string, attrs ...observe.Attr) {
	e.EmitAttrs(ctx, name, attrs)
}
func (e *eventer) EmitAttrs(ctx context.Context, name string, attrs []observe.Attr) {
	e.EmitEvent(ctx, observe.EventRecord{Name: name, Attributes: attrs})
}
func (e *eventer) EmitEvent(ctx context.Context, event observe.EventRecord) {
	if !e.Enabled(ctx, event.Name) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	event.Resource = e.provider.resource.Clone()
	event.Scope = e.scope.Clone()
	event.Attributes = observe.CloneAttrs(event.Attributes)
	if !event.SpanContext.IsValid() {
		event.SpanContext = observe.SpanContextFromContext(ctx)
	}
	batch := []observe.EventRecord{event}
	for _, processor := range e.provider.eventProcessors {
		if err := processor.OnEvents(ctx, batch); err != nil {
			e.provider.report(ctx, err)
		}
	}
	for _, exporter := range e.provider.eventExporters {
		if err := exporter.ExportEvents(ctx, batch); err != nil {
			e.provider.report(ctx, err)
		}
	}
}
