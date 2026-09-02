package observesdk

import (
	"context"
	"time"

	"goark.dev/observe"
)

type observer struct {
	provider *Provider
	scope    observe.Scope
}

func (o *observer) Enabled(ctx context.Context, kind observe.SignalKind, name string) bool {
	if o == nil || o.provider.state.Load() != providerActive {
		return false
	}
	switch kind {
	case observe.SignalTraces:
		return len(o.provider.spanProcessors)+len(o.provider.spanExporters) > 0
	case observe.SignalMetrics:
		return len(o.provider.metricProcessors)+len(o.provider.metricExporters) > 0
	case observe.SignalLogs:
		return o.provider.Logger(o.scope.Name).Enabled(ctx, observe.LevelInfo)
	case observe.SignalEvents:
		return o.provider.Eventer(o.scope.Name).Enabled(ctx, name)
	default:
		return false
	}
}

func (o *observer) Observe(ctx context.Context, observation observe.Observation) {
	if !o.Enabled(ctx, observation.Kind, observation.Name) {
		return
	}
	switch observation.Kind {
	case observe.SignalLogs:
		record := observe.LogRecord{Message: observation.Body.String(), Attributes: observation.Attributes}
		if !observation.Time.IsZero() {
			record.Time = observation.Time.Time()
		}
		o.provider.Logger(o.scope.Name).EmitRecord(ctx, record)
	case observe.SignalEvents:
		record := observe.EventRecord{Name: observation.Name, Body: observation.Body, Attributes: observation.Attributes}
		if !observation.Time.IsZero() {
			record.Time = observation.Time.Time()
		}
		o.provider.Eventer(o.scope.Name).EmitEvent(ctx, record)
	case observe.SignalTraces:
		start := time.Now()
		if !observation.Time.IsZero() {
			start = observation.Time.Time()
		}
		_, span := o.Start(ctx, observation.Name, observe.WithStartTime(start), observe.WithSpanAttrs(observation.Attributes...))
		span.End()
	}
}

func (o *observer) Start(ctx context.Context, name string, options ...observe.SpanStartOption) (context.Context, observe.Span) {
	return o.provider.Tracer(o.scope.Name, observe.WithScopeVersion(o.scope.Version), observe.WithSchemaURL(o.scope.SchemaURL), observe.WithScopeAttrs(o.scope.Attributes...)).Start(ctx, name, options...)
}
