package observesdk

import (
	"context"
	"time"

	"goark.dev/observe"
)

type logger struct {
	provider *Provider
	scope    observe.Scope
}

func (l *logger) Enabled(context.Context, observe.Level) bool {
	return l != nil && l.provider.state.Load() == providerActive && (len(l.provider.logProcessors)+len(l.provider.logExporters) > 0)
}

func (l *logger) Log(ctx context.Context, level observe.Level, message string, attrs ...observe.Attr) {
	l.LogAttrs(ctx, level, message, attrs)
}

func (l *logger) LogAttrs(ctx context.Context, level observe.Level, message string, attrs []observe.Attr) {
	l.EmitRecord(ctx, observe.LogRecord{Level: level, Message: message, Attributes: attrs})
}

func (l *logger) EmitRecord(ctx context.Context, record observe.LogRecord) {
	if !l.Enabled(ctx, record.Level) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	if record.Time.IsZero() {
		record.Time = now
	}
	record.ObservedTime = now
	record.Resource = l.provider.resource.Clone()
	record.Scope = l.scope.Clone()
	record.Attributes = observe.CloneAttrs(record.Attributes)
	if record.LevelText == "" {
		record.LevelText = record.Level.String()
	}
	if !record.SpanContext.IsValid() {
		record.SpanContext = observe.SpanContextFromContext(ctx)
	}
	batch := []observe.LogRecord{record}
	for _, processor := range l.provider.logProcessors {
		if err := processor.OnLogs(ctx, batch); err != nil {
			l.provider.report(ctx, err)
		}
	}
	for _, exporter := range l.provider.logExporters {
		if err := exporter.ExportLogs(ctx, batch); err != nil {
			l.provider.report(ctx, err)
		}
	}
}
