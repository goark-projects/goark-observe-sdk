package propagation

import (
	"context"
	"strings"

	"goark.dev/observe"
)

const traceParentHeader = "traceparent"

// TraceContext 返回 W3C Trace Context 传播器。
func TraceContext() observe.Propagator {
	return traceContext{}
}

type traceContext struct{}

func (traceContext) Inject(ctx context.Context, carrier observe.TextMapCarrier) {
	if carrier == nil {
		return
	}
	spanContext := observe.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return
	}
	flags := "00"
	if spanContext.IsSampled() {
		flags = "01"
	}
	carrier.Set(traceParentHeader, "00-"+spanContext.TraceID.String()+"-"+spanContext.SpanID.String()+"-"+flags)
}

func (traceContext) Extract(ctx context.Context, carrier observe.TextMapCarrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if carrier == nil {
		return ctx
	}
	parts := strings.Split(carrier.Get(traceParentHeader), "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[3]) != 2 {
		return ctx
	}
	traceID, err := observe.TraceIDFromHex(parts[1])
	if err != nil {
		return ctx
	}
	spanID, err := observe.SpanIDFromHex(parts[2])
	if err != nil {
		return ctx
	}
	var flags observe.TraceFlags
	if parts[3] == "01" {
		flags = observe.TraceFlagsSampled
	} else if parts[3] != "00" {
		return ctx
	}
	return observe.ContextWithSpanContext(ctx, observe.NewSpanContext(traceID, spanID, flags, "", true))
}

func (traceContext) Fields() []string {
	return []string{traceParentHeader}
}
