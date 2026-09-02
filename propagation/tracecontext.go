package propagation

import (
	"context"
	"strconv"
	"strings"

	"goark.dev/observe"
)

const traceParentHeader = "traceparent"
const traceStateHeader = "tracestate"

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
	if validTraceState(spanContext.TraceState) {
		carrier.Set(traceStateHeader, spanContext.TraceState)
	}
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
	parsedFlags, err := strconv.ParseUint(parts[3], 16, 8)
	if err != nil {
		return ctx
	}
	var flags observe.TraceFlags
	if parsedFlags&1 != 0 {
		flags = observe.TraceFlagsSampled
	}
	traceState := carrier.Get(traceStateHeader)
	if !validTraceState(traceState) {
		traceState = ""
	}
	return observe.ContextWithSpanContext(ctx, observe.NewSpanContext(traceID, spanID, flags, traceState, true))
}

func (traceContext) Fields() []string {
	return []string{traceParentHeader, traceStateHeader}
}

func validTraceState(value string) bool {
	return len(value) <= 512 && !strings.ContainsAny(value, "\r\n")
}
