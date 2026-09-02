package propagation_test

import (
	"context"
	"testing"

	"goark.dev/observe"
	"goark.dev/observe-sdk/propagation"
)

func TestTraceContextRoundTrip(t *testing.T) {
	t.Parallel()
	traceID, _ := observe.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := observe.SpanIDFromHex("0123456789abcdef")
	ctx := observe.ContextWithSpanContext(context.Background(), observe.NewSpanContext(traceID, spanID, observe.TraceFlagsSampled, "", false))
	carrier := observe.MapCarrier{}
	propagator := propagation.TraceContext()
	propagator.Inject(ctx, carrier)
	extracted := observe.SpanContextFromContext(propagator.Extract(context.Background(), carrier))
	if extracted.TraceID != traceID || extracted.SpanID != spanID || !extracted.IsSampled() || !extracted.Remote {
		t.Fatalf("extracted = %#v", extracted)
	}
}
