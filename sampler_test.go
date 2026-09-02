package observesdk_test

import (
	"testing"

	"goark.dev/observe"
	observesdk "goark.dev/observe-sdk"
)

func TestParentBasedSamplerRespectsUnsampledParent(t *testing.T) {
	t.Parallel()
	traceID, _ := observe.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := observe.SpanIDFromHex("0123456789abcdef")
	result := observesdk.ParentBased(observe.AlwaysOnSampler()).ShouldSample(t.Context(), observe.SamplingParameters{Parent: observe.NewSpanContext(traceID, spanID, observe.TraceFlagsNone, "vendor=value", true)})
	if result.Decision != observe.SamplingDrop || result.TraceState != "vendor=value" {
		t.Fatalf("result = %#v", result)
	}
}
