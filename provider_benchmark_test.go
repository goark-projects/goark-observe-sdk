package observesdk_test

import (
	"context"
	"testing"

	"goark.dev/observe"
	observesdk "goark.dev/observe-sdk"
)

func BenchmarkCounterAdd(b *testing.B) {
	provider, _ := observesdk.NewProvider(observesdk.WithExporters(&captureExporter{}))
	counter, _ := provider.Meter("benchmark").Int64Counter("requests", observe.WithAttributeKeys("method"))
	attrs := []observe.Attr{observe.String("method", "GET")}
	b.ReportAllocs()
	for b.Loop() {
		counter.AddAttrs(context.Background(), 1, attrs)
	}
}
