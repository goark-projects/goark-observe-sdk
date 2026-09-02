# Goark Observe SDK

[简体中文](README.zh-CN.md)

`goark.dev/observe-sdk` is the vendor-neutral runtime implementation of the contracts in `goark.dev/observe`. It provides bounded, concurrency-safe traces, metrics, logs, events, lifecycle management, and W3C Trace Context propagation without binding applications to OpenTelemetry or Prometheus.

## Features

- Cached instrumentation scopes and a complete `observe.Provider` implementation.
- Sampling, parent-child spans, status, events, links, and deterministic process-unique IDs.
- Counters, up-down counters, gauges, explicit-bound histograms, and observable callbacks.
- Bounded metric cardinality per instrument.
- Structured log and event pipelines with trace correlation.
- Idempotent flush and shutdown across processors and exporters.
- Zero-allocation steady-state counter recording for existing string-label series in the included benchmark.

## Install

```bash
go get goark.dev/observe-sdk
```

## Usage

```go
provider, err := observesdk.NewProvider(
    observesdk.WithResource(observe.NewResource("orders")),
    observesdk.WithExporters(exporter),
)
if err != nil {
    return err
}
defer provider.Shutdown(context.Background())

ctx, span := provider.Tracer("example/orders").Start(ctx, "orders.create")
defer span.End()
```

Exporters and processors remain separate extensions. Pass only bounded, non-blocking implementations on synchronous hot paths, or use an explicitly bounded asynchronous processor.

## Validation

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem
```

Licensed under Apache License 2.0.
