# Goark Observe SDK

[English](README.md)

`goark.dev/observe-sdk` 是 `goark.dev/observe` 核心契约的厂商无关运行时实现。它提供有界、并发安全的 trace、metric、log、event、生命周期管理和 W3C Trace Context 传播，不把应用绑定到 OpenTelemetry 或 Prometheus。

## 功能

- 完整实现 `observe.Provider`，按 instrumentation scope 缓存组件。
- 支持采样、父子 span、状态、事件、链接和进程内确定唯一的 ID。
- 支持 counter、up-down counter、gauge、显式边界 histogram 和异步回调指标。
- 每个指标具有明确的基数上限，避免无界内存增长。
- 日志与事件管线自动关联 trace 上下文。
- processor 和 exporter 的刷新、关闭并发安全且幂等。
- 可选的有界 `processor.BatchSpanProcessor`，避免请求线程同步等待 span exporter。
- 内置 benchmark 中，已有字符串标签 series 的稳定 counter 记录路径为零内存分配。

## 安装

```bash
go get goark.dev/observe-sdk
```

## 使用

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

exporter 与 processor 是独立扩展。同步热路径只能放入有界、不会长期阻塞的实现；异步处理必须显式配置有界队列。

```go
batch, err := processor.NewBatchSpanProcessor(
    spanExporter,
    processor.WithQueueSize(2048),
    processor.WithBatchSize(512),
)
provider, err := observesdk.NewProvider(observesdk.WithProcessors(batch))
```

批处理器负责其 exporter 生命周期。不要再通过 `WithExporters` 传入同一个 span exporter，否则会显式启用额外的同步导出路径。协议重试策略由 exporter 实现；批处理器负责限制内存队列，并通过错误处理器报告队列溢出。

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem
```

本项目采用 Apache License 2.0。
