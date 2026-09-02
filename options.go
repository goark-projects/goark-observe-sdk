package observesdk

import (
	"context"
	"fmt"
	"strings"

	"goark.dev/observe"
	"goark.dev/observe-sdk/propagation"
)

const defaultMetricCardinalityLimit = 2000

type config struct {
	resource               observe.Resource
	sampler                observe.Sampler
	propagator             observe.Propagator
	errorHandler           observe.ErrorHandler
	processors             []observe.Processor
	exporters              []observe.Exporter
	metricCardinalityLimit int
}

// Option 定制 SDK Provider。
type Option func(*config) error

// WithResource 设置全部信号绑定的资源信息。
func WithResource(resource observe.Resource) Option {
	return func(config *config) error {
		if strings.TrimSpace(resource.Name) == "" {
			return fmt.Errorf("observe-sdk: resource name is empty")
		}
		config.resource = resource.Clone()
		return nil
	}
}

// WithSampler 设置 trace 采样器。
func WithSampler(sampler observe.Sampler) Option {
	return func(config *config) error {
		if sampler == nil {
			return fmt.Errorf("observe-sdk: sampler is nil")
		}
		config.sampler = sampler
		return nil
	}
}

// WithPropagator 设置跨进程上下文传播器。
func WithPropagator(propagator observe.Propagator) Option {
	return func(config *config) error {
		if propagator == nil {
			return fmt.Errorf("observe-sdk: propagator is nil")
		}
		config.propagator = propagator
		return nil
	}
}

// WithErrorHandler 设置异步错误处理器。
func WithErrorHandler(handler observe.ErrorHandler) Option {
	return func(config *config) error {
		if handler == nil {
			return fmt.Errorf("observe-sdk: error handler is nil")
		}
		config.errorHandler = handler
		return nil
	}
}

// WithProcessors 追加信号处理器。
func WithProcessors(processors ...observe.Processor) Option {
	copied := append([]observe.Processor(nil), processors...)
	return func(config *config) error {
		for _, processor := range copied {
			if processor == nil {
				return fmt.Errorf("observe-sdk: processor is nil")
			}
		}
		config.processors = append(config.processors, copied...)
		return nil
	}
}

// WithExporters 追加信号导出器。
func WithExporters(exporters ...observe.Exporter) Option {
	copied := append([]observe.Exporter(nil), exporters...)
	return func(config *config) error {
		for _, exporter := range copied {
			if exporter == nil {
				return fmt.Errorf("observe-sdk: exporter is nil")
			}
			if err := exporter.Descriptor().Validate(); err != nil {
				return fmt.Errorf("observe-sdk: invalid exporter: %w", err)
			}
		}
		config.exporters = append(config.exporters, copied...)
		return nil
	}
}

// WithMetricCardinalityLimit 设置每个指标允许的最大属性组合数量。
func WithMetricCardinalityLimit(limit int) Option {
	return func(config *config) error {
		if limit <= 0 {
			return fmt.Errorf("observe-sdk: metric cardinality limit must be positive")
		}
		config.metricCardinalityLimit = limit
		return nil
	}
}

func defaultConfig() config {
	return config{
		resource:               observe.NewResource("unknown_service"),
		sampler:                observe.AlwaysOnSampler(),
		propagator:             propagation.TraceContext(),
		errorHandler:           observe.ErrorHandlerFunc(func(_ context.Context, _ error) {}),
		metricCardinalityLimit: defaultMetricCardinalityLimit,
	}
}
