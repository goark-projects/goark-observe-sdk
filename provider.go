package observesdk

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"goark.dev/observe"
)

const (
	providerActive uint32 = iota
	providerClosing
	providerClosed
)

// Provider 是可直接注册到应用组合根的 SDK 实现。
type Provider struct {
	resource   observe.Resource
	sampler    observe.Sampler
	propagator observe.Propagator
	errors     observe.ErrorHandler
	limit      int

	spanProcessors   []observe.SpanProcessor
	metricProcessors []observe.MetricProcessor
	logProcessors    []observe.LogProcessor
	eventProcessors  []observe.EventProcessor
	spanExporters    []observe.SpanExporter
	metricExporters  []observe.MetricExporter
	logExporters     []observe.LogExporter
	eventExporters   []observe.EventExporter
	lifecycles       []observe.Lifecycle

	tracers  sync.Map
	meters   sync.Map
	loggers  sync.Map
	eventers sync.Map

	metricMu     sync.Mutex
	metricScopes []*meter
	lifecycleMu  sync.Mutex
	state        atomic.Uint32
	idCounter    atomic.Uint64
	idSeed       uint64
}

// NewProvider 创建并校验 SDK Provider。
func NewProvider(options ...Option) (*Provider, error) {
	config := defaultConfig()
	for _, option := range options {
		if option != nil {
			if err := option(&config); err != nil {
				return nil, err
			}
		}
	}
	provider := &Provider{
		resource:   config.resource.Clone(),
		sampler:    config.sampler,
		propagator: config.propagator,
		errors:     config.errorHandler,
		limit:      config.metricCardinalityLimit,
		idSeed:     randomSeed(),
	}
	provider.bindProcessors(config.processors)
	provider.bindExporters(config.exporters)
	provider.lifecycles = uniqueLifecycles(config.processors, config.exporters)
	return provider, nil
}

func (p *Provider) bindProcessors(processors []observe.Processor) {
	for _, processor := range processors {
		if typed, ok := processor.(observe.SpanProcessor); ok {
			p.spanProcessors = append(p.spanProcessors, typed)
		}
		if typed, ok := processor.(observe.MetricProcessor); ok {
			p.metricProcessors = append(p.metricProcessors, typed)
		}
		if typed, ok := processor.(observe.LogProcessor); ok {
			p.logProcessors = append(p.logProcessors, typed)
		}
		if typed, ok := processor.(observe.EventProcessor); ok {
			p.eventProcessors = append(p.eventProcessors, typed)
		}
	}
}

func (p *Provider) bindExporters(exporters []observe.Exporter) {
	for _, exporter := range exporters {
		if typed, ok := exporter.(observe.SpanExporter); ok {
			p.spanExporters = append(p.spanExporters, typed)
		}
		if typed, ok := exporter.(observe.MetricExporter); ok {
			p.metricExporters = append(p.metricExporters, typed)
		}
		if typed, ok := exporter.(observe.LogExporter); ok {
			p.logExporters = append(p.logExporters, typed)
		}
		if typed, ok := exporter.(observe.EventExporter); ok {
			p.eventExporters = append(p.eventExporters, typed)
		}
	}
}

// Tracer 返回按 instrumentation scope 缓存的 tracer。
func (p *Provider) Tracer(name string, options ...observe.ScopeOption) observe.Tracer {
	if p == nil || p.state.Load() != providerActive || len(p.spanProcessors)+len(p.spanExporters) == 0 {
		return observe.NoopTracer()
	}
	scope := observe.NewScope(name, options...)
	key := scopeKey(scope)
	value, _ := p.tracers.LoadOrStore(key, &tracer{provider: p, scope: scope})
	return value.(*tracer)
}

// Meter 返回按 instrumentation scope 缓存的 meter。
func (p *Provider) Meter(name string, options ...observe.ScopeOption) observe.Meter {
	if p == nil || p.state.Load() != providerActive || len(p.metricProcessors)+len(p.metricExporters) == 0 {
		return observe.NoopMeter()
	}
	scope := observe.NewScope(name, options...)
	key := scopeKey(scope)
	if value, ok := p.meters.Load(key); ok {
		return value.(*meter)
	}
	created := newMeter(p, scope)
	value, loaded := p.meters.LoadOrStore(key, created)
	if loaded {
		return value.(*meter)
	}
	p.metricMu.Lock()
	p.metricScopes = append(p.metricScopes, created)
	p.metricMu.Unlock()
	return created
}

// Logger 返回按 instrumentation scope 缓存的日志桥接器。
func (p *Provider) Logger(name string, options ...observe.ScopeOption) observe.Logger {
	if p == nil || p.state.Load() != providerActive || len(p.logProcessors)+len(p.logExporters) == 0 {
		return observe.NoopLogger()
	}
	scope := observe.NewScope(name, options...)
	key := scopeKey(scope)
	value, _ := p.loggers.LoadOrStore(key, &logger{provider: p, scope: scope})
	return value.(*logger)
}

// Eventer 返回按 instrumentation scope 缓存的事件发送器。
func (p *Provider) Eventer(name string, options ...observe.ScopeOption) observe.Eventer {
	if p == nil || p.state.Load() != providerActive || len(p.eventProcessors)+len(p.eventExporters) == 0 {
		return observe.NoopEventer()
	}
	scope := observe.NewScope(name, options...)
	key := scopeKey(scope)
	value, _ := p.eventers.LoadOrStore(key, &eventer{provider: p, scope: scope})
	return value.(*eventer)
}

// Observer 返回统一轻量观测入口。
func (p *Provider) Observer(name string, options ...observe.ScopeOption) observe.Observer {
	if p == nil || p.state.Load() != providerActive {
		return observe.NoopObserver()
	}
	return &observer{provider: p, scope: observe.NewScope(name, options...)}
}

// Propagator 返回 Provider 使用的传播器。
func (p *Provider) Propagator() observe.Propagator {
	if p == nil || p.propagator == nil {
		return observe.NoopPropagator()
	}
	return p.propagator
}

func (p *Provider) report(ctx context.Context, err error) {
	if err != nil && p != nil && p.errors != nil {
		p.errors.Handle(ctx, err)
	}
}

func scopeKey(scope observe.Scope) string {
	var builder strings.Builder
	builder.Grow(len(scope.Name) + len(scope.Version) + len(scope.SchemaURL) + 32)
	builder.WriteString(scope.Name)
	builder.WriteByte(0)
	builder.WriteString(scope.Version)
	builder.WriteByte(0)
	builder.WriteString(scope.SchemaURL)
	for _, attr := range scope.Attributes {
		builder.WriteByte(0)
		builder.WriteString(attr.Key.String())
		builder.WriteByte('=')
		builder.WriteString(strconv.Itoa(int(attr.Value.Kind())))
		builder.WriteByte(':')
		builder.WriteString(attr.Value.String())
	}
	return builder.String()
}

func (p *Provider) ensureActive() error {
	if p == nil || p.state.Load() != providerActive {
		return fmt.Errorf("observe-sdk: provider is shut down")
	}
	return nil
}
