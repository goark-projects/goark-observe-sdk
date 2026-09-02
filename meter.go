package observesdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"goark.dev/observe"
)

type meter struct {
	provider    *Provider
	resource    observe.Resource
	scope       observe.Scope
	mu          sync.Mutex
	instruments map[string]metricInstrument
	callbacks   map[*callbackRegistration]struct{}
}

func (m *meter) Int64ObservableCounter(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentCounter, observe.NumberInt64, options)
}
func (m *meter) Float64ObservableCounter(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentCounter, observe.NumberFloat64, options)
}
func (m *meter) Int64ObservableUpDownCounter(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentUpDownCounter, observe.NumberInt64, options)
}
func (m *meter) Float64ObservableUpDownCounter(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentUpDownCounter, observe.NumberFloat64, options)
}
func (m *meter) Int64ObservableGauge(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentGauge, observe.NumberInt64, options)
}
func (m *meter) Float64ObservableGauge(name string, options ...observe.MetricOption) (observe.Observable, error) {
	return m.observable(name, observe.InstrumentGauge, observe.NumberFloat64, options)
}

func (m *meter) observable(name string, kind observe.InstrumentKind, numberKind observe.NumberKind, options []observe.MetricOption) (observe.Observable, error) {
	descriptor, err := observe.NewMetricDescriptor(name, kind, options...)
	if err != nil {
		return nil, err
	}
	return &observable{descriptor: descriptor, numberKind: numberKind, meter: m}, nil
}

func (m *meter) RegisterCallback(callback observe.Callback, instruments ...observe.Observable) (observe.Registration, error) {
	if callback == nil {
		return nil, fmt.Errorf("%w: callback is nil", observe.ErrInvalidMetric)
	}
	allowed := make(map[*observable]struct{}, len(instruments))
	for _, instrument := range instruments {
		value, ok := instrument.(*observable)
		if !ok || value == nil || value.meter != m {
			return nil, fmt.Errorf("%w: observable belongs to another meter", observe.ErrInvalidMetric)
		}
		allowed[value] = struct{}{}
	}
	registration := &callbackRegistration{meter: m, callback: callback, instruments: allowed}
	m.mu.Lock()
	m.callbacks[registration] = struct{}{}
	m.mu.Unlock()
	return registration, nil
}

func newMeter(provider *Provider, scope observe.Scope) *meter {
	return &meter{provider: provider, resource: provider.resource.Clone(), scope: scope.Clone(), instruments: make(map[string]metricInstrument), callbacks: make(map[*callbackRegistration]struct{})}
}

func (m *meter) Enabled(context.Context, observe.InstrumentKind, string) bool {
	return m != nil && m.provider.state.Load() == providerActive
}

func (m *meter) Int64Counter(name string, options ...observe.MetricOption) (observe.Int64Counter, error) {
	value, err := m.instrument(name, observe.InstrumentCounter, observe.NumberInt64, options)
	if err != nil {
		return nil, err
	}
	return int64Counter{value}, nil
}
func (m *meter) Float64Counter(name string, options ...observe.MetricOption) (observe.Float64Counter, error) {
	value, err := m.instrument(name, observe.InstrumentCounter, observe.NumberFloat64, options)
	if err != nil {
		return nil, err
	}
	return float64Counter{value}, nil
}
func (m *meter) Int64UpDownCounter(name string, options ...observe.MetricOption) (observe.Int64UpDownCounter, error) {
	value, err := m.instrument(name, observe.InstrumentUpDownCounter, observe.NumberInt64, options)
	if err != nil {
		return nil, err
	}
	return int64UpDownCounter{value}, nil
}
func (m *meter) Float64UpDownCounter(name string, options ...observe.MetricOption) (observe.Float64UpDownCounter, error) {
	value, err := m.instrument(name, observe.InstrumentUpDownCounter, observe.NumberFloat64, options)
	if err != nil {
		return nil, err
	}
	return float64UpDownCounter{value}, nil
}
func (m *meter) Int64Histogram(name string, options ...observe.MetricOption) (observe.Int64Histogram, error) {
	value, err := m.instrument(name, observe.InstrumentHistogram, observe.NumberInt64, options)
	if err != nil {
		return nil, err
	}
	return int64Histogram{value}, nil
}
func (m *meter) Float64Histogram(name string, options ...observe.MetricOption) (observe.Float64Histogram, error) {
	value, err := m.instrument(name, observe.InstrumentHistogram, observe.NumberFloat64, options)
	if err != nil {
		return nil, err
	}
	return float64Histogram{value}, nil
}
func (m *meter) Int64Gauge(name string, options ...observe.MetricOption) (observe.Int64Gauge, error) {
	value, err := m.instrument(name, observe.InstrumentGauge, observe.NumberInt64, options)
	if err != nil {
		return nil, err
	}
	return int64Gauge{value}, nil
}
func (m *meter) Float64Gauge(name string, options ...observe.MetricOption) (observe.Float64Gauge, error) {
	value, err := m.instrument(name, observe.InstrumentGauge, observe.NumberFloat64, options)
	if err != nil {
		return nil, err
	}
	return float64Gauge{value}, nil
}

func (m *meter) instrument(name string, kind observe.InstrumentKind, numberKind observe.NumberKind, options []observe.MetricOption) (*instrument, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: meter is nil", observe.ErrInvalidMetric)
	}
	descriptor, err := observe.NewMetricDescriptor(name, kind, options...)
	if err != nil {
		return nil, err
	}
	key := metricKey(name, kind, numberKind)
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.instruments[key]; ok {
		value := existing.(*instrument)
		if !reflect.DeepEqual(value.descriptor, descriptor) {
			return nil, fmt.Errorf("%w: metric %q was created with different options", observe.ErrInvalidMetric, name)
		}
		return value, nil
	}
	value := newInstrument(m, descriptor, numberKind)
	m.instruments[key] = value
	return value, nil
}

func metricKey(name string, kind observe.InstrumentKind, numberKind observe.NumberKind) string {
	return fmt.Sprintf("%s\x00%d\x00%d", name, kind, numberKind)
}

func (m *meter) snapshot(ctx context.Context) ([]observe.MetricData, []metricCommit) {
	m.mu.Lock()
	instruments := make([]metricInstrument, 0, len(m.instruments))
	for _, value := range m.instruments {
		instruments = append(instruments, value)
	}
	callbacks := make([]*callbackRegistration, 0, len(m.callbacks))
	for callback := range m.callbacks {
		callbacks = append(callbacks, callback)
	}
	m.mu.Unlock()
	metrics := make([]observe.MetricData, 0, len(instruments))
	commits := make([]metricCommit, 0, len(instruments))
	for _, instrument := range instruments {
		if data, ok, generation := instrument.snapshot(); ok {
			metrics = append(metrics, data)
			commits = append(commits, metricCommit{instrument: instrument, generation: generation})
		}
	}
	for _, registration := range callbacks {
		metrics = append(metrics, registration.collect(ctx, m.resource, m.scope, m.provider.errors)...)
	}
	return metrics, commits
}

type metricInstrument interface {
	snapshot() (observe.MetricData, bool, uint64)
	commit(uint64)
}

type metricCommit struct {
	instrument metricInstrument
	generation uint64
}

func (p *Provider) flushMetrics(ctx context.Context) error {
	if len(p.metricProcessors)+len(p.metricExporters) == 0 {
		return nil
	}
	p.metricMu.Lock()
	meters := append([]*meter(nil), p.metricScopes...)
	p.metricMu.Unlock()
	var metrics []observe.MetricData
	var commits []metricCommit
	for _, meter := range meters {
		data, pending := meter.snapshot(ctx)
		metrics = append(metrics, data...)
		commits = append(commits, pending...)
	}
	if len(metrics) == 0 {
		return nil
	}
	var joined error
	for _, processor := range p.metricProcessors {
		if err := processor.OnMetrics(ctx, metrics); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	for _, exporter := range p.metricExporters {
		if err := exporter.ExportMetrics(ctx, metrics); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if joined == nil {
		for _, commit := range commits {
			commit.instrument.commit(commit.generation)
		}
	}
	return joined
}
