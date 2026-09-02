package observesdk

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"goark.dev/observe"
)

type observable struct {
	descriptor observe.MetricDescriptor
	numberKind observe.NumberKind
	meter      *meter
}

func (o *observable) Descriptor() observe.MetricDescriptor { return o.descriptor.Clone() }
func (o *observable) NumberKind() observe.NumberKind       { return o.numberKind }

type callbackRegistration struct {
	meter        *meter
	callback     observe.Callback
	instruments  map[*observable]struct{}
	unregistered atomic.Bool
}

func (r *callbackRegistration) Unregister() error {
	if r == nil || !r.unregistered.CompareAndSwap(false, true) {
		return nil
	}
	r.meter.mu.Lock()
	delete(r.meter.callbacks, r)
	r.meter.mu.Unlock()
	return nil
}

func (r *callbackRegistration) collect(ctx context.Context, resource observe.Resource, scope observe.Scope, handler observe.ErrorHandler) []observe.MetricData {
	if r == nil || r.unregistered.Load() {
		return nil
	}
	collector := &callbackCollector{allowed: r.instruments, values: make(map[*observable][]observe.NumberPoint)}
	if err := r.callback(ctx, collector); err != nil {
		handler.Handle(ctx, err)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	out := make([]observe.MetricData, 0, len(collector.values))
	for instrument, points := range collector.values {
		data := observe.MetricData{Resource: resource.Clone(), Scope: scope.Clone(), Name: instrument.descriptor.Name, Description: instrument.descriptor.Description, Unit: instrument.descriptor.Unit, Kind: instrument.descriptor.Kind}
		if instrument.descriptor.Kind == observe.InstrumentGauge {
			data.Aggregation = observe.GaugeData{NumberKind: instrument.numberKind, Points: points}
		} else {
			data.Aggregation = observe.SumData{NumberKind: instrument.numberKind, Temporality: observe.TemporalityCumulative, Monotonic: instrument.descriptor.Kind == observe.InstrumentCounter, Points: points}
		}
		out = append(out, data)
	}
	return out
}

type callbackCollector struct {
	mu      sync.Mutex
	allowed map[*observable]struct{}
	values  map[*observable][]observe.NumberPoint
}

func (c *callbackCollector) ObserveInt64(instrument observe.Observable, value int64, attrs ...observe.Attr) {
	c.ObserveInt64Attrs(instrument, value, attrs)
}
func (c *callbackCollector) ObserveInt64Attrs(instrument observe.Observable, value int64, attrs []observe.Attr) {
	c.record(instrument, observe.Int64Number(value), observe.NumberInt64, attrs)
}
func (c *callbackCollector) ObserveFloat64(instrument observe.Observable, value float64, attrs ...observe.Attr) {
	c.ObserveFloat64Attrs(instrument, value, attrs)
}
func (c *callbackCollector) ObserveFloat64Attrs(instrument observe.Observable, value float64, attrs []observe.Attr) {
	c.record(instrument, observe.Float64Number(value), observe.NumberFloat64, attrs)
}
func (c *callbackCollector) record(instrument observe.Observable, value observe.Number, kind observe.NumberKind, attrs []observe.Attr) {
	typed, ok := instrument.(*observable)
	if !ok || typed.numberKind != kind {
		return
	}
	if _, ok = c.allowed[typed]; !ok {
		return
	}
	for _, attr := range attrs {
		if observe.ValidateMetricAttr(attr) != nil {
			return
		}
	}
	point := observe.NumberPoint{Attributes: observe.CloneAttrs(attrs), Time: time.Now(), Value: value}
	c.mu.Lock()
	c.values[typed] = append(c.values[typed], point)
	c.mu.Unlock()
}
