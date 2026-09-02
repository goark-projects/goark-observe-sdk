package observesdk

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"goark.dev/observe"
)

type series struct {
	attrs      []observe.Attr
	start      time.Time
	updated    time.Time
	intValue   int64
	floatValue float64
	count      uint64
	buckets    []uint64
	min        float64
	max        float64
	hasValue   bool
}

type instrument struct {
	meter       *meter
	descriptor  observe.MetricDescriptor
	numberKind  observe.NumberKind
	mu          sync.Mutex
	series      map[uint64][]*series
	seriesCount int
}

func newInstrument(meter *meter, descriptor observe.MetricDescriptor, numberKind observe.NumberKind) *instrument {
	return &instrument{meter: meter, descriptor: descriptor.Clone(), numberKind: numberKind, series: make(map[uint64][]*series)}
}

func (i *instrument) recordInt(ctx context.Context, value int64, attrs []observe.Attr, monotonic bool) {
	if i == nil || i.meter.provider.state.Load() != providerActive || (monotonic && value < 0) {
		return
	}
	key, ok := i.seriesKey(attrs)
	if !ok {
		return
	}
	now := time.Now()
	i.mu.Lock()
	defer i.mu.Unlock()
	point := i.getSeries(key, attrs, now)
	if point == nil {
		return
	}
	switch i.descriptor.Kind {
	case observe.InstrumentHistogram:
		point.intValue += value
		i.recordHistogram(point, float64(value), now)
	case observe.InstrumentGauge:
		point.intValue = value
		point.updated = now
	default:
		point.intValue += value
		point.updated = now
	}
}
func (i *instrument) recordFloat(ctx context.Context, value float64, attrs []observe.Attr, monotonic bool) {
	if i == nil || i.meter.provider.state.Load() != providerActive || math.IsNaN(value) || math.IsInf(value, 0) || (monotonic && value < 0) {
		return
	}
	key, ok := i.seriesKey(attrs)
	if !ok {
		return
	}
	now := time.Now()
	i.mu.Lock()
	defer i.mu.Unlock()
	point := i.getSeries(key, attrs, now)
	if point == nil {
		return
	}
	switch i.descriptor.Kind {
	case observe.InstrumentHistogram:
		i.recordHistogram(point, value, now)
	case observe.InstrumentGauge:
		point.floatValue = value
		point.updated = now
	default:
		point.floatValue += value
		point.updated = now
	}
}
func (i *instrument) getSeries(key uint64, attrs []observe.Attr, now time.Time) *series {
	for _, point := range i.series[key] {
		if i.matches(point.attrs, attrs) {
			return point
		}
	}
	if i.seriesCount >= i.meter.provider.limit {
		return nil
	}
	point := &series{attrs: i.filteredAttrs(attrs), start: now, updated: now}
	if i.descriptor.Kind == observe.InstrumentHistogram {
		point.buckets = make([]uint64, len(i.descriptor.ExplicitBounds)+1)
	}
	i.series[key] = append(i.series[key], point)
	i.seriesCount++
	return point
}
func (i *instrument) recordHistogram(point *series, value float64, now time.Time) {
	point.count++
	point.floatValue += value
	bucket := sort.SearchFloat64s(i.descriptor.ExplicitBounds, value)
	point.buckets[bucket]++
	if !point.hasValue || value < point.min {
		point.min = value
	}
	if !point.hasValue || value > point.max {
		point.max = value
	}
	point.hasValue = true
	point.updated = now
}

func (i *instrument) seriesKey(attrs []observe.Attr) (uint64, bool) {
	if len(i.descriptor.AttributeKeys) == 0 {
		return 0, true
	}
	for _, attr := range attrs {
		if observe.ValidateMetricAttr(attr) != nil {
			return 0, false
		}
	}
	hash := uint64(14695981039346656037)
	for _, key := range i.descriptor.AttributeKeys {
		value, ok := findAttr(attrs, key)
		if !ok {
			continue
		}
		hash = hashString(hash, key.String())
		hash ^= uint64(value.Kind())
		hash *= 1099511628211
		hash = hashString(hash, value.String())
	}
	return hash, true
}

func hashString(hash uint64, value string) uint64 {
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= 1099511628211
	}
	return hash
}

func (i *instrument) matches(expected []observe.Attr, actual []observe.Attr) bool {
	matched := 0
	for _, key := range i.descriptor.AttributeKeys {
		value, ok := findAttr(actual, key)
		if !ok {
			continue
		}
		if matched >= len(expected) || expected[matched].Key != key || expected[matched].Value != value {
			return false
		}
		matched++
	}
	return matched == len(expected)
}

func (i *instrument) filteredAttrs(attrs []observe.Attr) []observe.Attr {
	filtered := make([]observe.Attr, 0, len(i.descriptor.AttributeKeys))
	for _, key := range i.descriptor.AttributeKeys {
		if value, ok := findAttr(attrs, key); ok {
			filtered = append(filtered, observe.Attr{Key: key, Value: value})
		}
	}
	return filtered
}

func findAttr(attrs []observe.Attr, key observe.Key) (observe.Value, bool) {
	for i := len(attrs) - 1; i >= 0; i-- {
		if attrs[i].Key == key {
			return attrs[i].Value, true
		}
	}
	return observe.Value{}, false
}

type int64Counter struct{ *instrument }

func (c int64Counter) Add(ctx context.Context, v int64, a ...observe.Attr) {
	c.recordInt(ctx, v, a, true)
}
func (c int64Counter) AddAttrs(ctx context.Context, v int64, a []observe.Attr) {
	c.recordInt(ctx, v, a, true)
}

type float64Counter struct{ *instrument }

func (c float64Counter) Add(ctx context.Context, v float64, a ...observe.Attr) {
	c.recordFloat(ctx, v, a, true)
}
func (c float64Counter) AddAttrs(ctx context.Context, v float64, a []observe.Attr) {
	c.recordFloat(ctx, v, a, true)
}

type int64UpDownCounter struct{ *instrument }

func (c int64UpDownCounter) Add(ctx context.Context, v int64, a ...observe.Attr) {
	c.recordInt(ctx, v, a, false)
}
func (c int64UpDownCounter) AddAttrs(ctx context.Context, v int64, a []observe.Attr) {
	c.recordInt(ctx, v, a, false)
}

type float64UpDownCounter struct{ *instrument }

func (c float64UpDownCounter) Add(ctx context.Context, v float64, a ...observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}
func (c float64UpDownCounter) AddAttrs(ctx context.Context, v float64, a []observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}

type int64Histogram struct{ *instrument }

func (c int64Histogram) Record(ctx context.Context, v int64, a ...observe.Attr) {
	c.recordInt(ctx, v, a, false)
}
func (c int64Histogram) RecordAttrs(ctx context.Context, v int64, a []observe.Attr) {
	c.recordInt(ctx, v, a, false)
}

type float64Histogram struct{ *instrument }

func (c float64Histogram) Record(ctx context.Context, v float64, a ...observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}
func (c float64Histogram) RecordAttrs(ctx context.Context, v float64, a []observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}

type int64Gauge struct{ *instrument }

func (c int64Gauge) Record(ctx context.Context, v int64, a ...observe.Attr) {
	c.recordInt(ctx, v, a, false)
}
func (c int64Gauge) RecordAttrs(ctx context.Context, v int64, a []observe.Attr) {
	c.recordInt(ctx, v, a, false)
}

type float64Gauge struct{ *instrument }

func (c float64Gauge) Record(ctx context.Context, v float64, a ...observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}
func (c float64Gauge) RecordAttrs(ctx context.Context, v float64, a []observe.Attr) {
	c.recordFloat(ctx, v, a, false)
}
