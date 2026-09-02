package observesdk

import (
	"goark.dev/observe"
)

func (i *instrument) snapshot() (observe.MetricData, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.seriesCount == 0 {
		return observe.MetricData{}, false
	}
	data := observe.MetricData{Resource: i.meter.resource.Clone(), Scope: i.meter.scope.Clone(), Name: i.descriptor.Name, Description: i.descriptor.Description, Unit: i.descriptor.Unit, Kind: i.descriptor.Kind}
	switch i.descriptor.Kind {
	case observe.InstrumentGauge:
		data.Aggregation = i.gaugeSnapshot()
	case observe.InstrumentHistogram:
		data.Aggregation = i.histogramSnapshot()
	default:
		data.Aggregation = i.sumSnapshot()
	}
	return data, true
}
func (i *instrument) gaugeSnapshot() observe.GaugeData {
	points := make([]observe.NumberPoint, 0, i.seriesCount)
	for _, bucket := range i.series {
		for _, value := range bucket {
			points = append(points, observe.NumberPoint{Attributes: observe.CloneAttrs(value.attrs), StartTime: value.start, Time: value.updated, Value: i.number(value)})
		}
	}
	return observe.GaugeData{NumberKind: i.numberKind, Points: points}
}
func (i *instrument) sumSnapshot() observe.SumData {
	points := make([]observe.NumberPoint, 0, i.seriesCount)
	for _, bucket := range i.series {
		for _, value := range bucket {
			points = append(points, observe.NumberPoint{Attributes: observe.CloneAttrs(value.attrs), StartTime: value.start, Time: value.updated, Value: i.number(value)})
		}
	}
	return observe.SumData{NumberKind: i.numberKind, Temporality: observe.TemporalityCumulative, Monotonic: i.descriptor.Kind == observe.InstrumentCounter, Points: points}
}
func (i *instrument) histogramSnapshot() observe.HistogramData {
	points := make([]observe.HistogramPoint, 0, i.seriesCount)
	for _, bucket := range i.series {
		for _, value := range bucket {
			point := observe.HistogramPoint{Attributes: observe.CloneAttrs(value.attrs), StartTime: value.start, Time: value.updated, Count: value.count, Sum: i.histogramSum(value), Bounds: append([]float64(nil), i.descriptor.ExplicitBounds...), BucketCounts: append([]uint64(nil), value.buckets...), HasMin: value.hasValue, HasMax: value.hasValue}
			if value.hasValue {
				point.Min = i.histogramNumber(value.min)
				point.Max = i.histogramNumber(value.max)
			}
			points = append(points, point)
		}
	}
	return observe.HistogramData{NumberKind: i.numberKind, Temporality: observe.TemporalityCumulative, Points: points}
}
func (i *instrument) number(value *series) observe.Number {
	if i.numberKind == observe.NumberInt64 {
		return observe.Int64Number(value.intValue)
	}
	return observe.Float64Number(value.floatValue)
}
func (i *instrument) histogramSum(value *series) observe.Number {
	if i.numberKind == observe.NumberInt64 {
		return observe.Int64Number(value.intValue)
	}
	return observe.Float64Number(value.floatValue)
}
func (i *instrument) histogramNumber(value float64) observe.Number {
	if i.numberKind == observe.NumberInt64 {
		return observe.Int64Number(int64(value))
	}
	return observe.Float64Number(value)
}
