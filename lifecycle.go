package observesdk

import (
	"context"
	"errors"
	"reflect"

	"goark.dev/observe"
)

// ForceFlush 导出调用前已经接收的全部信号。
func (p *Provider) ForceFlush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.state.Load() == providerClosed {
		return nil
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var joined error
	joined = errors.Join(joined, p.flushMetrics(ctx))
	for _, lifecycle := range p.lifecycles {
		joined = errors.Join(joined, lifecycle.ForceFlush(ctx))
		if err := ctx.Err(); err != nil {
			return errors.Join(joined, err)
		}
	}
	return joined
}

// Shutdown 幂等停止接收数据、刷新管线并释放处理器和导出器资源。
func (p *Provider) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.state.Load() == providerClosed {
		return nil
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.state.Load() == providerClosed {
		return nil
	}
	p.state.Store(providerClosing)
	var joined error
	joined = errors.Join(joined, p.flushMetrics(ctx))
	for i := len(p.lifecycles) - 1; i >= 0; i-- {
		joined = errors.Join(joined, p.lifecycles[i].ForceFlush(ctx))
	}
	for i := len(p.lifecycles) - 1; i >= 0; i-- {
		joined = errors.Join(joined, p.lifecycles[i].Shutdown(ctx))
	}
	p.state.Store(providerClosed)
	return joined
}

func uniqueLifecycles(processors []observe.Processor, exporters []observe.Exporter) []observe.Lifecycle {
	items := make([]observe.Lifecycle, 0, len(processors)+len(exporters))
	seen := make(map[uintptr]struct{}, cap(items))
	appendItem := func(item observe.Lifecycle) {
		value := reflect.ValueOf(item)
		if value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Func || value.Kind() == reflect.Slice || value.Kind() == reflect.Chan {
			if value.IsNil() {
				return
			}
			pointer := value.Pointer()
			if _, ok := seen[pointer]; ok {
				return
			}
			seen[pointer] = struct{}{}
		}
		items = append(items, item)
	}
	for _, processor := range processors {
		appendItem(processor)
	}
	for _, exporter := range exporters {
		appendItem(exporter)
	}
	return items
}
