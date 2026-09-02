package processor

import (
	"context"
	"errors"
	"time"

	"goark.dev/observe"
)

type spanCommand uint8

const (
	commandNone spanCommand = iota
	commandFlush
	commandShutdown
)

type spanMessage struct {
	span     observe.SpanSnapshot
	command  spanCommand
	response chan error
	ctx      context.Context
}

func (p *BatchSpanProcessor) run() {
	timer := time.NewTimer(p.config.scheduleDelay)
	defer timer.Stop()
	batch := make([]observe.SpanSnapshot, 0, p.config.batchSize)
	for {
		select {
		case message := <-p.queue:
			if message.command != commandNone {
				err := p.export(message.ctx, batch)
				batch = batch[:0]
				flushCtx, cancelFlush := p.lifecycleContext(message.ctx)
				err = errors.Join(err, p.exporter.ForceFlush(flushCtx))
				cancelFlush()
				if message.command == commandShutdown {
					shutdownCtx, cancelShutdown := p.lifecycleContext(message.ctx)
					err = errors.Join(err, p.exporter.Shutdown(shutdownCtx))
					cancelShutdown()
					p.mu.Lock()
					p.state = spanClosed
					p.shutdownErr = err
					p.mu.Unlock()
					message.response <- err
					close(p.done)
					return
				}
				message.response <- err
				resetTimer(timer, p.config.scheduleDelay)
				continue
			}
			batch = append(batch, message.span)
			if len(batch) >= p.config.batchSize {
				if err := p.export(context.Background(), batch); err != nil {
					p.report(context.Background(), err)
				}
				batch = batch[:0]
				resetTimer(timer, p.config.scheduleDelay)
			}
		case <-timer.C:
			if len(batch) > 0 {
				if err := p.export(context.Background(), batch); err != nil {
					p.report(context.Background(), err)
				}
				batch = batch[:0]
			}
			timer.Reset(p.config.scheduleDelay)
		}
	}
}

func (p *BatchSpanProcessor) export(parent context.Context, spans []observe.SpanSnapshot) error {
	if len(spans) == 0 {
		return nil
	}
	ctx, cancel := p.lifecycleContext(parent)
	defer cancel()
	return p.exporter.ExportSpans(ctx, spans)
}
func (p *BatchSpanProcessor) lifecycleContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, p.config.exportTimeout)
}
func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
