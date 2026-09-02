package processor

import (
	"context"
	"fmt"
	"sync"

	"goark.dev/observe"
)

const (
	spanActive uint8 = iota
	spanClosing
	spanClosed
)

// BatchSpanProcessor 使用单后台协程和固定容量队列批量导出已结束 span。
type BatchSpanProcessor struct {
	mu          sync.Mutex
	exporter    observe.SpanExporter
	config      config
	queue       chan spanMessage
	state       uint8
	done        chan struct{}
	shutdownErr error
}

// NewBatchSpanProcessor 创建批量 span 处理器。
func NewBatchSpanProcessor(exporter observe.SpanExporter, options ...Option) (*BatchSpanProcessor, error) {
	if exporter == nil {
		return nil, fmt.Errorf("observe-sdk/processor: span exporter is nil")
	}
	config, err := newConfig(options)
	if err != nil {
		return nil, err
	}
	p := &BatchSpanProcessor{exporter: exporter, config: config, queue: make(chan spanMessage, config.queueSize), done: make(chan struct{})}
	go p.run()
	return p, nil
}

// OnStart 不修改正在记录的 span。
func (*BatchSpanProcessor) OnStart(context.Context, observe.Span) {}

// OnEnd 克隆快照并以非阻塞方式写入有界队列。
func (p *BatchSpanProcessor) OnEnd(ctx context.Context, span observe.SpanSnapshot) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.state != spanActive {
		p.mu.Unlock()
		p.report(ctx, ErrShutdown)
		return
	}
	select {
	case p.queue <- spanMessage{span: span.Clone()}:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		p.report(ctx, ErrQueueFull)
	}
}

// ForceFlush 等待调用前已接收的 span 完成导出。
func (p *BatchSpanProcessor) ForceFlush(ctx context.Context) error {
	return p.command(ctx, commandFlush)
}

// Shutdown 停止接收新 span，导出队列数据并关闭 exporter。
func (p *BatchSpanProcessor) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if p.state == spanClosed {
		err := p.shutdownErr
		p.mu.Unlock()
		return err
	}
	if p.state == spanClosing {
		done := p.done
		p.mu.Unlock()
		select {
		case <-done:
			p.mu.Lock()
			err := p.shutdownErr
			p.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.state = spanClosing
	response := make(chan error, 1)
	select {
	case p.queue <- spanMessage{command: commandShutdown, response: response, ctx: ctx}:
		p.mu.Unlock()
	case <-ctx.Done():
		p.state = spanActive
		p.mu.Unlock()
		return ctx.Err()
	}
	select {
	case err := <-response:
		<-p.done
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *BatchSpanProcessor) command(ctx context.Context, command spanCommand) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if p.state == spanClosed {
		p.mu.Unlock()
		return nil
	}
	if p.state != spanActive {
		p.mu.Unlock()
		return ErrShutdown
	}
	response := make(chan error, 1)
	select {
	case p.queue <- spanMessage{command: command, response: response, ctx: ctx}:
		p.mu.Unlock()
	case <-ctx.Done():
		p.mu.Unlock()
		return ctx.Err()
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *BatchSpanProcessor) report(ctx context.Context, err error) {
	if err != nil && p.config.errorHandler != nil {
		p.config.errorHandler.Handle(ctx, err)
	}
}
