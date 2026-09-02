package processor

import (
	"context"
	"fmt"
	"time"

	"goark.dev/observe"
)

const (
	defaultQueueSize     = 2048
	defaultBatchSize     = 512
	defaultScheduleDelay = 5 * time.Second
	defaultExportTimeout = 30 * time.Second
)

type config struct {
	queueSize     int
	batchSize     int
	scheduleDelay time.Duration
	exportTimeout time.Duration
	errorHandler  observe.ErrorHandler
}

// Option 定制批处理器的有界资源和导出行为。
type Option func(*config) error

// WithQueueSize 设置最多等待导出的 span 数量。
func WithQueueSize(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return fmt.Errorf("observe-sdk/processor: queue size must be positive")
		}
		c.queueSize = size
		return nil
	}
}

// WithBatchSize 设置单次导出的最大 span 数量。
func WithBatchSize(size int) Option {
	return func(c *config) error {
		if size <= 0 {
			return fmt.Errorf("observe-sdk/processor: batch size must be positive")
		}
		c.batchSize = size
		return nil
	}
}

// WithScheduleDelay 设置未满批次的最长等待时间。
func WithScheduleDelay(delay time.Duration) Option {
	return func(c *config) error {
		if delay <= 0 {
			return fmt.Errorf("observe-sdk/processor: schedule delay must be positive")
		}
		c.scheduleDelay = delay
		return nil
	}
}

// WithExportTimeout 设置每次 exporter 调用的最大持续时间。
func WithExportTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return fmt.Errorf("observe-sdk/processor: export timeout must be positive")
		}
		c.exportTimeout = timeout
		return nil
	}
}

// WithErrorHandler 设置热路径无法返回的队列和周期导出错误处理器。
func WithErrorHandler(handler observe.ErrorHandler) Option {
	return func(c *config) error {
		if handler == nil {
			return fmt.Errorf("observe-sdk/processor: error handler is nil")
		}
		c.errorHandler = handler
		return nil
	}
}

func newConfig(options []Option) (config, error) {
	resolved := config{queueSize: defaultQueueSize, batchSize: defaultBatchSize, scheduleDelay: defaultScheduleDelay, exportTimeout: defaultExportTimeout, errorHandler: observe.ErrorHandlerFunc(func(context.Context, error) {})}
	for _, option := range options {
		if option != nil {
			if err := option(&resolved); err != nil {
				return config{}, err
			}
		}
	}
	if resolved.batchSize > resolved.queueSize {
		return config{}, fmt.Errorf("observe-sdk/processor: batch size exceeds queue size")
	}
	return resolved, nil
}
