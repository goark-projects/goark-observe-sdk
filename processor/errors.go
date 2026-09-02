package processor

import "errors"

var (
	// ErrQueueFull 表示有界队列已满，当前信号被丢弃。
	ErrQueueFull = errors.New("observe-sdk/processor: queue is full")
	// ErrShutdown 表示处理器已经关闭，不再接收信号。
	ErrShutdown = errors.New("observe-sdk/processor: processor is shut down")
)
