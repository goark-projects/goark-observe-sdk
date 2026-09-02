// Package observesdk 提供与厂商无关的可观测运行时实现。
//
// 根包负责组合各信号、资源、采样器、处理器和导出器。具体传播协议位于
// propagation 子包，内部聚合与生命周期细节不暴露为公共契约。
package observesdk
