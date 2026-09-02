package observesdk

import (
	"context"

	"goark.dev/observe"
)

// ParentBased 返回尊重有效父级采样决定的采样器。
//
// 无父级时委托 root；已采样父级继续采样，未采样父级继续传播但不记录。
func ParentBased(root observe.Sampler) observe.Sampler {
	if root == nil {
		root = observe.AlwaysOnSampler()
	}
	return parentBasedSampler{root: root}
}

type parentBasedSampler struct{ root observe.Sampler }

func (s parentBasedSampler) ShouldSample(ctx context.Context, parameters observe.SamplingParameters) observe.SamplingResult {
	if !parameters.Parent.IsValid() {
		return s.root.ShouldSample(ctx, parameters)
	}
	if parameters.Parent.IsSampled() {
		return observe.SamplingResult{Decision: observe.SamplingRecordAndSample, TraceState: parameters.Parent.TraceState}
	}
	return observe.SamplingResult{Decision: observe.SamplingDrop, TraceState: parameters.Parent.TraceState}
}

func (parentBasedSampler) Description() string { return "parent_based" }
