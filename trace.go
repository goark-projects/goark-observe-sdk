package observesdk

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"reflect"
	"sync"
	"time"

	"goark.dev/observe"
)

const maxSpanCollectionSize = 128

type tracer struct {
	provider *Provider
	scope    observe.Scope
}

func (t *tracer) Start(ctx context.Context, name string, options ...observe.SpanStartOption) (context.Context, observe.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil || t.provider.ensureActive() != nil {
		return ctx, observe.NoopSpan()
	}
	config := observe.NewSpanStartConfig(options...)
	parent := observe.SpanContextFromContext(ctx)
	if config.NewRoot {
		parent = observe.SpanContext{}
	}
	traceID := parent.TraceID
	if !traceID.IsValid() {
		traceID = t.provider.nextTraceID()
	}
	result := t.provider.sampler.ShouldSample(ctx, observe.SamplingParameters{
		Parent: parent, TraceID: traceID, Name: name, Kind: config.Kind,
		Attributes: observe.CloneAttrs(config.Attributes), Links: cloneLinks(config.Links),
	})
	spanContext := observe.NewSpanContext(traceID, t.provider.nextSpanID(), observe.TraceFlagsNone, result.TraceState, false)
	if result.Decision == observe.SamplingRecordAndSample {
		spanContext.TraceFlags = observe.TraceFlagsSampled
	}
	if result.Decision == observe.SamplingDrop {
		span := nonRecordingSpan{spanContext: spanContext}
		return observe.ContextWithSpan(ctx, span), span
	}
	startTime := config.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}
	attrs := append(observe.CloneAttrs(config.Attributes), result.Attributes...)
	span := &recordingSpan{
		provider: t.provider, resource: t.provider.resource.Clone(), scope: t.scope.Clone(), name: name,
		spanContext: spanContext, parent: parent, kind: config.Kind, startTime: startTime,
		attributes: attrs, links: cloneLinks(config.Links),
	}
	for _, processor := range t.provider.spanProcessors {
		processor.OnStart(ctx, span)
	}
	return observe.ContextWithSpan(ctx, span), span
}

type nonRecordingSpan struct{ spanContext observe.SpanContext }

func (s nonRecordingSpan) SpanContext() observe.SpanContext   { return s.spanContext }
func (nonRecordingSpan) IsRecording() bool                    { return false }
func (nonRecordingSpan) SetName(string)                       {}
func (nonRecordingSpan) SetStatus(observe.StatusCode, string) {}
func (nonRecordingSpan) SetAttrs(...observe.Attr)             {}
func (nonRecordingSpan) AddEvent(string, ...observe.Attr)     {}
func (nonRecordingSpan) AddEventRecord(observe.SpanEvent)     {}
func (nonRecordingSpan) RecordError(error, ...observe.Attr)   {}
func (nonRecordingSpan) End(...observe.SpanEndOption)         {}

type recordingSpan struct {
	mu                sync.Mutex
	provider          *Provider
	resource          observe.Resource
	scope             observe.Scope
	name              string
	spanContext       observe.SpanContext
	parent            observe.SpanContext
	kind              observe.SpanKind
	startTime         time.Time
	status            observe.SpanStatus
	attributes        []observe.Attr
	events            []observe.SpanEvent
	links             []observe.SpanLink
	droppedAttributes int
	droppedEvents     int
	droppedLinks      int
	ended             bool
}

func (s *recordingSpan) SpanContext() observe.SpanContext {
	if s == nil {
		return observe.SpanContext{}
	}
	return s.spanContext
}
func (s *recordingSpan) IsRecording() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.ended
}
func (s *recordingSpan) SetName(name string) { s.update(func() { s.name = name }) }
func (s *recordingSpan) SetStatus(code observe.StatusCode, description string) {
	s.update(func() { s.status = observe.SpanStatus{Code: code, Description: description} })
}
func (s *recordingSpan) SetAttrs(attrs ...observe.Attr) {
	s.update(func() { s.attributes = appendBoundedAttrs(s.attributes, attrs, &s.droppedAttributes) })
}
func (s *recordingSpan) AddEvent(name string, attrs ...observe.Attr) {
	s.AddEventRecord(observe.SpanEvent{Name: name, Attributes: attrs})
}
func (s *recordingSpan) AddEventRecord(event observe.SpanEvent) {
	s.update(func() {
		if len(s.events) >= maxSpanCollectionSize {
			s.droppedEvents++
			return
		}
		if event.Time.IsZero() {
			event.Time = time.Now()
		}
		s.events = append(s.events, event.Clone())
	})
}
func (s *recordingSpan) RecordError(err error, attrs ...observe.Attr) {
	if err == nil {
		return
	}
	typeName := reflect.TypeOf(err).String()
	attrs = append(attrs, observe.String(observe.AttrErrorType, typeName), observe.String(observe.AttrErrorMessage, err.Error()))
	s.AddEvent("exception", attrs...)
	s.SetStatus(observe.StatusError, err.Error())
}
func (s *recordingSpan) End(options ...observe.SpanEndOption) {
	if s == nil {
		return
	}
	config := observe.NewSpanEndConfig(options...)
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	endTime := config.EndTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	snapshot := observe.SpanSnapshot{
		Resource: s.resource.Clone(), Scope: s.scope.Clone(), Name: s.name, SpanContext: s.spanContext,
		Parent: s.parent, Kind: s.kind, StartTime: s.startTime, EndTime: endTime, Status: s.status,
		Attributes: observe.CloneAttrs(s.attributes), Events: cloneEvents(s.events), Links: cloneLinks(s.links),
		DroppedAttributes: s.droppedAttributes, DroppedEvents: s.droppedEvents, DroppedLinks: s.droppedLinks,
	}
	s.mu.Unlock()
	ctx := observe.ContextWithSpanContext(context.Background(), snapshot.SpanContext)
	for _, processor := range s.provider.spanProcessors {
		processor.OnEnd(ctx, snapshot)
	}
	if len(s.provider.spanExporters) > 0 {
		batch := []observe.SpanSnapshot{snapshot}
		for _, exporter := range s.provider.spanExporters {
			if err := exporter.ExportSpans(ctx, batch); err != nil {
				s.provider.report(ctx, err)
			}
		}
	}
}
func (s *recordingSpan) update(fn func()) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		fn()
	}
}

func appendBoundedAttrs(target []observe.Attr, attrs []observe.Attr, dropped *int) []observe.Attr {
	remaining := maxSpanCollectionSize - len(target)
	if remaining <= 0 {
		*dropped += len(attrs)
		return target
	}
	if len(attrs) > remaining {
		*dropped += len(attrs) - remaining
		attrs = attrs[:remaining]
	}
	return append(target, observe.CloneAttrs(attrs)...)
}

func cloneEvents(source []observe.SpanEvent) []observe.SpanEvent {
	if len(source) == 0 {
		return nil
	}
	out := make([]observe.SpanEvent, len(source))
	for i := range source {
		out[i] = source[i].Clone()
	}
	return out
}
func cloneLinks(source []observe.SpanLink) []observe.SpanLink {
	if len(source) == 0 {
		return nil
	}
	out := make([]observe.SpanLink, len(source))
	for i := range source {
		out[i] = source[i].Clone()
	}
	return out
}

func randomSeed() uint64 {
	var buffer [8]byte
	if _, err := rand.Read(buffer[:]); err == nil {
		return binary.LittleEndian.Uint64(buffer[:])
	}
	return uint64(time.Now().UnixNano())
}
func (p *Provider) nextUint64() uint64 {
	value := p.idCounter.Add(1) + p.idSeed
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
func (p *Provider) nextTraceID() observe.TraceID {
	var id observe.TraceID
	binary.LittleEndian.PutUint64(id[:8], p.nextUint64())
	binary.LittleEndian.PutUint64(id[8:], p.nextUint64())
	return id
}
func (p *Provider) nextSpanID() observe.SpanID {
	var id observe.SpanID
	binary.LittleEndian.PutUint64(id[:], p.nextUint64())
	return id
}
