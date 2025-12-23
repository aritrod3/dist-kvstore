package observability

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OTELTracer struct {
	tracer trace.Tracer
}

type otelSpan struct {
	span trace.Span
}

func NewOTELTracer(provider trace.TracerProvider, name string) *OTELTracer {
	if provider == nil {
		provider = trace.NewNoopTracerProvider()
	}
	return &OTELTracer{tracer: provider.Tracer(name)}
}

func (t *OTELTracer) Start(ctx context.Context, name string) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, name)
	return ctx, otelSpan{span: span}
}

func (s otelSpan) End(err error) {
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
	s.span.End()
}
