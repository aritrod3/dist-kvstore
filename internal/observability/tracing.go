package observability

import "context"

type Span interface {
	End(err error)
}

type Tracer interface {
	Start(ctx context.Context, name string) (context.Context, Span)
}

type NoopTracer struct{}

type noopSpan struct{}

func (NoopTracer) Start(ctx context.Context, name string) (context.Context, Span) {
	return ctx, noopSpan{}
}

func (noopSpan) End(err error) {}
