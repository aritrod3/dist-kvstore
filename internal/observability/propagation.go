package observability

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

type MetadataCarrier map[string]string

func (c MetadataCarrier) Get(key string) string {
	return c[strings.ToLower(key)]
}

func (c MetadataCarrier) Set(key string, value string) {
	c[strings.ToLower(key)] = value
}

func (c MetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func Inject(ctx context.Context, propagator propagation.TextMapPropagator, carrier MetadataCarrier) {
	if propagator == nil {
		return
	}
	propagator.Inject(ctx, carrier)
}

func Extract(ctx context.Context, propagator propagation.TextMapPropagator, carrier MetadataCarrier) context.Context {
	if propagator == nil {
		return ctx
	}
	return propagator.Extract(ctx, carrier)
}
