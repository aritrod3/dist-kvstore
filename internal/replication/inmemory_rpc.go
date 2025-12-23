package replication

import (
	"context"
	"errors"

	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/storage"
	"go.opentelemetry.io/otel/propagation"
)

type InMemoryReplicaRPC struct {
	engines    map[string]*storage.Engine
	tracer     observability.Tracer
	propagator propagation.TextMapPropagator
}

func NewInMemoryReplicaRPC(engines map[string]*storage.Engine, tracer observability.Tracer, propagator propagation.TextMapPropagator) *InMemoryReplicaRPC {
	if tracer == nil {
		tracer = observability.NoopTracer{}
	}
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}
	return &InMemoryReplicaRPC{
		engines:    engines,
		tracer:     tracer,
		propagator: propagator,
	}
}

func (r *InMemoryReplicaRPC) ReplicatePut(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	return r.ReplicatePutWithMeta(ctx, nodeID, key, entry, nil)
}

func (r *InMemoryReplicaRPC) ReplicateDelete(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	return r.ReplicateDeleteWithMeta(ctx, nodeID, key, entry, nil)
}

func (r *InMemoryReplicaRPC) ReplicatePutWithMeta(ctx context.Context, nodeID string, key string, entry storage.Entry, meta map[string]string) error {
	return r.applyWithMeta(ctx, nodeID, key, entry, meta)
}

func (r *InMemoryReplicaRPC) ReplicateDeleteWithMeta(ctx context.Context, nodeID string, key string, entry storage.Entry, meta map[string]string) error {
	return r.applyWithMeta(ctx, nodeID, key, entry, meta)
}

func (r *InMemoryReplicaRPC) applyWithMeta(ctx context.Context, nodeID string, key string, entry storage.Entry, meta map[string]string) error {
	engine, ok := r.engines[nodeID]
	if !ok {
		return errors.New("replica engine not found")
	}
	if meta != nil {
		ctx = observability.Extract(ctx, r.propagator, observability.MetadataCarrier(meta))
	}
	ctx, span := r.tracer.Start(ctx, "replica.apply")
	_, _, err := engine.ApplyRemote(key, entry)
	span.End(err)
	return err
}
