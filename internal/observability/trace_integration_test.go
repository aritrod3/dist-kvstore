package observability_test

import (
	"context"
	"testing"

	"github.com/aritrod3/kv-store/internal/cluster"
	"github.com/aritrod3/kv-store/internal/hashring"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/replication"
	"github.com/aritrod3/kv-store/internal/storage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDistributedTraceFlow(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := observability.NewOTELTracer(tp, "kvstore-test")
	propagator := propagation.TraceContext{}

	primary, err := storage.Open(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	defer primary.Close()
	replica, err := storage.Open(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open replica: %v", err)
	}
	defer replica.Close()

	manager, err := cluster.NewManager(8, 0, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	if err := manager.Register(hashring.Node{ID: "node-1"}); err != nil {
		t.Fatalf("register primary: %v", err)
	}
	if err := manager.Register(hashring.Node{ID: "node-2"}); err != nil {
		t.Fatalf("register replica: %v", err)
	}

	writer := replication.NewInMemoryReplicaRPC(map[string]*storage.Engine{
		"node-2": replica,
	}, tracer, propagator)
	replicator := replication.NewReplicator(primary, writer, 1, nil, tracer, nil, propagator)

	coordinator, err := cluster.NewCoordinator("node-1", manager, 3, replicator, nil, tracer)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}

	ctx, root := tracer.Start(context.Background(), "client.request")
	_, err = coordinator.Put(ctx, "k1", []byte("v1"), replication.WriteConsistencyStrong)
	root.End(err)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	rootSpan := findSpan(spans, "client.request")
	coordSpan := findSpan(spans, "coordinator.put")
	primarySpan := findSpan(spans, "replication.put")
	sendSpan := findSpan(spans, "replication.send")
	applySpan := findSpan(spans, "replica.apply")

	if rootSpan == nil || coordSpan == nil || primarySpan == nil || sendSpan == nil || applySpan == nil {
		t.Fatalf("expected full trace chain, got spans: %v", spanNames(spans))
	}
	if coordSpan.Parent.SpanID() != rootSpan.SpanContext.SpanID() {
		t.Fatalf("expected coordinator span to be child of client span")
	}
	if primarySpan.Parent.SpanID() != coordSpan.SpanContext.SpanID() {
		t.Fatalf("expected replication.put span to be child of coordinator span")
	}
	if sendSpan.Parent.SpanID() != primarySpan.SpanContext.SpanID() {
		t.Fatalf("expected replication.send span to be child of replication.put span")
	}
	if applySpan.Parent.SpanID() != sendSpan.SpanContext.SpanID() {
		t.Fatalf("expected replica.apply span to be child of replication.send span")
	}
}

func findSpan(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.Name)
	}
	return names
}
