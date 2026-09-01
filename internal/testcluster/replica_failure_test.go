package testcluster

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aritrod3/kv-store/internal/cluster"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/replication"
	"github.com/aritrod3/kv-store/internal/storage"
	"go.opentelemetry.io/otel/propagation"
)

// dropReplicaWriter drops replication to selected nodes (simulates a dead/paused replica).
type dropReplicaWriter struct {
	inner replication.ReplicaWriterWithMeta
	drop  func(nodeID string) bool
}

func (d *dropReplicaWriter) ReplicatePut(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	return d.ReplicatePutWithMeta(ctx, nodeID, key, entry, nil)
}

func (d *dropReplicaWriter) ReplicateDelete(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	return d.ReplicateDeleteWithMeta(ctx, nodeID, key, entry, nil)
}

func (d *dropReplicaWriter) ReplicatePutWithMeta(ctx context.Context, nodeID string, key string, entry storage.Entry, meta map[string]string) error {
	if d.drop(nodeID) {
		return nil
	}
	return d.inner.ReplicatePutWithMeta(ctx, nodeID, key, entry, meta)
}

func (d *dropReplicaWriter) ReplicateDeleteWithMeta(ctx context.Context, nodeID string, key string, entry storage.Entry, meta map[string]string) error {
	if d.drop(nodeID) {
		return nil
	}
	return d.inner.ReplicateDeleteWithMeta(ctx, nodeID, key, entry, meta)
}

func TestReplicaFailureAndCatchUp(t *testing.T) {
	var drop atomic.Bool
	drop.Store(true)

	c, err := Open(filepath.Join(t.TempDir(), "c"), &Options{
		NewWriter: func(engines map[string]*storage.Engine, tracer observability.Tracer, prop propagation.TextMapPropagator) replication.ReplicaWriter {
			inner := replication.NewInMemoryReplicaRPC(engines, tracer, prop)
			return &dropReplicaWriter{
				inner: inner,
				drop: func(nodeID string) bool {
					return drop.Load() && nodeID == Node3
				},
			}
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	key, err := FindKeyForPrimary(c.Manager, Node1)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	coord, err := c.PrimaryCoordinator(key)
	if err != nil {
		t.Fatalf("coord: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		v := []byte{byte(i)}
		if _, err := coord.Put(ctx, key, v, replication.WriteConsistencyFast); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	var e1 storage.Entry
	var ok1 bool
	for time.Now().Before(deadline) {
		e1, ok1 = c.Engines[Node1].Get(key)
		e2, ok2 := c.Engines[Node2].Get(key)
		if ok1 && ok2 && len(e1.Value) > 0 && len(e2.Value) > 0 && e1.Value[0] == e2.Value[0] {
			break
		}
		time.Sleep(500 * time.Microsecond)
	}
	e2, ok2 := c.Engines[Node2].Get(key)
	if !ok1 || !ok2 || e1.Value[0] != e2.Value[0] {
		t.Fatalf("primary and healthy replica diverged ok1=%v ok2=%v v1=%v v2=%v", ok1, ok2, e1.Value, e2.Value)
	}
	if e3, ok3 := c.Engines[Node3].Get(key); ok3 && e3.Value[0] == e1.Value[0] {
		t.Fatalf("expected stalled replica to lag; got matching value")
	}

	drop.Store(false)
	recovery := &cluster.RecoveryManager{}
	auto, err := cluster.NewAutoRecovery(c.Manager, recovery, cluster.MapEngineProvider(c.Engines), Node1, 0, time.Second)
	if err != nil {
		t.Fatalf("auto recovery: %v", err)
	}

	start := time.Now()
	if err := auto.CheckLag(); err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	catchUp := time.Since(start)

	e3, ok3 := c.Engines[Node3].Get(key)
	if !ok3 || e3.Value[0] != e1.Value[0] {
		t.Fatalf("after catch-up: node3=%v ok=%v want %v", e3, ok3, e1.Value[0])
	}
	t.Logf("catch_up_duration_ms=%.3f", catchUp.Seconds()*1000)

	for _, id := range []string{Node1, Node2, Node3} {
		e, ok := c.Engines[id].Get(key)
		if !ok {
			t.Fatalf("node %s missing key", id)
		}
		if string(e.Value) != string(e1.Value) {
			t.Fatalf("node %s value %q != %q", id, e.Value, e1.Value)
		}
	}
}
