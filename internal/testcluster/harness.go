package testcluster

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aritrod3/kv-store/internal/cluster"
	"github.com/aritrod3/kv-store/internal/consistency"
	"github.com/aritrod3/kv-store/internal/hashring"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/replication"
	"github.com/aritrod3/kv-store/internal/storage"
	"go.opentelemetry.io/otel/propagation"
)

// WriterFactory builds a ReplicaWriter after engines exist (e.g. fault injection wrapping InMemoryReplicaRPC).
type WriterFactory func(engines map[string]*storage.Engine, tracer observability.Tracer, prop propagation.TextMapPropagator) replication.ReplicaWriter

const (
	Node1 = "node-1"
	Node2 = "node-2"
	Node3 = "node-3"
)

// Cluster is a local 3-node deployment with in-memory replication RPC.
type Cluster struct {
	Manager       *cluster.Manager
	Engines       map[string]*storage.Engine
	Coordinators  map[string]*cluster.Coordinator
	Replicators   map[string]*replication.Replicator
	ReplicaReader *EngineReader
	Quorum        *observability.QuorumRecorder
}

type EngineReader struct {
	Engines map[string]*storage.Engine
}

func (e *EngineReader) Read(ctx context.Context, nodeID string, key string) (storage.Entry, bool, error) {
	eng := e.Engines[nodeID]
	if eng == nil {
		return storage.Entry{}, false, fmt.Errorf("unknown node %q", nodeID)
	}
	entry, ok := eng.Get(key)
	return entry, ok, nil
}

type Options struct {
	Quorum *observability.QuorumRecorder
	// Writer overrides the default InMemoryReplicaRPC. Ignored if NewWriter is set.
	Writer replication.ReplicaWriter
	// NewWriter is called with the node engines map after Open creates them; overrides Writer.
	NewWriter WriterFactory
}

// Open creates three on-disk engines under dataDir, registers nodes, and wires coordinators.
func Open(dataDir string, opt *Options) (*Cluster, error) {
	if opt == nil {
		opt = &Options{}
	}
	manager, err := cluster.NewManager(64, 0, nil)
	if err != nil {
		return nil, err
	}
	ids := []string{Node1, Node2, Node3}
	engines := make(map[string]*storage.Engine)
	for _, id := range ids {
		dir := filepath.Join(dataDir, id)
		// Avoid maybeSnapshot during Put: Put holds the shard Lock and CreateSnapshot
		// takes RLock per shard — same goroutine would deadlock. Large SnapshotEvery for bench harness only.
		eng, err := storage.Open(storage.Config{DataDir: dir, SnapshotEvery: 1 << 50})
		if err != nil {
			for _, e := range engines {
				_ = e.Close()
			}
			return nil, err
		}
		engines[id] = eng
	}
	for _, id := range ids {
		if err := manager.Register(hashring.Node{ID: id}); err != nil {
			for _, e := range engines {
				_ = e.Close()
			}
			return nil, err
		}
	}
	prop := propagation.TraceContext{}
	tracer := observability.NoopTracer{}
	var writer replication.ReplicaWriter
	switch {
	case opt.NewWriter != nil:
		writer = opt.NewWriter(engines, tracer, prop)
	case opt.Writer != nil:
		writer = opt.Writer
	default:
		writer = replication.NewInMemoryReplicaRPC(engines, tracer, prop)
	}
	return assembleCluster(manager, engines, writer, tracer, prop, opt)
}

func assembleCluster(manager *cluster.Manager, engines map[string]*storage.Engine, writer replication.ReplicaWriter, tracer observability.Tracer, prop propagation.TextMapPropagator, opt *Options) (*Cluster, error) {
	if opt == nil {
		opt = &Options{}
	}
	ids := []string{Node1, Node2, Node3}
	reader := &EngineReader{Engines: engines}
	coords := make(map[string]*cluster.Coordinator)
	repls := make(map[string]*replication.Replicator)
	for _, id := range ids {
		localID := id
		rep := replication.NewReplicator(engines[localID], writer, 8, nil, tracer, nil, prop, opt.Quorum)
		rc := consistency.NewReadCoordinator(
			func(key string) (storage.Entry, bool) { return engines[localID].Get(key) },
			reader,
			nil,
			opt.Quorum,
		)
		coord, err := cluster.NewCoordinator(localID, manager, 3, rep, rc, tracer)
		if err != nil {
			for _, e := range engines {
				_ = e.Close()
			}
			return nil, err
		}
		coords[localID] = coord
		repls[localID] = rep
	}
	return &Cluster{
		Manager:       manager,
		Engines:       engines,
		Coordinators:  coords,
		Replicators:   repls,
		ReplicaReader: reader,
		Quorum:        opt.Quorum,
	}, nil
}

func (c *Cluster) Close() {
	for _, r := range c.Replicators {
		r.Close()
	}
	for _, e := range c.Engines {
		_ = e.Close()
	}
}

// PrimaryCoordinator returns the coordinator that must execute writes for key.
func (c *Cluster) PrimaryCoordinator(key string) (*cluster.Coordinator, error) {
	n, err := c.Manager.PrimaryForKey(key)
	if err != nil {
		return nil, err
	}
	return c.Coordinators[n.ID], nil
}

// FindKeyForPrimary returns a key whose primary is primaryID (linear scan).
func FindKeyForPrimary(manager *cluster.Manager, primaryID string) (string, error) {
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		p, err := manager.PrimaryForKey(key)
		if err != nil {
			return "", err
		}
		if p.ID == primaryID {
			return key, nil
		}
	}
	return "", fmt.Errorf("no key found for primary %s", primaryID)
}

// FindKeysForPrimary returns n distinct keys that hash to the given primary.
func FindKeysForPrimary(manager *cluster.Manager, primaryID string, n int) ([]string, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive")
	}
	out := make([]string, 0, n)
	for i := 0; i < 500000 && len(out) < n; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		p, err := manager.PrimaryForKey(key)
		if err != nil {
			return nil, err
		}
		if p.ID == primaryID {
			out = append(out, key)
		}
	}
	if len(out) < n {
		return nil, fmt.Errorf("only found %d keys for primary %s", len(out), primaryID)
	}
	return out, nil
}
