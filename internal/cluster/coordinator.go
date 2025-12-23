package cluster

import (
	"context"
	"errors"

	"github.com/aritrod3/kv-store/internal/consistency"
	"github.com/aritrod3/kv-store/internal/hashring"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/replication"
	"github.com/aritrod3/kv-store/internal/storage"
)

var ErrNotPrimary = errors.New("not primary for key")

type Coordinator struct {
	nodeID            string
	manager           *Manager
	replicationFactor int
	replicator        *replication.Replicator
	reader            *consistency.ReadCoordinator
	tracer            observability.Tracer
}

func NewCoordinator(nodeID string, manager *Manager, replicationFactor int, replicator *replication.Replicator, reader *consistency.ReadCoordinator, tracer observability.Tracer) (*Coordinator, error) {
	if nodeID == "" {
		return nil, errors.New("node id required")
	}
	if manager == nil {
		return nil, errors.New("manager required")
	}
	if replicationFactor != 3 {
		return nil, errors.New("replication factor must be 3")
	}
	if tracer == nil {
		tracer = observability.NoopTracer{}
	}
	return &Coordinator{
		nodeID:            nodeID,
		manager:           manager,
		replicationFactor: replicationFactor,
		replicator:        replicator,
		reader:            reader,
		tracer:            tracer,
	}, nil
}

func (c *Coordinator) Put(ctx context.Context, key string, value []byte, mode replication.WriteConsistency) (storage.Entry, error) {
	ctx, span := c.tracer.Start(ctx, "coordinator.put")
	defer span.End(nil)
	primary, replicas, err := c.primaryAndReplicas(key)
	if err != nil {
		return storage.Entry{}, err
	}
	if primary.ID != c.nodeID {
		return storage.Entry{}, ErrNotPrimary
	}
	replicaIDs := nodeIDs(replicas, primary.ID)
	return c.replicator.Put(ctx, key, value, replicaIDs, mode)
}

func (c *Coordinator) Delete(ctx context.Context, key string, mode replication.WriteConsistency) (storage.Entry, error) {
	ctx, span := c.tracer.Start(ctx, "coordinator.delete")
	defer span.End(nil)
	primary, replicas, err := c.primaryAndReplicas(key)
	if err != nil {
		return storage.Entry{}, err
	}
	if primary.ID != c.nodeID {
		return storage.Entry{}, ErrNotPrimary
	}
	replicaIDs := nodeIDs(replicas, primary.ID)
	return c.replicator.Delete(ctx, key, replicaIDs, mode)
}

func (c *Coordinator) Get(ctx context.Context, key string, mode consistency.ReadConsistency) (storage.Entry, bool, error) {
	ctx, span := c.tracer.Start(ctx, "coordinator.get")
	defer span.End(nil)
	_, replicas, err := c.primaryAndReplicas(key)
	if err != nil {
		return storage.Entry{}, false, err
	}
	replicaIDs := nodeIDs(replicas, c.nodeID)
	return c.reader.Get(ctx, key, replicaIDs, mode)
}

func (c *Coordinator) primaryAndReplicas(key string) (hashring.Node, []hashring.Node, error) {
	nodes, err := c.manager.ReplicasForKey(key, c.replicationFactor)
	if err != nil {
		return hashring.Node{}, nil, err
	}
	if len(nodes) == 0 {
		return hashring.Node{}, nil, errors.New("no nodes available")
	}
	return nodes[0], nodes, nil
}

func nodeIDs(nodes []hashring.Node, exclude string) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == exclude {
			continue
		}
		ids = append(ids, node.ID)
	}
	return ids
}
