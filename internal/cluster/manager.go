package cluster

import (
	"errors"
	"sync"
	"time"

	"github.com/aritrod3/kv-store/internal/hashring"
	"github.com/aritrod3/kv-store/internal/metrics"
)

type NodeState struct {
	Node          hashring.Node
	LastHeartbeat time.Time
	Alive         bool
}

type Manager struct {
	mu               sync.RWMutex
	ring             *hashring.Ring
	nodes            map[string]*NodeState
	heartbeatTimeout time.Duration
	metrics          metrics.Recorder
	onRejoin         func(nodeID string)
}

func NewManager(virtualNodes int, heartbeatTimeout time.Duration, metrics metrics.Recorder) (*Manager, error) {
	ring, err := hashring.New(virtualNodes)
	if err != nil {
		return nil, err
	}
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 5 * time.Second
	}
	return &Manager{
		ring:             ring,
		nodes:            make(map[string]*NodeState),
		heartbeatTimeout: heartbeatTimeout,
		metrics:          metrics,
	}, nil
}

func (m *Manager) Register(node hashring.Node) error {
	return m.RegisterAt(node, time.Now())
}

func (m *Manager) RegisterAt(node hashring.Node, now time.Time) error {
	if node.ID == "" {
		return errors.New("node id required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[node.ID]; ok {
		return errors.New("node already registered")
	}
	if err := m.ring.AddNode(node); err != nil {
		return err
	}
	m.nodes[node.ID] = &NodeState{
		Node:          node,
		LastHeartbeat: now,
		Alive:         true,
	}
	if m.metrics != nil {
		m.metrics.SetNodeAlive(node.ID, true)
	}
	return nil
}

func (m *Manager) UpdateHeartbeat(nodeID string) error {
	return m.UpdateHeartbeatAt(nodeID, time.Now())
}

func (m *Manager) UpdateHeartbeatAt(nodeID string, now time.Time) error {
	if nodeID == "" {
		return errors.New("node id required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.nodes[nodeID]
	if !ok {
		return errors.New("node not registered")
	}
	state.LastHeartbeat = now
	if !state.Alive {
		state.Alive = true
		_ = m.ring.AddNode(state.Node)
		if m.metrics != nil {
			m.metrics.SetNodeAlive(state.Node.ID, true)
		}
		if m.onRejoin != nil {
			m.onRejoin(nodeID)
		}
	}
	return nil
}

func (m *Manager) SetOnRejoin(fn func(nodeID string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRejoin = fn
}

func (m *Manager) DetectFailures(now time.Time) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var failed []string
	for id, state := range m.nodes {
		if !state.Alive {
			continue
		}
		if now.Sub(state.LastHeartbeat) > m.heartbeatTimeout {
			state.Alive = false
			_ = m.ring.RemoveNode(id)
			if m.metrics != nil {
				m.metrics.SetNodeAlive(id, false)
			}
			failed = append(failed, id)
		}
	}
	return failed
}

func (m *Manager) PrimaryForKey(key string) (hashring.Node, error) {
	nodes, err := m.ring.GetNodes(key, 1)
	if err != nil {
		return hashring.Node{}, err
	}
	return nodes[0], nil
}

func (m *Manager) ReplicasForKey(key string, count int) ([]hashring.Node, error) {
	return m.ring.GetNodes(key, count)
}

func (m *Manager) AliveNodes() []hashring.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]hashring.Node, 0, len(m.nodes))
	for _, state := range m.nodes {
		if state.Alive {
			nodes = append(nodes, state.Node)
		}
	}
	return nodes
}
