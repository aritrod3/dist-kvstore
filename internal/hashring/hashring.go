package hashring

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type Node struct {
	ID      string
	Address string
}

type Ring struct {
	mu          sync.RWMutex
	replicas    int
	positions   []uint32
	posToNodeID map[uint32]string
	nodes       map[string]Node
}

func New(replicas int) (*Ring, error) {
	if replicas <= 0 {
		return nil, errors.New("replicas must be positive")
	}
	return &Ring{
		replicas:    replicas,
		positions:   []uint32{},
		posToNodeID: make(map[uint32]string),
		nodes:       make(map[string]Node),
	}, nil
}

func (r *Ring) AddNode(node Node) error {
	if node.ID == "" {
		return errors.New("node id required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodes[node.ID]; exists {
		return errors.New("node already exists")
	}
	r.nodes[node.ID] = node
	for i := 0; i < r.replicas; i++ {
		pos := hashPosition(node.ID + "-" + strconv.Itoa(i))
		r.positions = append(r.positions, pos)
		r.posToNodeID[pos] = node.ID
	}
	sort.Slice(r.positions, func(i, j int) bool { return r.positions[i] < r.positions[j] })
	return nil
}

func (r *Ring) RemoveNode(nodeID string) error {
	if nodeID == "" {
		return errors.New("node id required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodes[nodeID]; !exists {
		return errors.New("node not found")
	}
	delete(r.nodes, nodeID)
	positions := r.positions[:0]
	for _, pos := range r.positions {
		if r.posToNodeID[pos] == nodeID {
			delete(r.posToNodeID, pos)
			continue
		}
		positions = append(positions, pos)
	}
	r.positions = positions
	return nil
}

func (r *Ring) GetNode(key string) (Node, error) {
	if key == "" {
		return Node{}, errors.New("key required")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.positions) == 0 {
		return Node{}, errors.New("ring is empty")
	}
	pos := hashPosition(key)
	idx := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= pos })
	if idx == len(r.positions) {
		idx = 0
	}
	nodeID := r.posToNodeID[r.positions[idx]]
	node, ok := r.nodes[nodeID]
	if !ok {
		return Node{}, errors.New("node not found for key")
	}
	return node, nil
}

func (r *Ring) GetNodes(key string, count int) ([]Node, error) {
	if key == "" {
		return nil, errors.New("key required")
	}
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.positions) == 0 {
		return nil, errors.New("ring is empty")
	}
	if count > len(r.nodes) {
		count = len(r.nodes)
	}
	pos := hashPosition(key)
	start := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= pos })
	if start == len(r.positions) {
		start = 0
	}
	nodes := make([]Node, 0, count)
	seen := make(map[string]struct{})
	for i := 0; i < len(r.positions) && len(nodes) < count; i++ {
		idx := (start + i) % len(r.positions)
		nodeID := r.posToNodeID[r.positions[idx]]
		if _, ok := seen[nodeID]; ok {
			continue
		}
		node, ok := r.nodes[nodeID]
		if !ok {
			continue
		}
		seen[nodeID] = struct{}{}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return nil, errors.New("no nodes available")
	}
	return nodes, nil
}

func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func hashPosition(input string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(input))
	return h.Sum32()
}
