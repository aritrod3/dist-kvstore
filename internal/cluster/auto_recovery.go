package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aritrod3/kv-store/internal/storage"
)

type EngineProvider interface {
	Engine(nodeID string) (*storage.Engine, bool)
}

type MapEngineProvider map[string]*storage.Engine

func (m MapEngineProvider) Engine(nodeID string) (*storage.Engine, bool) {
	engine, ok := m[nodeID]
	return engine, ok
}

type AutoRecovery struct {
	manager      *Manager
	recovery     *RecoveryManager
	engines      EngineProvider
	primaryID    string
	lagThreshold uint64
	interval     time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewAutoRecovery(manager *Manager, recovery *RecoveryManager, engines EngineProvider, primaryID string, lagThreshold uint64, interval time.Duration) (*AutoRecovery, error) {
	if manager == nil {
		return nil, errors.New("manager required")
	}
	if recovery == nil {
		return nil, errors.New("recovery manager required")
	}
	if engines == nil {
		return nil, errors.New("engine provider required")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ar := &AutoRecovery{
		manager:      manager,
		recovery:     recovery,
		engines:      engines,
		primaryID:    primaryID,
		lagThreshold: lagThreshold,
		interval:     interval,
		stopCh:       make(chan struct{}),
	}
	manager.SetOnRejoin(ar.onRejoin)
	return ar, nil
}

func (a *AutoRecovery) Start() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = a.CheckLag()
			case <-a.stopCh:
				return
			}
		}
	}()
}

func (a *AutoRecovery) Stop() {
	close(a.stopCh)
	a.wg.Wait()
}

func (a *AutoRecovery) onRejoin(nodeID string) {
	_ = a.catchUp(nodeID)
}

func (a *AutoRecovery) CheckLag() error {
	primaryID := a.primaryID
	if primaryID == "" {
		alive := a.manager.AliveNodes()
		if len(alive) == 0 {
			return errors.New("no alive nodes")
		}
		primaryID = alive[0].ID
	}
	primary, ok := a.engines.Engine(primaryID)
	if !ok {
		return errors.New("primary engine not found")
	}
	primarySeq := primary.LastSeq()

	for _, node := range a.manager.AliveNodes() {
		if node.ID == primaryID {
			continue
		}
		replica, ok := a.engines.Engine(node.ID)
		if !ok {
			continue
		}
		replicaSeq := replica.LastSeq()
		if primarySeq > replicaSeq && (primarySeq-replicaSeq) > a.lagThreshold {
			if err := a.catchUp(node.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *AutoRecovery) catchUp(targetID string) error {
	primaryID := a.primaryID
	if primaryID == "" {
		alive := a.manager.AliveNodes()
		for _, node := range alive {
			if node.ID != targetID {
				primaryID = node.ID
				break
			}
		}
	}
	if primaryID == "" {
		return errors.New("no source node available")
	}
	source, ok := a.engines.Engine(primaryID)
	if !ok {
		return errors.New("source engine not found")
	}
	target, ok := a.engines.Engine(targetID)
	if !ok {
		return errors.New("target engine not found")
	}
	if err := source.CreateSnapshot(); err != nil {
		return err
	}
	return a.recovery.CatchUpReplica(
		context.Background(),
		source.SnapshotPath(),
		source.WALPath(),
		target,
	)
}
