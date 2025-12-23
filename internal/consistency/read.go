package consistency

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aritrod3/kv-store/internal/metrics"
	"github.com/aritrod3/kv-store/internal/storage"
)

type ReadConsistency int

const (
	ReadConsistencyUnspecified ReadConsistency = iota
	ReadConsistencyLocal
	ReadConsistencyQuorum
)

var ErrReadQuorumFailed = errors.New("read quorum not reached")

type ReplicaReader interface {
	Read(ctx context.Context, nodeID string, key string) (storage.Entry, bool, error)
}

type ReadCoordinator struct {
	localReader func(key string) (storage.Entry, bool)
	reader      ReplicaReader
	metrics     metrics.Recorder
}

func NewReadCoordinator(localReader func(key string) (storage.Entry, bool), reader ReplicaReader, metrics metrics.Recorder) *ReadCoordinator {
	return &ReadCoordinator{
		localReader: localReader,
		reader:      reader,
		metrics:     metrics,
	}
}

func (r *ReadCoordinator) Get(ctx context.Context, key string, replicaIDs []string, mode ReadConsistency) (storage.Entry, bool, error) {
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.ObserveLatency("get", time.Since(start))
		}
	}()
	if mode == ReadConsistencyLocal || len(replicaIDs) == 0 {
		entry, ok := r.localReader(key)
		return entry, ok, nil
	}
	total := len(replicaIDs) + 1
	required := total/2 + 1
	responses := 1
	entry, ok := r.localReader(key)
	best := entry
	found := ok

	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range replicaIDs {
		replicaID := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, ok, err := r.reader.Read(ctx, replicaID, key)
			if err != nil {
				return
			}
			mu.Lock()
			responses++
			if ok {
				if !found || newer(e, best) {
					best = e
					found = true
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if responses < required {
		return storage.Entry{}, false, ErrReadQuorumFailed
	}
	if !found || best.Tombstone {
		return best, false, nil
	}
	return best, true, nil
}

func newer(candidate storage.Entry, current storage.Entry) bool {
	if candidate.Version != current.Version {
		return candidate.Version > current.Version
	}
	return candidate.TimestampUnixMillis > current.TimestampUnixMillis
}
