package consistency

import (
	"context"
	"errors"
	"sync"

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
}

func NewReadCoordinator(localReader func(key string) (storage.Entry, bool), reader ReplicaReader) *ReadCoordinator {
	return &ReadCoordinator{
		localReader: localReader,
		reader:      reader,
	}
}

func (r *ReadCoordinator) Get(ctx context.Context, key string, replicaIDs []string, mode ReadConsistency) (storage.Entry, bool, error) {
	if mode == ReadConsistencyLocal || len(replicaIDs) == 0 {
		return r.localReader(key)
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
