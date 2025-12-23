package replication

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aritrod3/kv-store/internal/metrics"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/storage"
)

var ErrWriteQuorumFailed = errors.New("write quorum not reached")

type ReplicaWriter interface {
	ReplicatePut(ctx context.Context, nodeID string, key string, entry storage.Entry) error
	ReplicateDelete(ctx context.Context, nodeID string, key string, entry storage.Entry) error
}

type Replicator struct {
	local   *storage.Engine
	writer  ReplicaWriter
	workers int

	queue chan func()
	wg    sync.WaitGroup

	metrics metrics.Recorder
	tracer  observability.Tracer
	logger  observability.Logger
}

func NewReplicator(local *storage.Engine, writer ReplicaWriter, workers int, metrics metrics.Recorder, tracer observability.Tracer, logger observability.Logger) *Replicator {
	if workers <= 0 {
		workers = 4
	}
	if tracer == nil {
		tracer = observability.NoopTracer{}
	}
	r := &Replicator{
		local:   local,
		writer:  writer,
		workers: workers,
		queue:   make(chan func(), 1024),
		metrics: metrics,
		tracer:  tracer,
		logger:  logger,
	}
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.worker()
	}
	return r
}

func (r *Replicator) Close() {
	close(r.queue)
	r.wg.Wait()
}

func (r *Replicator) Pending() int {
	return len(r.queue)
}

func (r *Replicator) Put(ctx context.Context, key string, value []byte, replicaIDs []string, mode WriteConsistency) (storage.Entry, error) {
	var err error
	start := time.Now()
	ctx, span := r.tracer.Start(ctx, "replication.put")
	defer func(errp *error) {
		if r.metrics != nil {
			r.metrics.ObserveLatency("put", time.Since(start))
			if errp != nil && *errp != nil {
				r.metrics.IncFailedWrites("put")
			}
		}
		if errp != nil {
			span.End(*errp)
			return
		}
		span.End(nil)
	}(&err)
	entry, err := r.local.Put(key, value)
	if err != nil {
		return storage.Entry{}, err
	}
	if r.metrics != nil {
		if size, sizeErr := r.local.WALSizeBytes(); sizeErr == nil {
			r.metrics.SetWALSizeBytes(size)
		}
	}
	switch mode {
	case WriteConsistencyStrong:
		if err := r.replicateSync(ctx, key, entry, replicaIDs, false); err != nil {
			return storage.Entry{}, err
		}
	default:
		r.replicateAsync(key, entry, replicaIDs, false)
	}
	return entry, nil
}

func (r *Replicator) Delete(ctx context.Context, key string, replicaIDs []string, mode WriteConsistency) (storage.Entry, error) {
	var err error
	start := time.Now()
	ctx, span := r.tracer.Start(ctx, "replication.delete")
	defer func(errp *error) {
		if r.metrics != nil {
			r.metrics.ObserveLatency("delete", time.Since(start))
			if errp != nil && *errp != nil {
				r.metrics.IncFailedWrites("delete")
			}
		}
		if errp != nil {
			span.End(*errp)
			return
		}
		span.End(nil)
	}(&err)
	entry, err := r.local.Delete(key)
	if err != nil {
		return storage.Entry{}, err
	}
	if r.metrics != nil {
		if size, sizeErr := r.local.WALSizeBytes(); sizeErr == nil {
			r.metrics.SetWALSizeBytes(size)
		}
	}
	switch mode {
	case WriteConsistencyStrong:
		if err := r.replicateSync(ctx, key, entry, replicaIDs, true); err != nil {
			return storage.Entry{}, err
		}
	default:
		r.replicateAsync(key, entry, replicaIDs, true)
	}
	return entry, nil
}

func (r *Replicator) replicateAsync(key string, entry storage.Entry, replicaIDs []string, tombstone bool) {
	for _, id := range replicaIDs {
		replicaID := id
		r.queue <- func() {
			ctx := context.Background()
			if tombstone {
				_ = r.writer.ReplicateDelete(ctx, replicaID, key, entry)
				return
			}
			_ = r.writer.ReplicatePut(ctx, replicaID, key, entry)
		}
	}
	r.updateLag()
}

func (r *Replicator) replicateSync(ctx context.Context, key string, entry storage.Entry, replicaIDs []string, tombstone bool) error {
	total := len(replicaIDs) + 1
	required := total/2 + 1
	acks := 1
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range replicaIDs {
		replicaID := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if tombstone {
				err = r.writer.ReplicateDelete(ctx, replicaID, key, entry)
			} else {
				err = r.writer.ReplicatePut(ctx, replicaID, key, entry)
			}
			if err == nil {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if acks < required {
		return ErrWriteQuorumFailed
	}
	return nil
}

func (r *Replicator) worker() {
	defer r.wg.Done()
	for task := range r.queue {
		task()
		r.updateLag()
	}
}

func (r *Replicator) updateLag() {
	if r.metrics != nil {
		r.metrics.SetReplicationLag(len(r.queue))
	}
}
