package storage

import (
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	DataDir          string
	WALFile          string
	SnapshotFile     string
	Shards           int
	SnapshotEvery    uint64
	SnapshotInterval time.Duration
}

type Engine struct {
	shards           []shard
	wal              *WAL
	snap             *Snapshotter
	dataDir          string
	walPath          string
	snapPath         string
	seqMu            sync.Mutex
	seq              uint64
	snapshotMu       sync.Mutex
	lastSnapshotSeq  uint64
	snapshotEvery    uint64
	snapshotInterval time.Duration
	snapshotStop     chan struct{}
	snapshotDone     chan struct{}
}

type shard struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func Open(cfg Config) (*Engine, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("data dir required")
	}
	if cfg.WALFile == "" {
		cfg.WALFile = "wal.log"
	}
	if cfg.SnapshotFile == "" {
		cfg.SnapshotFile = "snapshot.bin"
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 16
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 1000
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(cfg.DataDir, cfg.WALFile)
	snapPath := filepath.Join(cfg.DataDir, cfg.SnapshotFile)

	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	snap := NewSnapshotter(snapPath)
	data, seq, err := snap.Load()
	if err != nil {
		wal.Close()
		return nil, err
	}
	engine := &Engine{
		shards:           make([]shard, cfg.Shards),
		wal:              wal,
		snap:             snap,
		dataDir:          cfg.DataDir,
		walPath:          walPath,
		snapPath:         snapPath,
		seq:              seq,
		lastSnapshotSeq:  seq,
		snapshotEvery:    cfg.SnapshotEvery,
		snapshotInterval: cfg.SnapshotInterval,
	}
	for i := range engine.shards {
		engine.shards[i] = shard{data: make(map[string]Entry)}
	}
	for key, entry := range data {
		sh := engine.shardFor(key)
		sh.mu.Lock()
		sh.data[key] = entry
		sh.mu.Unlock()
	}
	if err := wal.Replay(func(rec walRecord) error {
		engine.applyRecord(rec)
		engine.updateSeq(rec.Seq)
		return nil
	}); err != nil {
		wal.Close()
		return nil, err
	}
	engine.startSnapshotLoop()
	return engine, nil
}

func (e *Engine) Get(key string) (Entry, bool) {
	sh := e.shardFor(key)
	sh.mu.RLock()
	entry, ok := sh.data[key]
	sh.mu.RUnlock()
	if !ok || entry.Tombstone {
		return Entry{}, false
	}
	entry.Value = cloneBytes(entry.Value)
	return entry, true
}

func (e *Engine) GetEntry(key string) (Entry, bool) {
	sh := e.shardFor(key)
	sh.mu.RLock()
	entry, ok := sh.data[key]
	sh.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}
	entry.Value = cloneBytes(entry.Value)
	return entry, true
}

func (e *Engine) Put(key string, value []byte) (Entry, error) {
	if key == "" {
		return Entry{}, errors.New("key required")
	}
	now := time.Now().UnixMilli()
	seq := e.nextSeq()
	sh := e.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	prev := sh.data[key]
	entry := Entry{
		Value:               cloneBytes(value),
		Version:             prev.Version + 1,
		TimestampUnixMillis: now,
		Tombstone:           false,
	}
	rec := walRecord{
		Op:                  walOpPut,
		Key:                 key,
		Value:               cloneBytes(value),
		Version:             entry.Version,
		TimestampUnixMillis: now,
		Tombstone:           false,
		Seq:                 seq,
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, err
	}
	sh.data[key] = entry
	if err := e.maybeSnapshot(); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (e *Engine) Delete(key string) (Entry, error) {
	if key == "" {
		return Entry{}, errors.New("key required")
	}
	now := time.Now().UnixMilli()
	seq := e.nextSeq()
	sh := e.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	prev := sh.data[key]
	entry := Entry{
		Value:               nil,
		Version:             prev.Version + 1,
		TimestampUnixMillis: now,
		Tombstone:           true,
	}
	rec := walRecord{
		Op:                  walOpDelete,
		Key:                 key,
		Value:               nil,
		Version:             entry.Version,
		TimestampUnixMillis: now,
		Tombstone:           true,
		Seq:                 seq,
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, err
	}
	sh.data[key] = entry
	if err := e.maybeSnapshot(); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (e *Engine) ApplyRemote(key string, entry Entry) (Entry, bool, error) {
	if key == "" {
		return Entry{}, false, errors.New("key required")
	}
	sh := e.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	prev := sh.data[key]
	if prev.Version >= entry.Version {
		return prev, false, nil
	}
	seq := e.nextSeq()
	rec := walRecord{
		Op:                  walOpPut,
		Key:                 key,
		Value:               cloneBytes(entry.Value),
		Version:             entry.Version,
		TimestampUnixMillis: entry.TimestampUnixMillis,
		Tombstone:           entry.Tombstone,
		Seq:                 seq,
	}
	if entry.Tombstone {
		rec.Op = walOpDelete
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, false, err
	}
	entry.Value = cloneBytes(entry.Value)
	sh.data[key] = entry
	if err := e.maybeSnapshot(); err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

func (e *Engine) CreateSnapshot() error {
	copyData := make(map[string]Entry)
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.RLock()
		for k, v := range sh.data {
			v.Value = cloneBytes(v.Value)
			copyData[k] = v
		}
		sh.mu.RUnlock()
	}
	if err := e.snap.Save(copyData, e.LastSeq()); err != nil {
		return err
	}
	return e.wal.Reset()
}

func (e *Engine) Close() error {
	if e.snapshotStop != nil {
		close(e.snapshotStop)
		<-e.snapshotDone
	}
	return e.wal.Close()
}

func (e *Engine) WALPath() string {
	return e.walPath
}

func (e *Engine) SnapshotPath() string {
	return e.snapPath
}

func (e *Engine) WALSizeBytes() (int64, error) {
	info, err := os.Stat(e.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func (e *Engine) LastSeq() uint64 {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	return e.seq
}

func (e *Engine) CompactTombstones(ttl time.Duration, now time.Time) (int, error) {
	if ttl <= 0 {
		return 0, errors.New("ttl must be positive")
	}
	removed := 0
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.Lock()
		for key, entry := range sh.data {
			if !entry.Tombstone {
				continue
			}
			age := time.Duration(now.UnixMilli()-entry.TimestampUnixMillis) * time.Millisecond
			if age >= ttl {
				delete(sh.data, key)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	if removed == 0 {
		return 0, nil
	}
	if err := e.CreateSnapshot(); err != nil {
		return removed, err
	}
	e.setLastSnapshotSeq(e.LastSeq())
	return removed, nil
}

func (e *Engine) applyRecord(rec walRecord) {
	entry := Entry{
		Value:               cloneBytes(rec.Value),
		Version:             rec.Version,
		TimestampUnixMillis: rec.TimestampUnixMillis,
		Tombstone:           rec.Tombstone,
	}
	sh := e.shardFor(rec.Key)
	sh.mu.Lock()
	sh.data[rec.Key] = entry
	sh.mu.Unlock()
}

func (e *Engine) nextSeq() uint64 {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	e.seq++
	return e.seq
}

func (e *Engine) updateSeq(seq uint64) {
	if seq == 0 {
		return
	}
	e.seqMu.Lock()
	if seq > e.seq {
		e.seq = seq
	}
	e.seqMu.Unlock()
}

func (e *Engine) startSnapshotLoop() {
	if e.snapshotInterval <= 0 {
		return
	}
	e.snapshotStop = make(chan struct{})
	e.snapshotDone = make(chan struct{})
	ticker := time.NewTicker(e.snapshotInterval)
	go func() {
		defer close(e.snapshotDone)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = e.snapshotNow()
			case <-e.snapshotStop:
				return
			}
		}
	}()
}

func (e *Engine) maybeSnapshot() error {
	if e.snapshotEvery == 0 {
		return nil
	}
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	if e.LastSeq()-e.lastSnapshotSeq < e.snapshotEvery {
		return nil
	}
	if err := e.CreateSnapshot(); err != nil {
		return err
	}
	e.lastSnapshotSeq = e.LastSeq()
	return nil
}

func (e *Engine) snapshotNow() error {
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	if err := e.CreateSnapshot(); err != nil {
		return err
	}
	e.lastSnapshotSeq = e.LastSeq()
	return nil
}

func (e *Engine) setLastSnapshotSeq(seq uint64) {
	e.snapshotMu.Lock()
	if seq > e.lastSnapshotSeq {
		e.lastSnapshotSeq = seq
	}
	e.snapshotMu.Unlock()
}

func (e *Engine) shardFor(key string) *shard {
	idx := int(hashKey(key)) % len(e.shards)
	return &e.shards[idx]
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
