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
	DataDir      string
	WALFile      string
	SnapshotFile string
	Shards       int
}

type Engine struct {
	shards   []shard
	wal      *WAL
	snap     *Snapshotter
	dataDir  string
	walPath  string
	snapPath string
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
	data, err := snap.Load()
	if err != nil {
		wal.Close()
		return nil, err
	}
	engine := &Engine{
		shards:   make([]shard, cfg.Shards),
		wal:      wal,
		snap:     snap,
		dataDir:  cfg.DataDir,
		walPath:  walPath,
		snapPath: snapPath,
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
		return nil
	}); err != nil {
		wal.Close()
		return nil, err
	}
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
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, err
	}
	sh.data[key] = entry
	return entry, nil
}

func (e *Engine) Delete(key string) (Entry, error) {
	if key == "" {
		return Entry{}, errors.New("key required")
	}
	now := time.Now().UnixMilli()
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
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, err
	}
	sh.data[key] = entry
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
	rec := walRecord{
		Op:                  walOpPut,
		Key:                 key,
		Value:               cloneBytes(entry.Value),
		Version:             entry.Version,
		TimestampUnixMillis: entry.TimestampUnixMillis,
		Tombstone:           entry.Tombstone,
	}
	if entry.Tombstone {
		rec.Op = walOpDelete
	}
	if err := e.wal.Append(rec); err != nil {
		return Entry{}, false, err
	}
	entry.Value = cloneBytes(entry.Value)
	sh.data[key] = entry
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
	if err := e.snap.Save(copyData); err != nil {
		return err
	}
	return e.wal.Reset()
}

func (e *Engine) Close() error {
	return e.wal.Close()
}

func (e *Engine) WALPath() string {
	return e.walPath
}

func (e *Engine) SnapshotPath() string {
	return e.snapPath
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
