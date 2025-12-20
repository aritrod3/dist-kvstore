package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	DataDir      string
	WALFile      string
	SnapshotFile string
}

type Engine struct {
	mu   sync.RWMutex
	data map[string]Entry
	wal  *WAL
	snap *Snapshotter
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
		data: data,
		wal:  wal,
		snap: snap,
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, ok := e.data[key]
	if !ok || entry.Tombstone {
		return Entry{}, false
	}
	entry.Value = cloneBytes(entry.Value)
	return entry, true
}

func (e *Engine) GetEntry(key string) (Entry, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, ok := e.data[key]
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
	e.mu.Lock()
	defer e.mu.Unlock()
	prev := e.data[key]
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
	e.data[key] = entry
	return entry, nil
}

func (e *Engine) Delete(key string) (Entry, error) {
	if key == "" {
		return Entry{}, errors.New("key required")
	}
	now := time.Now().UnixMilli()
	e.mu.Lock()
	defer e.mu.Unlock()
	prev := e.data[key]
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
	e.data[key] = entry
	return entry, nil
}

func (e *Engine) ApplyRemote(key string, entry Entry) (Entry, bool, error) {
	if key == "" {
		return Entry{}, false, errors.New("key required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	prev := e.data[key]
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
	e.data[key] = entry
	return entry, true, nil
}

func (e *Engine) CreateSnapshot() error {
	e.mu.RLock()
	copyData := make(map[string]Entry, len(e.data))
	for k, v := range e.data {
		v.Value = cloneBytes(v.Value)
		copyData[k] = v
	}
	e.mu.RUnlock()
	if err := e.snap.Save(copyData); err != nil {
		return err
	}
	return e.wal.Reset()
}

func (e *Engine) Close() error {
	return e.wal.Close()
}

func (e *Engine) applyRecord(rec walRecord) {
	entry := Entry{
		Value:               cloneBytes(rec.Value),
		Version:             rec.Version,
		TimestampUnixMillis: rec.TimestampUnixMillis,
		Tombstone:           rec.Tombstone,
	}
	e.mu.Lock()
	e.data[rec.Key] = entry
	e.mu.Unlock()
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
