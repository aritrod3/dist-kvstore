package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

func (e *Engine) RestoreFrom(snapshotPath string, walPath string) error {
	if snapshotPath == "" || walPath == "" {
		return errors.New("snapshot and wal paths required")
	}
	data, err := NewSnapshotter(snapshotPath).Load()
	if err != nil {
		return err
	}
	for i := range e.shards {
		e.shards[i].mu.Lock()
	}
	for i := range e.shards {
		e.shards[i].data = make(map[string]Entry)
	}
	for key, entry := range data {
		sh := e.shardFor(key)
		sh.data[key] = entry
	}
	if err := ReplayWALFile(walPath, func(rec walRecord) error {
		e.applyRecordToData(rec)
		return nil
	}); err != nil {
		for i := range e.shards {
			e.shards[i].mu.Unlock()
		}
		return err
	}
	for i := range e.shards {
		e.shards[i].mu.Unlock()
	}
	if err := copyFile(snapshotPath, e.snapPath); err != nil {
		return err
	}
	if err := copyFile(walPath, e.walPath); err != nil {
		return err
	}
	if err := e.wal.Close(); err != nil {
		return err
	}
	wal, err := OpenWAL(e.walPath)
	if err != nil {
		return err
	}
	e.wal = wal
	return nil
}

func (e *Engine) applyRecordToData(rec walRecord) {
	entry := Entry{
		Value:               cloneBytes(rec.Value),
		Version:             rec.Version,
		TimestampUnixMillis: rec.TimestampUnixMillis,
		Tombstone:           rec.Tombstone,
	}
	sh := e.shardFor(rec.Key)
	sh.data[rec.Key] = entry
}

func copyFile(src string, dst string) error {
	if src == dst {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), "copy-*.tmp")
	if err != nil {
		return err
	}
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dst)
}
