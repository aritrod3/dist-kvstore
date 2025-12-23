package storage

import (
	"encoding/gob"
	"os"
	"path/filepath"
)

type Snapshotter struct {
	path string
}

type snapshotPayload struct {
	Data map[string]Entry
	Seq  uint64
}

func NewSnapshotter(path string) *Snapshotter {
	return &Snapshotter{path: path}
}

func (s *Snapshotter) Load() (map[string]Entry, uint64, error) {
	file, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]Entry{}, 0, nil
		}
		return nil, 0, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	var payload snapshotPayload
	if err := dec.Decode(&payload); err != nil {
		return nil, 0, err
	}
	if payload.Data == nil {
		return map[string]Entry{}, payload.Seq, nil
	}
	return payload.Data, payload.Seq, nil
}

func (s *Snapshotter) Save(data map[string]Entry, seq uint64) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(tmp)
	if err := enc.Encode(snapshotPayload{Data: data, Seq: seq}); err != nil {
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
	return os.Rename(tmp.Name(), s.path)
}
