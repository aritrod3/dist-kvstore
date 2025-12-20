package storage

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walOpPut    = "put"
	walOpDelete = "delete"
)

type walRecord struct {
	Op                  string
	Key                 string
	Value               []byte
	Version             uint64
	TimestampUnixMillis int64
	Tombstone           bool
}

type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
	enc  *gob.Encoder
}

func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		path: path,
		file: file,
		enc:  gob.NewEncoder(file),
	}, nil
}

func (w *WAL) Append(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(rec); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Replay(apply func(walRecord) error) error {
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := apply(rec); err != nil {
			return err
		}
	}
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.enc = gob.NewEncoder(file)
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}
