package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/aritrod3/kv-store/internal/storage"
)

type fakeWriter struct {
	fail map[string]error
}

func (f *fakeWriter) ReplicatePut(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	if err, ok := f.fail[nodeID]; ok {
		return err
	}
	return nil
}

func (f *fakeWriter) ReplicateDelete(ctx context.Context, nodeID string, key string, entry storage.Entry) error {
	if err, ok := f.fail[nodeID]; ok {
		return err
	}
	return nil
}

func TestStrongWriteQuorumFailure(t *testing.T) {
	engine, err := storage.Open(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	writer := &fakeWriter{fail: map[string]error{
		"r1": errors.New("down"),
		"r2": errors.New("down"),
	}}
	replicator := NewReplicator(engine, writer, 1, nil, nil, nil, nil, nil)
	defer replicator.Close()

	_, err = replicator.Put(context.Background(), "key", []byte("v"), []string{"r1", "r2"}, WriteConsistencyStrong)
	if !errors.Is(err, ErrWriteQuorumFailed) {
		t.Fatalf("expected quorum failure, got %v", err)
	}
}

func TestReplicaDownStrongWrite(t *testing.T) {
	engine, err := storage.Open(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	writer := &fakeWriter{fail: map[string]error{
		"r1": errors.New("down"),
	}}
	replicator := NewReplicator(engine, writer, 1, nil, nil, nil, nil, nil)
	defer replicator.Close()

	_, err = replicator.Put(context.Background(), "key", []byte("v"), []string{"r1", "r2"}, WriteConsistencyStrong)
	if err != nil {
		t.Fatalf("expected success with one replica down, got %v", err)
	}
}

func TestReplicaDownFastWrite(t *testing.T) {
	engine, err := storage.Open(storage.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	writer := &fakeWriter{fail: map[string]error{
		"r1": errors.New("down"),
		"r2": errors.New("down"),
	}}
	replicator := NewReplicator(engine, writer, 1, nil, nil, nil, nil, nil)
	defer replicator.Close()

	_, err = replicator.Put(context.Background(), "key", []byte("v"), []string{"r1", "r2"}, WriteConsistencyFast)
	if err != nil {
		t.Fatalf("expected fast write to succeed, got %v", err)
	}
}
