package storage

import (
	"fmt"
	"testing"
)

func TestAutoSnapshotBoundsWAL(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(Config{
		DataDir:          dir,
		Shards:           4,
		SnapshotEvery:    10,
		SnapshotInterval: 0,
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	for i := 0; i < 25; i++ {
		if _, err := engine.Put(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	count := 0
	if err := ReplayWALFile(engine.WALPath(), func(rec walRecord) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if count > 9 {
		t.Fatalf("expected WAL records <= 9, got %d", count)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restarted, err := Open(Config{
		DataDir:          dir,
		Shards:           4,
		SnapshotEvery:    10,
		SnapshotInterval: 0,
	})
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer restarted.Close()

	for i := 0; i < 25; i++ {
		entry, ok := restarted.Get(fmt.Sprintf("k%d", i))
		if !ok || string(entry.Value) != "v" {
			t.Fatalf("missing key k%d after restart", i)
		}
	}
}
