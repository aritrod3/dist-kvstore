package storage

import (
	"testing"
	"time"
)

func TestCompactTombstones(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(Config{DataDir: dir, Shards: 4})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	old := time.Now().Add(-2 * time.Hour)
	entry := Entry{
		Value:               nil,
		Version:             1,
		TimestampUnixMillis: old.UnixMilli(),
		Tombstone:           true,
	}
	_, _, err = engine.ApplyRemote("key", entry)
	if err != nil {
		t.Fatalf("apply remote: %v", err)
	}

	removed, err := engine.CompactTombstones(30*time.Minute, time.Now())
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if _, ok := engine.GetEntry("key"); ok {
		t.Fatalf("expected key to be removed")
	}
}
