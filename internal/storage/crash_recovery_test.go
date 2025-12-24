package storage

import (
	"errors"
	"testing"
)

func TestCrashAfterWALAppendRecovery(t *testing.T) {
	dir := t.TempDir()
	crashErr := errors.New("crash")
	engine, err := Open(Config{
		DataDir: dir,
		AfterWALAppendHook: func() error {
			return crashErr
		},
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	_, err = engine.Put("k1", []byte("v1"))
	if !errors.Is(err, crashErr) {
		t.Fatalf("expected crash error, got %v", err)
	}
	if _, ok := engine.GetEntry("k1"); ok {
		t.Fatalf("expected in-memory state to not include k1")
	}
	_ = engine.Close()

	restarted, err := Open(Config{DataDir: dir})
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	defer restarted.Close()

	entry, ok := restarted.Get("k1")
	if !ok || string(entry.Value) != "v1" {
		t.Fatalf("expected k1 to be recovered after restart")
	}
	if entry.Version != 1 || entry.Tombstone {
		t.Fatalf("unexpected entry state after recovery")
	}
}
