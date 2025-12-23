package cluster

import (
	"testing"
	"time"

	"github.com/aritrod3/kv-store/internal/hashring"
	"github.com/aritrod3/kv-store/internal/storage"
)

func TestAutoRecoveryRejoinCatchUp(t *testing.T) {
	primaryDir := t.TempDir()
	replicaDir := t.TempDir()
	primary, err := storage.Open(storage.Config{DataDir: primaryDir})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	defer primary.Close()
	replica, err := storage.Open(storage.Config{DataDir: replicaDir})
	if err != nil {
		t.Fatalf("open replica: %v", err)
	}
	defer replica.Close()

	manager, err := NewManager(8, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	start := time.Now()
	if err := manager.RegisterAt(hashring.Node{ID: "node-1"}, start); err != nil {
		t.Fatalf("register primary: %v", err)
	}
	if err := manager.RegisterAt(hashring.Node{ID: "node-2"}, start); err != nil {
		t.Fatalf("register replica: %v", err)
	}

	recovery := &RecoveryManager{}
	engines := MapEngineProvider{
		"node-1": primary,
		"node-2": replica,
	}
	auto, err := NewAutoRecovery(manager, recovery, engines, "node-1", 0, time.Second)
	if err != nil {
		t.Fatalf("auto recovery: %v", err)
	}
	_ = auto

	if err := manager.UpdateHeartbeatAt("node-1", start.Add(50*time.Millisecond)); err != nil {
		t.Fatalf("primary heartbeat: %v", err)
	}
	manager.DetectFailures(start.Add(20 * time.Millisecond))

	if _, err := primary.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if _, err := primary.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("put k2: %v", err)
	}

	if err := manager.UpdateHeartbeatAt("node-2", start.Add(60*time.Millisecond)); err != nil {
		t.Fatalf("rejoin heartbeat: %v", err)
	}

	entry, ok := replica.Get("k1")
	if !ok || string(entry.Value) != "v1" {
		t.Fatalf("expected k1 to be recovered")
	}
	entry, ok = replica.Get("k2")
	if !ok || string(entry.Value) != "v2" {
		t.Fatalf("expected k2 to be recovered")
	}
}

func TestAutoRecoveryLagDetection(t *testing.T) {
	primaryDir := t.TempDir()
	replicaDir := t.TempDir()
	primary, err := storage.Open(storage.Config{DataDir: primaryDir})
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	defer primary.Close()
	replica, err := storage.Open(storage.Config{DataDir: replicaDir})
	if err != nil {
		t.Fatalf("open replica: %v", err)
	}
	defer replica.Close()

	manager, err := NewManager(8, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	if err := manager.Register(hashring.Node{ID: "node-1"}); err != nil {
		t.Fatalf("register primary: %v", err)
	}
	if err := manager.Register(hashring.Node{ID: "node-2"}); err != nil {
		t.Fatalf("register replica: %v", err)
	}

	recovery := &RecoveryManager{}
	engines := MapEngineProvider{
		"node-1": primary,
		"node-2": replica,
	}
	auto, err := NewAutoRecovery(manager, recovery, engines, "node-1", 0, time.Second)
	if err != nil {
		t.Fatalf("auto recovery: %v", err)
	}

	if _, err := primary.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := auto.CheckLag(); err != nil {
		t.Fatalf("check lag: %v", err)
	}
	if _, ok := replica.Get("k1"); !ok {
		t.Fatalf("expected lag recovery to run")
	}
}
