package cluster

import (
	"testing"
	"time"

	"github.com/aritrod3/kv-store/internal/hashring"
)

func TestPrimaryReassignmentOnFailure(t *testing.T) {
	manager, err := NewManager(8, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	start := time.Now()
	if err := manager.RegisterAt(hashring.Node{ID: "node-1"}, start); err != nil {
		t.Fatalf("register node-1: %v", err)
	}
	if err := manager.RegisterAt(hashring.Node{ID: "node-2"}, start); err != nil {
		t.Fatalf("register node-2: %v", err)
	}

	key := "test-key"
	primary, err := manager.PrimaryForKey(key)
	if err != nil {
		t.Fatalf("primary: %v", err)
	}
	if primary.ID != "node-1" && primary.ID != "node-2" {
		t.Fatalf("unexpected primary: %s", primary.ID)
	}

	otherID := "node-1"
	if primary.ID == "node-1" {
		otherID = "node-2"
	}
	if err := manager.UpdateHeartbeatAt(otherID, start.Add(50*time.Millisecond)); err != nil {
		t.Fatalf("keep %s alive: %v", otherID, err)
	}
	manager.DetectFailures(start.Add(20 * time.Millisecond))
	primaryAfter, err := manager.PrimaryForKey(key)
	if err != nil {
		t.Fatalf("primary after: %v", err)
	}
	if primaryAfter.ID != otherID {
		t.Fatalf("expected primary to move to %s, got %s", otherID, primaryAfter.ID)
	}
}
