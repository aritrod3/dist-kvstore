package cluster

import "testing"

func TestCoordinatorReplicationFactorEnforced(t *testing.T) {
	manager, err := NewManager(8, 0, nil)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	if _, err := NewCoordinator("node-1", manager, 2, nil, nil, nil); err == nil {
		t.Fatalf("expected error for replicationFactor != 3")
	}
	if _, err := NewCoordinator("node-1", manager, 3, nil, nil, nil); err != nil {
		t.Fatalf("expected replicationFactor=3 to succeed, got %v", err)
	}
}
