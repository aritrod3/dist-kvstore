package testcluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aritrod3/kv-store/internal/replication"
)

func TestReplicationLagThreeNodeCluster(t *testing.T) {
	c, err := Open(filepath.Join(t.TempDir(), "c"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	key, err := FindKeyForPrimary(c.Manager, Node1)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	coord, err := c.PrimaryCoordinator(key)
	if err != nil {
		t.Fatalf("coord: %v", err)
	}

	const iterations = 30
	var sum time.Duration
	var max time.Duration
	var n int

	for i := 0; i < iterations; i++ {
		val := []byte{byte(i % 255)}
		t0 := time.Now()
		if _, err := coord.Put(context.Background(), key, val, replication.WriteConsistencyFast); err != nil {
			t.Fatalf("put: %v", err)
		}
		deadline := time.Now().Add(3 * time.Second)
		seen := map[string]time.Time{}
		for time.Now().Before(deadline) {
			for _, id := range []string{Node2, Node3} {
				if _, ok := seen[id]; ok {
					continue
				}
				e, ok := c.Engines[id].Get(key)
				if ok && len(e.Value) == 1 && e.Value[0] == val[0] {
					seen[id] = time.Now()
				}
			}
			if len(seen) == 2 {
				break
			}
			time.Sleep(50 * time.Microsecond)
		}
		if len(seen) < 2 {
			t.Fatalf("iteration %d: replicas did not catch up", i)
		}
		for id, ts := range seen {
			lag := ts.Sub(t0)
			sum += lag
			n++
			if lag > max {
				max = lag
			}
			_ = id
		}
	}

	avg := time.Duration(int64(sum) / int64(n))
	t.Logf("replication_lag avg_ms=%.3f max_ms=%.3f (n=%d sample points)", avg.Seconds()*1000, max.Seconds()*1000, n)
}
