package observability

import (
	"bytes"
	"testing"
	"time"
)

func TestQuorumRecorderSummary(t *testing.T) {
	q := NewQuorumRecorder(false)
	for _, d := range []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 100 * time.Millisecond} {
		q.Record("read_quorum", d)
	}
	n, avg, p50, p99 := q.Summary()
	if n != 3 {
		t.Fatalf("n=%d", n)
	}
	if p50 != 2*time.Millisecond {
		t.Fatalf("p50=%v", p50)
	}
	if p99 != 100*time.Millisecond {
		t.Fatalf("p99=%v", p99)
	}
	if avg == 0 {
		t.Fatal("avg")
	}
	var buf bytes.Buffer
	q.WriteSummary(&buf)
	if buf.Len() < 10 {
		t.Fatal("expected summary output")
	}
}
