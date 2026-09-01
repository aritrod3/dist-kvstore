package observability

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// QuorumRecorder records quorum resolution durations for read and write paths.
// It is safe for concurrent use. When LogEach is true, every Record call logs one line.
type QuorumRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	maxN    int
	LogEach bool
	Logger  *log.Logger
}

func NewQuorumRecorder(logEach bool) *QuorumRecorder {
	return &QuorumRecorder{
		maxN:    1_000_000,
		LogEach: logEach,
		Logger:  log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (q *QuorumRecorder) Record(kind string, d time.Duration) {
	if q == nil {
		return
	}
	if q.LogEach && q.Logger != nil {
		q.Logger.Printf("quorum op=%s resolution_ns=%d", kind, d.Nanoseconds())
	}
	q.mu.Lock()
	if len(q.samples) < q.maxN {
		q.samples = append(q.samples, d)
	}
	q.mu.Unlock()
}

// Summary returns count, average, p50, and p99 for recorded quorum durations.
func (q *QuorumRecorder) Summary() (n int, avg, p50, p99 time.Duration) {
	if q == nil {
		return 0, 0, 0, 0
	}
	q.mu.Lock()
	s := append([]time.Duration(nil), q.samples...)
	q.mu.Unlock()
	n = len(s)
	if n == 0 {
		return 0, 0, 0, 0
	}
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	avg = sum / time.Duration(n)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	p50 = s[(n*50)/100]
	if n == 1 {
		p99 = s[0]
		return n, avg, p50, p99
	}
	idx99 := (n * 99) / 100
	if idx99 >= n {
		idx99 = n - 1
	}
	p99 = s[idx99]
	return n, avg, p50, p99
}

// WriteSummary prints aggregate quorum stats (avg, p50, p99) to w.
func (q *QuorumRecorder) WriteSummary(w io.Writer) {
	if q == nil || w == nil {
		return
	}
	n, avg, p50, p99 := q.Summary()
	fmt.Fprintf(w, "quorum_summary samples=%d avg=%s p50=%s p99=%s\n", n, avg, p50, p99)
}
