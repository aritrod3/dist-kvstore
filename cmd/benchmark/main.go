// Command benchmark runs concurrent load against a local 3-node in-process
// cluster and writes benchmark_results.csv (see .gitignore).
//
// Isolated vs contended (equal total ops, same key set, same mix):
//
//	go run ./cmd/benchmark -compare
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/aritrod3/kv-store/internal/consistency"
	"github.com/aritrod3/kv-store/internal/observability"
	"github.com/aritrod3/kv-store/internal/replication"
	"github.com/aritrod3/kv-store/internal/testcluster"
)

type result struct {
	scenario   string
	clients    int
	opsEach    int
	totalOps   int64
	elapsedSec float64
	throughput float64
	p50        time.Duration
	p95        time.Duration
	p99        time.Duration
}

func main() {
	clients := flag.Int("clients", 8, "concurrent client goroutines (contended scenario)")
	opsEach := flag.Int("ops", 500, "operations per client in the contended scenario")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of ops that are reads (0..1)")
	csvPath := flag.String("csv", "benchmark_results.csv", "output CSV path")
	dataDir := flag.String("data-dir", "", "temp data root (default: OS temp dir)")
	quorumLog := flag.Bool("quorum-log", false, "log each quorum resolution to stdout")
	compare := flag.Bool("compare", false, "run isolated (1 client) then contended (-clients), equal total ops")
	writeMode := flag.String("write-mode", "strong", "write consistency: strong or fast")
	readMode := flag.String("read-mode", "quorum", "read consistency: quorum or local")
	flag.Parse()

	wMode, err := parseWriteMode(*writeMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	rMode, err := parseReadMode(*readMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	tmp := *dataDir
	cleanup := false
	if tmp == "" {
		tmp, err = os.MkdirTemp("", "kvstore-bench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "mkdir temp: %v\n", err)
			os.Exit(1)
		}
		cleanup = true
	} else {
		_ = os.MkdirAll(tmp, 0o755)
	}
	if cleanup {
		defer os.RemoveAll(tmp)
	}

	fmt.Printf("hardware goos=%s goarch=%s num_cpu=%d goversion=%s\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Printf("workload write_mode=%s read_mode=%s read_ratio=%.2f keys_per_primary=192 cluster=3-node-inprocess\n",
		*writeMode, *readMode, *readRatio)

	totalOps := *clients * *opsEach
	var results []result
	if *compare {
		results = append(results, runScenario(tmp, "isolated", 1, totalOps, *readRatio, wMode, rMode, *quorumLog))
		results = append(results, runScenario(tmp, "contended", *clients, *opsEach, *readRatio, wMode, rMode, *quorumLog))
	} else {
		results = append(results, runScenario(tmp, "single", *clients, *opsEach, *readRatio, wMode, rMode, *quorumLog))
	}

	for _, r := range results {
		fmt.Printf("scenario=%s clients=%d ops_each=%d total_ops=%d duration_sec=%.4f throughput_ops_per_sec=%.2f latency_p50=%s p95=%s p99=%s\n",
			r.scenario, r.clients, r.opsEach, r.totalOps, r.elapsedSec, r.throughput, r.p50, r.p95, r.p99)
	}

	if err := writeCSV(*csvPath, results); err != nil {
		fmt.Fprintf(os.Stderr, "csv: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s\n", *csvPath)
}

func parseWriteMode(s string) (replication.WriteConsistency, error) {
	switch s {
	case "strong":
		return replication.WriteConsistencyStrong, nil
	case "fast":
		return replication.WriteConsistencyFast, nil
	default:
		return 0, fmt.Errorf("write-mode must be strong or fast, got %q", s)
	}
}

func parseReadMode(s string) (consistency.ReadConsistency, error) {
	switch s {
	case "quorum":
		return consistency.ReadConsistencyQuorum, nil
	case "local":
		return consistency.ReadConsistencyLocal, nil
	default:
		return 0, fmt.Errorf("read-mode must be quorum or local, got %q", s)
	}
}

func runScenario(tmp, scenario string, clients, opsEach int, readRatio float64, wMode replication.WriteConsistency, rMode consistency.ReadConsistency, quorumLog bool) result {
	quorum := observability.NewQuorumRecorder(quorumLog)
	c, err := testcluster.Open(filepath.Join(tmp, scenario), &testcluster.Options{Quorum: quorum})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	defer quorum.WriteSummary(os.Stdout)

	// Keys pinned to one primary. Strong writes from different primaries can
	// deadlock: replicateSync holds work on the source engine while ApplyRemote
	// takes a shard lock on the destination, and the reverse path can meet it.
	keys, err := testcluster.FindKeysForPrimary(c.Manager, testcluster.Node1, 192)
	if err != nil {
		fmt.Fprintf(os.Stderr, "find keys: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	totalOps := int64(clients * opsEach)
	latencies := make([]time.Duration, 0, totalOps)
	var latMu sync.Mutex

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			val := []byte("v")
			for j := 0; j < opsEach; j++ {
				key := keys[(seed+j*17)%len(keys)]
				coord, err := c.PrimaryCoordinator(key)
				if err != nil {
					fmt.Fprintf(os.Stderr, "coordinator: %v\n", err)
					os.Exit(1)
				}
				read := float64((seed+j)%100)/100.0 < readRatio
				t0 := time.Now()
				if read {
					_, _, err := coord.Get(ctx, key, rMode)
					if err != nil {
						fmt.Fprintf(os.Stderr, "get: %v\n", err)
						os.Exit(1)
					}
				} else {
					val[0] = byte((seed + j) % 251)
					_, err := coord.Put(ctx, key, val, wMode)
					if err != nil {
						fmt.Fprintf(os.Stderr, "put: %v\n", err)
						os.Exit(1)
					}
				}
				d := time.Since(t0)
				latMu.Lock()
				latencies = append(latencies, d)
				latMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return result{
		scenario:   scenario,
		clients:    clients,
		opsEach:    opsEach,
		totalOps:   totalOps,
		elapsedSec: elapsed,
		throughput: float64(totalOps) / elapsed,
		p50:        percentile(latencies, 0.50),
		p95:        percentile(latencies, 0.95),
		p99:        percentile(latencies, 0.99),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(n)) - 1)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

func writeCSV(path string, results []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{
		"scenario", "clients", "ops_each", "total_ops", "duration_sec", "throughput_ops_per_sec",
		"p50_ns", "p95_ns", "p99_ns",
	})
	for _, r := range results {
		_ = w.Write([]string{
			r.scenario,
			fmt.Sprintf("%d", r.clients),
			fmt.Sprintf("%d", r.opsEach),
			fmt.Sprintf("%d", r.totalOps),
			fmt.Sprintf("%.6f", r.elapsedSec),
			fmt.Sprintf("%.4f", r.throughput),
			fmt.Sprintf("%d", r.p50.Nanoseconds()),
			fmt.Sprintf("%d", r.p95.Nanoseconds()),
			fmt.Sprintf("%d", r.p99.Nanoseconds()),
		})
	}
	w.Flush()
	return w.Error()
}
