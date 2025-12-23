package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Recorder interface {
	ObserveLatency(op string, d time.Duration)
	IncFailedWrites(op string)
	SetReplicationLag(pending int)
	SetWALSizeBytes(size int64)
	SetNodeAlive(nodeID string, alive bool)
	Handler() http.Handler
}

type Prometheus struct {
	reg            *prometheus.Registry
	latency        *prometheus.HistogramVec
	failedWrites   *prometheus.CounterVec
	replicationLag prometheus.Gauge
	walSizeBytes   prometheus.Gauge
	nodeAlive      *prometheus.GaugeVec
}

func NewPrometheus() *Prometheus {
	reg := prometheus.NewRegistry()
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_latency_seconds",
		Help:    "Operation latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"op"})
	failedWrites := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_failed_writes_total",
		Help: "Total failed writes by operation.",
	}, []string{"op"})
	replicationLag := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kvstore_replication_lag",
		Help: "Pending replication tasks.",
	})
	walSizeBytes := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kvstore_wal_size_bytes",
		Help: "WAL size in bytes.",
	})
	nodeAlive := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_node_alive",
		Help: "Node availability (1=alive, 0=down).",
	}, []string{"node"})
	reg.MustRegister(latency, failedWrites, replicationLag, walSizeBytes, nodeAlive)
	return &Prometheus{
		reg:            reg,
		latency:        latency,
		failedWrites:   failedWrites,
		replicationLag: replicationLag,
		walSizeBytes:   walSizeBytes,
		nodeAlive:      nodeAlive,
	}
}

func (p *Prometheus) ObserveLatency(op string, d time.Duration) {
	p.latency.WithLabelValues(op).Observe(d.Seconds())
}

func (p *Prometheus) IncFailedWrites(op string) {
	p.failedWrites.WithLabelValues(op).Inc()
}

func (p *Prometheus) SetReplicationLag(pending int) {
	p.replicationLag.Set(float64(pending))
}

func (p *Prometheus) SetWALSizeBytes(size int64) {
	p.walSizeBytes.Set(float64(size))
}

func (p *Prometheus) SetNodeAlive(nodeID string, alive bool) {
	value := 0.0
	if alive {
		value = 1.0
	}
	p.nodeAlive.WithLabelValues(nodeID).Set(value)
}

func (p *Prometheus) Handler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}
