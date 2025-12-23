# Distributed KV Store

Production-style, replicated key–value store with sharding, WAL-backed durability, and configurable consistency.

## Goals
- Horizontal scalability
- Fault tolerance
- Predictable consistency guarantees
- Measurable latency and throughput

## Architecture (high level)
```
Client -> Coordinator -> Primary -> Replicas
                |            |
                +--> Read path (local or quorum)
```
- Consistent hashing for partitioning
- Replication factor N=3
- WAL + snapshots for durability

## Consistency Model
- Default: local reads (fast), optional quorum reads (strong)
- Writes: fast (primary ack) or strong (majority ack)
- Conflict resolution: version, then timestamp

## Failure Scenarios
- Primary failure: detected via heartbeat timeout; ring updated; new primary chosen
- Replica failure: catch-up via snapshot transfer + WAL replay
- Network partition: strong writes can reject to preserve consistency; fast writes continue

## Tradeoffs
- Append-only WAL for durability and fast writes, slower compaction
- Quorum reads/writes increase latency but improve consistency
- Shard locks avoid global contention, add per-key overhead

## Benchmarks
- `go test ./internal/storage -bench=.` for storage engine latency
- Metrics exposed via Prometheus for p50/p95 and throughput tracking

## Usage
- `make test` or `go test ./...`
- `go test ./internal/storage -bench=.`
- `docker compose -f deploy/docker-compose.yml up`

## Resume Bullet
Built a distributed key–value store supporting sharding, replication, WAL-backed durability, and configurable consistency guarantees; implemented failure recovery, replica catch-up, and load-balanced partitioning with consistent hashing.

## Future Work
- Anti-entropy repair for replica divergence
- On-disk SSTables with compaction scheduling
- Cross-node tracing and distributed profiling

## Repo Layout
- `cmd/` entrypoints
- `internal/` core packages
- `proto/` gRPC definitions
- `docs/` design docs and diagrams
- `deploy/` local deploy assets
- `scripts/` helper scripts

## Status
Initial scaffold. Implementation follows the 10-step plan.
