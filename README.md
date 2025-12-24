# Distributed KV Store

A small distributed KV store I built to show real systems choices, not a toy. It’s opinionated: replication factor is fixed at 3, writes hit a WAL on every operation, and there’s a clear line between fast and strong consistency.

## Goals
- Scale out with consistent hashing and virtual nodes.
- Keep data safe with WAL + snapshots.
- Make consistency tradeoffs explicit (fast vs strong).
- Measure latency and replication lag, not hand‑wave it.

## Architecture (high level)
```
Client -> Coordinator -> Primary -> Replicas
                |            |
                +--> Read path (local or quorum)
```
- Consistent hashing for partitioning
- Replication factor N=3 (enforced in code)
- WAL + snapshots for durability

## Consistency Model
- Default is local reads (fast), quorum reads are explicit.
- Writes are either fast (primary ack) or strong (majority ack).
- Conflict resolution is version, then timestamp. Simple and predictable.

## Failure Scenarios
- Primary failure: heartbeat timeout drops it from the ring, a new primary is chosen.
- Replica failure: automatic catch‑up via snapshot + WAL replay.
- Network partition: strong writes can fail; fast writes continue and may diverge.

## Tradeoffs
- WAL fsync on every write keeps durability simple, but it’s the throughput bottleneck.
- Quorum reads/writes cost latency. I still keep them because I want the option.
- Shard locks avoid a global lock, but the WAL mutex still serializes writes.

## Benchmarks
- `go test ./internal/storage -bench=.` for storage engine latency
- Prometheus exposes p50/p95 latency, WAL size, replication lag

## Usage
- `make test` or `go test ./...`
- `go test ./internal/storage -bench=.`
- `docker compose -f deploy/docker-compose.yml up`

## Resume Bullet
Built a distributed key–value store with sharding, N=3 replication, WAL‑backed durability, and strong/fast consistency modes; implemented crash recovery, replica catch‑up, and quorum‑based writes.

## Future Work
- Anti‑entropy repair for replica drift.
- SSTables + compaction scheduling.
- Real gRPC transport and cross‑node tracing.

## Repo Layout
- `cmd/` entrypoints
- `internal/` core packages
- `proto/` gRPC definitions
- `docs/` design docs and diagrams
- `deploy/` local deploy assets
- `scripts/` helper scripts

## Status
Core storage, replication, failure handling, and observability are implemented. Transport is still stubbed.
