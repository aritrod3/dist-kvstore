# Distributed KV Store

Production-style, replicated key–value store with sharding, WAL-backed durability, and configurable consistency.

## Goals
- Horizontal scalability
- Fault tolerance
- Predictable consistency guarantees
- Measurable latency and throughput

## Architecture (high level)
- Client -> Coordinator -> Replicas
- Consistent hashing for partitioning
- Replication factor N=3
- WAL + snapshots for durability

## Repo Layout
- `cmd/` entrypoints
- `internal/` core packages
- `proto/` gRPC definitions
- `docs/` design docs and diagrams
- `deploy/` local deploy assets
- `scripts/` helper scripts

## Status
Initial scaffold. Implementation follows the 10-step plan.
