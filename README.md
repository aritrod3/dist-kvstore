# kv-store

Three-replica KV store in Go. Consistent hashing, a gob WAL that calls `Sync` on every append, and a write path you pick: fast (primary ack) or strong (majority). I wrote it so those choices sit in functions a reviewer can open, not behind a server I never finished. There is no `cmd` binary. Tests and `cmd/benchmark` call `Coordinator` in-process through `InMemoryReplicaRPC`.

## Design decisions

Every `Put` and `Delete` encodes a gob `walRecord` and `WAL.Append` fsyncs it before the shard map moves. Crash after the sync, process dies, `Open` loads `snapshot.bin` and replays. `TestCrashAfterWALAppendRecovery` covers that. I fsync every record on purpose. Group commit would have hidden the cost. It also would have been correct. Sixteen shard mutexes look concurrent until they all wait on the same WAL lock.

I did not implement generation-token fencing. There is no lease. `Coordinator.Put` asks `Manager.ReplicasForKey` and returns `ErrNotPrimary` if this process is not `nodes[0]`. That only works because the test cluster shares one in-memory ring. `DetectFailures` will drop a silent node, but nothing calls it unless a test does.

What a stale replica actually hits: `Engine.ApplyRemote`. If `prev.Version >= entry.Version`, the write is ignored (`applied=false`). An old primary is a different story. It still does `prev.Version+1` on its own engine and will commit. I would put a cluster generation on failover and check it in `ApplyRemote` before the WAL line. A fenced worker gets an error. Today it gets a successful Put.

No tenants. `MaxInFlight` is not a thing here. The async replica queue is `make(chan func(), 1024)` in `NewReplicator`. Fill it and `replicateAsync` blocks the caller. I would rather stall than drop a fast write on the floor. A 429 would need an HTTP server.

`proto/kvstore.proto` has Get/Put/Delete and the consistency enums. Nothing generates it. `Prometheus.Handler` exists. Nothing binds a port. Repeat `Put` of the same bytes still bumps version. I did not make that idempotent.

Fast writes (`WriteConsistencyFast`): primary WAL, then enqueue. Workers fire `ReplicatePut` once. If the replica is down, that copy is gone until catch-up. Strong writes (`replicateSync`): wait until `acks >= total/2+1`, primary counts as one. With RF=3, one replica is enough (`TestReplicaDownStrongWrite`). Both down: `ErrWriteQuorumFailed`. The annoying part: `local.Put` already ran. You get an error and a durable value on the coordinator. I should have replicated first or rolled back. I didn't.

I used gob files instead of SQLite. Snapshot is encode, `Sync`, `Rename`. WAL is append. Catch-up is `RestoreFrom` copying those two files. You can read the whole durability story in `wal.go` / `snapshot.go` / `recovery.go`. SQLite would have given me a transaction around snapshot-then-truncate, which I got wrong. See below.

`NewCoordinator` rejects any replication factor other than 3. I did not want a knobs discussion.

Local read is `Engine.Get`. Quorum read waits for majority and keeps whatever `newer` ranks higher: version first, then `TimestampUnixMillis`. `Get` hides tombstones. `GetEntry` does not.

`CatchUpReplica` copies the whole snapshot and WAL. `CheckLag` compares `LastSeq()` to one configured source, not the key's primary. Works in a three-engine test. Breaks as soon as two keys live on different primaries. I knew that and shipped it anyway for the failure test.

`ApplyRemote` ties on version only. Quorum reads also look at timestamp. Inconsistent on purpose. The replica is applying a version the primary already picked. The reader is looking at drift.

## Benchmarks

Not multi-tenant. One client vs eight, same primary, same fsyncing WAL.

```
go run ./cmd/benchmark -compare
```

or `python3 benchmark.py -compare`.

MacBookPro17,1, Apple M1, 8 cores (4P+4E), 8 GB, macOS 26.6.2, Go 1.27.2 darwin/arm64. Three engines on local disk. RPC is in-process. Harness sets `SnapshotEvery` to `1<<50` so we don't hit the deadlock in Limitations.

192 keys hashed to `node-1`. I pinned them. Strong writes from two primaries deadlock on shard locks; there is a comment in `cmd/benchmark` about it. Mix is 50% quorum `Get`, 50% strong `Put`. Isolated: 1 goroutine, 4000 ops. Contended: 8 goroutines × 500. Time is client wall clock around `Coordinator.Get` / `Put`.

One run:

| scenario   | clients | ops  | throughput | p50     | p95     | p99     |
|------------|---------|------|------------|---------|---------|---------|
| isolated   | 1       | 4000 | 200 ops/s  | 0.08 ms | 12.1 ms | 14.9 ms |
| contended  | 8       | 4000 | 178 ops/s  | 42.4 ms | 110 ms  | 141 ms  |

Eight clients and throughput does not move. p99 goes from 14.9 ms to 141 ms. That is the WAL `Sync`. Quorum wait by itself (just the replica RPCs) was p99 10.3 ms vs 108 ms.

This is not a network number. `InMemoryReplicaRPC` is a function call. `go test ./internal/storage -bench=.` hangs on the snapshot deadlock. Skip it.

## Architecture

```
test / cmd/benchmark
        |
        v
 Coordinator          FNV-32 ring, 64 vnodes per node in the harness
        |             write refused unless this node is replicas[0]
        v
 Replicator  ---->  Engine (16 shards, gob WAL+fsync, snapshot.bin)
        |
        +-- strong: ApplyRemote, wait majority
        +-- fast:   1024-deep queue, no retry
        +-- reads:  local Get, or majority + newer()
```

`cluster.Manager` is the membership map. `hashring.Ring` places keys. Spans can chain `coordinator.put` -> `replication.put` -> `replication.send` -> `replica.apply`. `QuorumRecorder` will print p50/p99 if you pass it in. None of that is a process you can start.

## Running it locally

Go 1.25 (`go.mod`). CI still says 1.22.

```
go test ./internal/cluster ./internal/replication ./internal/observability ./internal/testcluster
go run ./cmd/benchmark -compare
go run ./cmd/benchmark -h
```

`make test` and `go test ./...` freeze in `TestAutoSnapshotBoundsWAL`. Don't.

`deploy/docker-compose.yml` runs `./cmd/kvstore`. That directory is empty. The YAML next to it is unused.

## Limitations and what I would do next

`Put` holds the shard `Lock`, then `maybeSnapshot` calls `CreateSnapshot`, which `RLock`s every shard including the one it already owns. Go RWMutex will just sit there. Default `SnapshotEvery` is 1000. The harness only survives because it sets that to `1<<50`. I would snapshot off the Put stack, or stop snapshotting from Put.

Worse, even if you dodge the lock: copy shards one by one, read `LastSeq()`, `wal.Reset()`. A Put on an already-copied shard can make it into the WAL and then get truncated. Restart loads a snapshot that never had that key. Truncate to the snapshotted seq, under the WAL mutex. Or use a real store.

Failed strong write still sits on the primary. Failed fast replica is forgotten. Crash loses the 1024 queue.

No fence on failover. Two live primaries plus strong writes: deadlock. That is why the bench refuses to spread keys.

`AutoRecovery` clones from one `primaryID`. Ring placement is ignored.

Torn gob record: `Open` fails the engine. No checksum.

`proto/` and `deploy/` describe a cluster that is not in this repo. Nothing listens on `:9101`.

Fix the snapshot lock and the truncate. Then a generation in `ApplyRemote`. Then a transport for the proto. Then retries. I would delete the compose file until there is something to compose.
