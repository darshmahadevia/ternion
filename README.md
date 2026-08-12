# QuorumKV

QuorumKV is a three-Node distributed key-value database in Go. Each Node is an
independent process with its own identity, network endpoints, WAL, Snapshot
files, and persistent volume. QuorumKV implements its own deterministic Raft
state machine and exposes typed gRPC APIs plus `quorumkvctl`.

## 60-second demo

Requirements: Docker and Docker Compose.

```sh
demo/run.sh
```

The walkthrough starts three independently persisted Nodes, performs
`SET`/`GET`/`DELETE`, kills the Leader, confirms majority progress, restarts the
old Leader, demonstrates minority unavailability, and demonstrates automatic
Snapshot/compaction followed by stale-Follower Snapshot recovery. It cleans up
its volumes when it exits. The demo uses a tiny Snapshot threshold so the
recovery path is visible quickly; normal configurations default to 64 MiB.

For a manual local Cluster, copy `quorumkv.example.yaml` three times, keep the
Cluster Identity and member map identical, and change `node.id` and
`node.data_dir` in each copy. Start each with:

```sh
go run ./cmd/quorumkv -config node-1.yaml
go run ./cmd/quorumkv -config node-2.yaml
go run ./cmd/quorumkv -config node-3.yaml
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 status
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 session open
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 set <session-id> 1 greeting hello
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 get greeting
go run ./cmd/quorumkvctl -address 127.0.0.1:7401 delete <session-id> 2 greeting
```

A Follower returns a typed Leader hint and the client follows it directly.
Successful reads perform a fresh Quorum confirmation. `GetStatus` is a local
observation, not a Cluster-health claim. Liveness and readiness are separate
health services; metrics and JSON logs provide operational evidence.

## Architecture, guarantees, and non-goals

See [docs/architecture.md](docs/architecture.md) for the architecture diagram,
consistency and crash guarantees, Quorum trade-offs, split-brain wording,
linearizable versus eventual consistency, and the explicit v1 non-goals.

## Correctness evidence

The repository currently verifies deterministic Raft transitions, WAL and
Snapshot recovery, linearizability, seeded fault schedules, and real
multi-process election, failover, partition, restart, repair, and Snapshot
scenarios. CI runs formatting, the full Go suite, race detection, vet, static
analysis, Protobuf validation, and Linux/Windows portable coverage. The public
project makes no production-readiness claim. A reproducible durable benchmark
and its raw measured result are in [benchmark/README.md](benchmark/README.md);
published numbers are local development measurements with hardware and workload
metadata, not production claims.

## Replay a deterministic fault schedule

```sh
go run ./cmd/quorumkvsim -seed 42 -steps 1000 -trace .traces/seed-42.json
```

A failed schedule prints its exact replay command and CI retains traces as
artifacts.
