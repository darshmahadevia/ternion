# Ternion

Ternion is a strongly consistent key-value database for a fixed three-Node Cluster, written in Go. It implements its own deterministic Raft state machine and persists each Node's state in a segmented write-ahead log and Snapshots.

This is a systems project, not a production database. It has no authentication or TLS and should run only on a trusted development network.

[![CI](https://github.com/darshmahadevia/ternion/actions/workflows/ci.yml/badge.svg)](https://github.com/darshmahadevia/ternion/actions/workflows/ci.yml)

```text
ternionctl ──gRPC──> any Node ──Raft peer gRPC──> the other two Nodes
                         │                              │
                         └── segmented WAL              └── independent volume
                             + in-memory state
```

## What it does

- Exposes `GET`, `SET`, and `DELETE` through typed gRPC APIs and `ternionctl`.
- Acknowledges a Mutation only after a Quorum of Nodes has persisted it and the Leader has committed and applied it.
- The Leader confirms its authority with a fresh Quorum before returning a successful read. Followers return a typed Leader hint instead of serving stale data.
- Deduplicates retried Mutations with Client Sessions and sequence numbers.
- Recovers from restarts through checksummed WAL segments and atomically installed Snapshots.
- Repairs stale Followers with conflict hints or Snapshot transfer after log compaction.
- Reports Prometheus metrics, JSON logs, and separate liveness and readiness states.

## Watch a cluster fail and recover

You need Docker with Docker Compose.

```sh
sh demo/run.sh
```

The script builds Ternion, starts three Nodes with separate persistent volumes, and runs a guided failure scenario. It writes and reads data, kills the Leader, shows that the remaining two Nodes can continue, restarts the old Leader, isolates a minority, and forces Snapshot recovery for a stale Follower. The script removes its containers and volumes when it exits.

The demo lowers the Snapshot threshold so compaction happens quickly. Normal configurations default to 64 MiB.

## Run a cluster manually

You need Go 1.25 or newer. Copy [`ternion.example.yaml`](ternion.example.yaml) once per Node. Keep the Cluster Identity and member map the same in all three files, then give each Node a unique `node.id` and `node.data_dir`. Start each Node in a separate terminal.

```sh
go run ./cmd/ternion -config node-1.yaml
go run ./cmd/ternion -config node-2.yaml
go run ./cmd/ternion -config node-3.yaml
```

Use a fourth terminal to inspect the cluster and change data:

```sh
go run ./cmd/ternionctl -address 127.0.0.1:7401 status
go run ./cmd/ternionctl -address 127.0.0.1:7401 session open
go run ./cmd/ternionctl -address 127.0.0.1:7401 set <session-id> 1 greeting hello
go run ./cmd/ternionctl -address 127.0.0.1:7401 get greeting
go run ./cmd/ternionctl -address 127.0.0.1:7401 delete <session-id> 2 greeting
```

The client follows Leader hints directly. `status` reports one Node's local view, not the health of the whole Cluster.

## Consistency and failure behavior

A completed Mutation is visible to a later successful read. A minority partition cannot accept Mutations or serve successful reads. A timeout leaves the outcome unknown, so callers should retry with the same Client Session and sequence number rather than issue a new Mutation.

Two Nodes may briefly believe they are the Leader in different Terms. Raft's election rules and intersecting Quorums prevent those beliefs from producing conflicting committed histories.

The full contract, failure cases, and architecture diagram are in [`docs/architecture.md`](docs/architecture.md).

## Verification

Run the test suite and static checks:

```sh
go test ./...
go test -race ./...
go vet ./...
```

The repository tests deterministic state transitions, WAL and Snapshot recovery, linearizability, seeded fault schedules, and real multi-process elections, partitions, restarts, and repairs. CI also runs formatting, Staticcheck, Protobuf lint and regeneration checks, and portable package tests on Linux and Windows.

A failed simulation prints the command needed to replay the same schedule:

```sh
go run ./cmd/ternionsim -seed 42 -steps 1000 -trace .traces/seed-42.json
```

One published local run on an AMD Ryzen 7 8845HS measured 715.1 durable SETs per second and 1,028.1 linearizable GETs per second, with p99 latencies of 15.50 ms and 11.52 ms. These are development-machine results, not production claims. The raw result, workload, and benchmark runner are in [`benchmark/README.md`](benchmark/README.md).

## Deliberate limits

Ternion supports exactly three Nodes. It does not implement dynamic membership, transactions, watches, TTLs, authentication, authorization, TLS, WAN tuning, or Byzantine fault tolerance.
