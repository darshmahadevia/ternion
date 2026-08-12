# QuorumKV v1: architecture and guarantees

## One-minute tour

```text
quorumkvctl ──gRPC──> any Node ──Raft peer gRPC──> the other two Nodes
                         │                         │
                         └── segmented WAL         └── independent volume
                             + in-memory state
```

Run the public failure walkthrough with Docker and Compose:

```sh
demo/run.sh
```

It builds the binaries from this checkout, starts three independent processes
with three volumes, exercises CRUD, kills and restarts a Leader, demonstrates
majority progress, demonstrates minority rejection, and uses the deliberately
small demo Snapshot threshold to show a stale Follower recovering from a
Snapshot. The demo threshold is not the production default. Use `docker compose
down -v` to remove its data.

## Guarantees

- A mutation is acknowledged only after durable replication to a Quorum,
  commitment, and application by the Leader.
- A successful `GET` is linearizable. Followers redirect; they do not serve
  stale reads.
- A retry with the same Client Session and sequence has at-most-once effect.
  A timeout means the result is unknown, not that the mutation failed.
- Raft prevents conflicting committed histories through vote/log safety and
  intersecting Quorums. Nodes can temporarily believe they lead in different
  Terms; that is not a claim that split-brain beliefs are impossible.
- Snapshots are validated and atomically installed. WAL compaction retains
  history needed by the active state and later entries.

## Trade-offs and non-goals

QuorumKV v1 is a fixed three-Node Cluster. A minority cannot make progress,
which is the deliberate availability trade-off for linearizability and safe
commitment. The project does not provide dynamic membership, transactions,
watches, TTLs, TLS, authentication, authorization, WAN tuning, Byzantine
fault tolerance, or production-readiness. Plaintext gRPC is intended only for
trusted development networks.

## Failure cases

A Leader crash before append leaves no mutation. A crash after local
persistence but before Quorum leaves an uncommitted entry that a later Leader
may overwrite. After Quorum persistence or commitment, Raft preserves the
entry and a retry returns its cached result. A lost response is therefore
ambiguous: retry the same Session and sequence rather than issuing a new
mutation. A stale Follower is repaired by conflict hints, or by Snapshot once
its required WAL history has been compacted.

Linearizable consistency means a completed write is visible to a later read,
and an isolated minority is unavailable. Eventual consistency could serve a
minority's stale local state, but would not provide that read-after-write
contract; QuorumKV intentionally chooses the former guarantee.
