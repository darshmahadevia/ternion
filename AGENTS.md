# Repository Guidelines

## Go engineering standards

- Prefer simple, explicit Go over clever abstractions.
- Keep packages cohesive and APIs narrow; hide implementation details under `internal`.
- Keep the Raft core deterministic and independent of networking, storage, timers, and generated Protobuf types.
- Make concurrency ownership explicit; avoid shared mutable state and leaked goroutines.
- Propagate `context.Context` across I/O boundaries and wrap errors with useful context.
- Use meaningful names and comments that explain intent or invariants, not syntax.
- Test observable behavior at the highest practical seam.
- Run formatting, tests, race detection, and static analysis before considering a ticket complete.
- Deliver changes as thin, independently verifiable vertical slices with green CI.

## Agent skills

### Issue tracker

Specs and tickets live in GitHub Issues for `Het-Jethva/quorumkv`. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the canonical triage-label mapping. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repository using root `CONTEXT.md` and `docs/adr/`. See `docs/agents/domain.md`.
