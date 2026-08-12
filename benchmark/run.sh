#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
mkdir -p "$root/.benchmark-work"
work=$(mktemp -d "$root/.benchmark-work/run.XXXXXX")
output=${1:-"$root/benchmark/results/latest.json"}
if [ "$#" -gt 0 ]; then shift; fi
mkdir -p "$(dirname "$output")"

pids=""
cleanup() {
  for pid in $pids; do
    if [ "$(go env GOOS)" = "windows" ]; then
      # go run owns a child executable on Windows; terminate the process tree.
      taskkill.exe //PID "$pid" //T //F >/dev/null 2>&1 || true
    else
      kill "$pid" 2>/dev/null || true
    fi
  done
  if [ "$(go env GOOS)" = "windows" ]; then
    # The shell PID is not always the go-run child PID under Git Bash.
    taskkill.exe //IM quorumkv.exe //T //F >/dev/null 2>&1 || true
  fi
  if [ "$(go env GOOS)" != "windows" ]; then
    for pid in $pids; do
      wait "$pid" 2>/dev/null || true
    done
  fi
  if [ "${KEEP_BENCHMARK_WORK:-}" = "1" ]; then
    printf 'benchmark work retained at %s\n' "$work" >&2
  else
    for attempt in 1 2 3 4 5; do
      rm -rf "$work" 2>/dev/null && break
      sleep 1
    done
    if [ -e "$work" ]; then
      printf 'warning: could not remove benchmark work directory %s\n' "$work" >&2
    fi
  fi
}
trap cleanup EXIT INT TERM

if [ "$(go env GOOS)" != "windows" ]; then
  node_binary="$work/quorumkv"
  bench_binary="$work/quorumkvbench"
  (
    cd "$root"
    go build -o "$node_binary" ./cmd/quorumkv
    go build -o "$bench_binary" ./cmd/quorumkvbench
  )
fi

for node in 1 2 3; do
  cat >"$work/node-$node.yaml" <<EOF
version: 1
cluster_id: durable-benchmark
active_session_limit: 1024
snapshot_threshold_bytes: 67108864
node:
  id: node-$node
  data_dir: data/node-$node
members:
  node-1:
    peer_address: 127.0.0.1:17301
    client_address: 127.0.0.1:17401
  node-2:
    peer_address: 127.0.0.1:17302
    client_address: 127.0.0.1:17402
  node-3:
    peer_address: 127.0.0.1:17303
    client_address: 127.0.0.1:17403
EOF
  if [ "$(go env GOOS)" = "windows" ]; then
    (cd "$root" && go run ./cmd/quorumkv -config "$work/node-$node.yaml") >"$work/node-$node.log" 2>&1 &
  else
    "$node_binary" -config "$work/node-$node.yaml" >"$work/node-$node.log" 2>&1 &
  fi
  pids="$pids $!"
done

# The benchmark client's bounded retry handles startup and Leader election.
if [ "$(go env GOOS)" = "windows" ]; then
  (cd "$root" && go run ./cmd/quorumkvbench \
    -addresses 127.0.0.1:17401,127.0.0.1:17402,127.0.0.1:17403 \
    -output "$output" "$@")
else
  "$bench_binary" \
    -addresses 127.0.0.1:17401,127.0.0.1:17402,127.0.0.1:17403 \
    -output "$output" "$@"
fi
printf 'wrote %s\n' "$output"
