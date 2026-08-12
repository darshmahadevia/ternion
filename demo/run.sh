#!/usr/bin/env sh
set -eu

compose='docker compose -f docker-compose.yml'
ctl="$compose run --rm --no-deps quorumkvctl"
cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

$compose build node-1
$compose up -d node-1
sleep 1
$compose up -d node-2
sleep 2
$compose up -d node-3

status() { $ctl -timeout 5s -address "${1}:740${1#node-}" status; }
wait_ready() {
  i=0
  while [ "$i" -lt 60 ]; do
    if status node-1 >/dev/null 2>&1 && status node-2 >/dev/null 2>&1 && status node-3 >/dev/null 2>&1; then return; fi
    i=$((i + 1)); sleep 1
  done
  echo 'Nodes did not become ready' >&2; exit 1
}
wait_ready

session=$($ctl -address node-1:7401 session open | sed -n 's/.*"session_id":"\([0-9a-f]*\)".*/\1/p')
test -n "$session"
echo '== CRUD =='
$ctl -address node-1:7401 set "$session" 1 greeting hello
$ctl -address node-2:7402 get greeting
$ctl -address node-3:7403 delete "$session" 2 greeting

leader=
i=0
while [ "$i" -lt 30 ]; do
  leader=$(status node-1 2>/dev/null | sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p' || true)
  [ -n "$leader" ] && break
  i=$((i + 1)); sleep 1
done
if [ -z "$leader" ]; then echo 'could not identify the Leader' >&2; exit 1; fi
majority=node-1
[ "$leader" = node-1 ] && majority=node-2
[ "$leader" = node-2 ] && majority=node-1
echo "== kill Leader $leader and continue on the majority =="
$compose kill "$leader"
i=0
while [ "$i" -lt 30 ]; do
  replacement=$(status "$majority" 2>/dev/null | sed -n 's/.*"leader_id":"\([^"]*\)".*/\1/p' || true)
  if [ -n "$replacement" ] && [ "$replacement" != "$leader" ]; then break; fi
  i=$((i + 1)); sleep 1
done
$ctl -timeout 15s -address "$majority:740${majority#node-}" set "$session" 3 after-failover majority-progress
$ctl -timeout 15s -address "$majority:740${majority#node-}" get after-failover
$compose start "$leader"

# The low demo threshold causes automatic snapshots. Stop one stale Follower,
# commit more entries on the majority, then let it recover from a Snapshot.
echo '== Snapshot-based stale-Follower recovery =='
old_snapshot=$(status node-3 | sed -n 's/.*"snapshot_index":\([0-9]*\).*/\1/p')
$compose stop node-3
payload=$(awk 'BEGIN { for (i = 0; i < 8192; i++) printf "x" }')
for sequence in 4 5 6 7 8; do
  $ctl -timeout 15s -address node-2:7402 set "$session" "$sequence" "snapshot-$sequence" "$payload"
done
$compose start node-3

i=0
while [ "$i" -lt 60 ]; do
  new_snapshot=$(status node-3 2>/dev/null | sed -n 's/.*"snapshot_index":\([0-9]*\).*/\1/p' || true)
  if [ -n "$new_snapshot" ] && [ "$new_snapshot" -gt "$old_snapshot" ]; then break; fi
  i=$((i + 1)); sleep 1
done
if [ -z "${new_snapshot:-}" ] || [ "$new_snapshot" -le "$old_snapshot" ]; then
  echo 'stale Follower did not install a newer Snapshot' >&2; exit 1
fi
status node-1
status node-2
status node-3

echo '== minority unavailability (stop one member of the two-node majority) =='
$compose stop node-1 node-2
if $ctl -address node-3:7403 --timeout 2s get greeting >/dev/null 2>&1; then
  echo 'unexpected minority success' >&2; exit 1
fi
echo 'minority rejected client work as expected'
