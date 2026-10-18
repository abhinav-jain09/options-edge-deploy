#!/usr/bin/env bash
# verify-avro-rollforward-test.sh — the barrier's FAILURE paths, against a mocked kafka-topics.
#
# The barrier is only worth having if it refuses. Its happy path is easy to see working against a
# live broker; what needs a test is every way it could wrongly PASS — Codex round 2 found two of
# those by reading it (an absent topic admitted as success, and a `--list` failure indistinguishable
# from an absent topic inside a conditional). Each case below is one of those, plus the ones a
# reviewer would ask about next.
#
# The mock is a kafka-topics.sh on PATH whose behaviour is driven by MOCK_* variables, so the test
# exercises the REAL script rather than a reimplementation of its rules.
#
# Run: bash scripts/kafka/verify-avro-rollforward-test.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"

cat > "$TMP/bin/kafka-topics.sh" <<'MOCK'
#!/usr/bin/env bash
# MOCK_LIST_RC      — exit code for --list (default 0)
# MOCK_LIST         — newline-separated topic names for --list
# MOCK_DESCRIBE     — "<topic>=<describe first line>" entries, one per line
# MOCK_DESCRIBE_RC  — exit code for --describe (default 0)
set -u
mode=""; topic=""
for a in "$@"; do
  case "$a" in --list) mode=list ;; --describe) mode=describe ;; esac
done
prev=""
for a in "$@"; do
  [ "$prev" = "--topic" ] && topic="$a"
  prev="$a"
done
if [ "$mode" = list ]; then
  printf '%s\n' "${MOCK_LIST:-}" | grep -v '^$' || true
  exit "${MOCK_LIST_RC:-0}"
fi
if [ "$mode" = describe ]; then
  line="$(printf '%s\n' "${MOCK_DESCRIBE:-}" | grep "^${topic}=" | head -1 | cut -d= -f2-)"
  if [ -z "$line" ]; then
    echo "Error while executing topic command : Topic '$topic' does not exist as expected" >&2
    exit 1
  fi
  printf '%s\n' "$line"
  exit "${MOCK_DESCRIBE_RC:-0}"
fi
exit 0
MOCK
chmod +x "$TMP/bin/kafka-topics.sh"

PRE_ID=Etuw6A2-RZiWnbq4oPLHLw            # the es4 pre-cutover id, from avro-rollforward.lib.sh
T=es.options.databento.gex.oi-status
GOOD_CFG='min.insync.replicas=1,cleanup.policy=delete,segment.bytes=268435456,retention.ms=43200000'
desc() { printf '%s=Topic: %s\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: %s\n' "$T" "$T" "$1" "$2" "$3"; }

pass=0; fail=0
run() { # <name> <expected rc> ; env comes from the caller
  local name="$1" want="$2" out rc
  out="$(PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" --cluster es4 mock:9092 "$TMP/bin" 2>&1)"
  rc=$?
  if [ "$rc" = "$want" ]; then
    pass=$((pass+1)); echo "ok    $name (rc=$rc)"
  else
    fail=$((fail+1)); echo "FAIL  $name: expected rc=$want, got rc=$rc"; printf '%s\n' "$out" | sed 's/^/        /'
  fi
}

echo "=== verify-avro-rollforward failure paths ==="

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  run "a rolled-forward topic with the declared shape PASSES" 0

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc "$PRE_ID" 32 "$GOOD_CFG")" \
  run "the PRE-CUTOVER topic id is refused" 1

MOCK_LIST="" MOCK_DESCRIBE="" \
  run "an ABSENT topic is refused, not admitted as success" 1

MOCK_LIST_RC=1 MOCK_LIST="" MOCK_DESCRIBE="" \
  run "a --list failure is refused, not read as an empty cluster" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$T=Topic: $T	PartitionCount: 32	Configs: $GOOD_CFG" \
  run "a describe with NO topic id is refused, not parsed as empty" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 1 "$GOOD_CFG")" \
  run "the wrong PARTITION COUNT is refused (broker auto-create default)" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'min.insync.replicas=1,cleanup.policy=compact,retention.ms=43200000')" \
  run "the wrong CLEANUP POLICY is refused" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'min.insync.replicas=1,cleanup.policy=delete,retention.ms=604800000')" \
  run "the wrong RETENTION is refused" 1

# A versioned sibling present is REPORTED, not fatal — the documented exception, asserted so a later
# edit cannot quietly make it fatal and deadlock the cutover rollout.
MOCK_LIST="$(printf '%s\n%s\n' "$T" "$T.v2")" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  run "a versioned sibling present does NOT fail the barrier" 0

# The cluster argument is mandatory: without it the script cannot know which topics apply.
out="$(PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" mock:9092 2>&1)"; rc=$?
if [ "$rc" = 2 ]; then pass=$((pass+1)); echo "ok    a missing --cluster is refused (rc=2)"
else fail=$((fail+1)); echo "FAIL  a missing --cluster should exit 2, got rc=$rc"; fi

out="$(PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" --cluster nope mock:9092 "$TMP/bin" 2>&1)"; rc=$?
if [ "$rc" != 0 ]; then pass=$((pass+1)); echo "ok    an unknown cluster is refused (rc=$rc)"
else fail=$((fail+1)); echo "FAIL  an unknown cluster was accepted"; fi

echo "=== $pass passed, $fail failed ==="
[ "$fail" -eq 0 ] || exit 1
