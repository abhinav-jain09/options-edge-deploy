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

# kafka-get-offsets: drives which partitions the wire check looks at.
#   MOCK_EARLIEST / MOCK_LATEST — "<topic>:<partition>:<offset>" lines. A partition whose latest
#   exceeds its earliest is non-empty, which is how rf_nonempty_partitions decides.
cat > "$TMP/bin/kafka-get-offsets.sh" <<'MOCK'
#!/usr/bin/env bash
set -u
for a in "$@"; do case "$a" in -2) printf '%s\n' "${MOCK_EARLIEST:-}" | grep -v '^$'; exit "${MOCK_OFFSETS_RC:-0}" ;; esac; done
printf '%s\n' "${MOCK_LATEST:-}" | grep -v '^$'
exit "${MOCK_OFFSETS_RC:-0}"
MOCK
chmod +x "$TMP/bin/kafka-get-offsets.sh"

# kafka-console-consumer: emits the bytes the wire check inspects.
#   MOCK_WIRE — "avro" (0x00 frame), "json" ({...), or "empty" (nothing, i.e. unreadable).
cat > "$TMP/bin/kafka-console-consumer.sh" <<'MOCK'
#!/usr/bin/env bash
set -u
# MOCK_WIRE applies to every offset; MOCK_WIRE_AT_<offset> overrides one, which is how the
# "Avro at the start, JSON at the end" case is expressed.
off=""
prev=""
for a in "$@"; do [ "$prev" = "--offset" ] && off="$a"; prev="$a"; done
eval "specific=\${MOCK_WIRE_AT_${off}:-}"
kind="${specific:-${MOCK_WIRE:-avro}}"
case "$kind" in
  avro) printf '\000\000\000\000\001some-avro-body' ;;
  json) printf '{"emitterEpoch":1,"state":"GATED"}' ;;
  tombstone) printf 'null\n' ;;
  empty) : ;;
esac
exit 0
MOCK
chmod +x "$TMP/bin/kafka-console-consumer.sh"

PRE_ID=Etuw6A2-RZiWnbq4oPLHLw            # the es4 pre-cutover id, from avro-rollforward.lib.sh
T=es.options.databento.gex.oi-status
GOOD_CFG='min.insync.replicas=1,cleanup.policy=delete,segment.bytes=268435456,retention.ms=43200000'
desc() { printf '%s=Topic: %s\tTopicId: %s\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: %s\n' "$T" "$T" "$1" "$2" "$3"; }

# snap <records-in-partition-0> -> full 32-partition earliest/latest snapshots, exported as
# SNAP_EARLY / SNAP_LATE. Every declared partition must appear or the barrier refuses the response
# as partial, so a case that wants ONE non-empty partition still has to name all 32.
snap() {
  local n="$1" i
  SNAP_EARLY=""; SNAP_LATE=""
  for i in $(seq 0 31); do
    SNAP_EARLY="$SNAP_EARLY$T:$i:0
"
    if [ "$i" = 0 ]; then SNAP_LATE="$SNAP_LATE$T:$i:$n
"; else SNAP_LATE="$SNAP_LATE$T:$i:0
"; fi
  done
}

pass=0; fail=0
run() { # <name> <expected rc> ; env comes from the caller
  # Default the wire inputs to "one non-empty partition carrying Avro" so the pre-existing cases
  # keep testing what they were written to test; the wire cases below override them.
  local name="$1" want="$2" out rc
  # The default snapshots name ALL 32 declared partitions, because a partial response is now refused.
  local all_early all_late i
  all_early=""; all_late=""
  for i in $(seq 0 31); do
    all_early="$all_early$T:$i:0\n"
    all_late="$all_late$T:$i:$([ "$i" = 0 ] && echo 5 || echo 0)\n"
  done
  out="$(MOCK_EARLIEST="${MOCK_EARLIEST-$(printf "$all_early")}" MOCK_LATEST="${MOCK_LATEST-$(printf "$all_late")}" \
         MOCK_WIRE="${MOCK_WIRE-avro}" \
         RF_ALLOW_VERSIONED_SIBLINGS="${RF_ALLOW_VERSIONED_SIBLINGS:-0}" \
         PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" --cluster es4 mock:9092 "$TMP/bin" 2>&1)"
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

# The PREFIX traps: an unanchored substring grep would accept every one of these.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'min.insync.replicas=1,cleanup.policy=delete,segment.bytes=268435456,retention.ms=432000000')" \
  run "retention.ms=432000000 is refused where 43200000 is declared (prefix trap)" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'min.insync.replicas=10,cleanup.policy=delete,segment.bytes=268435456,retention.ms=43200000')" \
  run "min.insync.replicas=10 is refused where 1 is declared (prefix trap)" 1

# cleanup.policy=compact,delete must NOT satisfy a declared cleanup.policy=delete: the comma makes
# the value two words, and the declared value is the whole of it.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'min.insync.replicas=1,cleanup.policy=compact,delete,segment.bytes=268435456,retention.ms=43200000')" \
  run "cleanup.policy=compact,delete is refused where delete is declared" 1

# And a config that is ABSENT from the broker, rather than wrong, is refused too.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 'cleanup.policy=delete,retention.ms=43200000')" \
  run "a MISSING declared config (min.insync.replicas) is refused" 1

# THE WIRE FORMAT — the check that caught a real, self-inflicted defect on 2026-10-18. The dev topic
# was recreated at 15:10 and the still-running PRE-CUTOVER image wrote 31 hand-built JSON heartbeats
# into it before the new image rolled at 15:59. The topic id was post-wipe the whole time, so an
# id-only barrier passed over exactly the retained JSON the rule forbids an Avro consumer to meet.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" MOCK_WIRE=json \
  run "a JSON value written AFTER the wipe is refused (post-wipe contamination)" 1

MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" MOCK_WIRE=avro \
  run "an Avro value at the earliest offset passes" 0

# An EMPTY topic has no non-empty partition, so there is nothing to read and nothing to refuse.
snap 0
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=json \
  run "an EMPTY topic passes (no record to classify)" 0

# A non-empty partition that cannot be read is refused rather than assumed Avro.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" MOCK_WIRE=empty \
  run "a non-empty partition that reads back nothing is refused" 1

# And a failure of the offset query itself must not read as "no non-empty partitions".
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" MOCK_OFFSETS_RC=1 \
  run "a kafka-get-offsets failure is refused, not read as an empty topic" 1

# THE DECISIVE CASE Codex found missing: Avro at the START, retained JSON at the END. An
# earliest-only check passes this, and an Avro consumer still meets the JSON.
snap 5
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=avro MOCK_WIRE_AT_4=json \
  run "Avro at the start and JSON at the END is refused" 1

# A TOMBSTONE is a valid record, not malformed. options.ibkr.gex.status is pure compact and retains
# tombstones for 48 h by design, so refusing one would block a correct topic.
snap 5
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=tombstone \
  run "a TOMBSTONE passes (null value is valid, not malformed)" 0

# THE TRANSACTION MARKER AT THE LOG END, which failed a correct topic twice on 19 Oct and skipped
# the production Deploy stage both times. The offset below the high watermark was a commit marker:
# it occupies an offset but is delivered in neither isolation level, so the fixed read there
# returned nothing and the gate called the topic unverifiable. The walk-back judges the last record
# that actually delivers.
snap 5
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=avro MOCK_WIRE_AT_4=empty \
  run "a transaction MARKER at the log end passes on the Avro record below it" 0

# More than one marker at the end: consecutive transactions each leave their own, so the walk is not
# a single step back.
snap 5
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=avro \
  MOCK_WIRE_AT_4=empty MOCK_WIRE_AT_3=empty \
  run "SEVERAL markers at the log end pass on the Avro record below them" 0

# THE CASE THAT MUST NOT BE WEAKENED BY THE WALK-BACK: markers at the end and retained JSON under
# them. The walk stops at the first offset that DELIVERS, so the JSON is still met and still fails.
# If this ever passes, the fix has become a way to hide contamination behind a marker.
snap 5
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=avro \
  MOCK_WIRE_AT_4=empty MOCK_WIRE_AT_3=json \
  run "a marker at the end over retained JSON is still REFUSED" 1

# The walk is BOUNDED. A partition whose last RF_LOG_END_PROBE_BACK offsets all deliver nothing is
# refused, not walked down the whole log until something passes: here the only readable record sits
# below the window, so the log end stays unverified.
snap 20
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=empty MOCK_WIRE_AT_10=avro \
  run "a record BELOW the probe window does not rescue the log end" 1

# And the same record, once the window is widened to reach it, does.
snap 20
RF_LOG_END_PROBE_BACK=9 \
  MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=empty MOCK_WIRE_AT_10=avro \
  run "RF_LOG_END_PROBE_BACK widens the window to that record" 0

# The walk never probes below the partition's EARLIEST offset, so a partition shorter than the
# window cannot be read at a negative offset.
snap 2
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$SNAP_EARLY" MOCK_LATEST="$SNAP_LATE" MOCK_WIRE=empty \
  run "a partition shorter than the probe window is refused, not probed below earliest" 1

# A PARTIAL offsets response must not read as authoritative: it would omit a contaminated partition.
MOCK_LIST="$T" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  MOCK_EARLIEST="$T:0:0
" MOCK_LATEST="$T:0:5
" \
  run "a PARTIAL offsets response (1 of 32 partitions) is refused" 1

# A versioned sibling present is REPORTED, not fatal — the documented exception, asserted so a later
# edit cannot quietly make it fatal and deadlock the cutover rollout.
MOCK_LIST="$(printf '%s\n%s\n' "$T" "$T.v2")" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  run "a versioned sibling present FAILS by default (owner rule)" 1

RF_ALLOW_VERSIONED_SIBLINGS=1 \
  MOCK_LIST="$(printf '%s\n%s\n' "$T" "$T.v2")" MOCK_DESCRIBE="$(desc NEWIDxxxxxxxxxxxxxxxxx 32 "$GOOD_CFG")" \
  run "a versioned sibling is allowed ONLY with the explicit cutover-window opt-out" 0

# The cluster argument is mandatory: without it the script cannot know which topics apply.
out="$(PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" mock:9092 2>&1)"; rc=$?
if [ "$rc" = 2 ]; then pass=$((pass+1)); echo "ok    a missing --cluster is refused (rc=2)"
else fail=$((fail+1)); echo "FAIL  a missing --cluster should exit 2, got rc=$rc"; fi

out="$(PATH="$TMP/bin:$PATH" bash "$HERE/verify-avro-rollforward.sh" --cluster nope mock:9092 "$TMP/bin" 2>&1)"; rc=$?
if [ "$rc" != 0 ]; then pass=$((pass+1)); echo "ok    an unknown cluster is refused (rc=$rc)"
else fail=$((fail+1)); echo "FAIL  an unknown cluster was accepted"; fi

echo "=== $pass passed, $fail failed ==="
[ "$fail" -eq 0 ] || exit 1
