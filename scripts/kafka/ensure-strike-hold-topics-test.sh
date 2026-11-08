#!/usr/bin/env bash
# ensure-strike-hold-topics-test.sh — drives the REAL barrier against mocked Kafka CLIs.
#
# It exists because of a live failure. On the first real deploy (service-deploy 3247) the barrier
# created both topics with exactly the right shapes and then REFUSED them, reporting
# "config of 'options.spx.strike-hold.scoring' carries 5 retention.bytes values". The duplicate
# guard counted a bare `retention.bytes=` substring, which on a real `kafka-configs --describe
# --all` dump matches five times: the value, `local.retention.bytes`, and three occurrences inside
# the `synonyms={...}` text. The fix counts with the SAME boundary `extract` uses.
#
# The arm that matters most is the first: a byte-accurate dev dump, synonyms included. A mock that
# printed only the handful of keys the script reads would have passed throughout and taught nothing.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BAR="$HERE/ensure-strike-hold-topics.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pass=0; fail=0
ok()   { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }

# A dev config dump, in the real shape: every broker default, plus synonyms.
dump() { # dump <policy> <retentionBytes>
  cat <<DUMP
All configs for topic T are:
  compression.type=producer sensitive=false synonyms={DEFAULT_CONFIG:compression.type=producer}
  min.insync.replicas=1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=1, STATIC_BROKER_CONFIG:min.insync.replicas=1}
  local.retention.ms=-2 sensitive=false synonyms={DEFAULT_CONFIG:log.local.retention.ms=-2}
  cleanup.policy=$1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=$1, DEFAULT_CONFIG:log.cleanup.policy=delete}
  local.retention.bytes=-2 sensitive=false synonyms={DEFAULT_CONFIG:log.local.retention.bytes=-2}
  retention.bytes=$2 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=$2, DEFAULT_CONFIG:log.retention.bytes=-1}
  retention.ms=-1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=-1}
  delete.retention.ms=86400000 sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.delete.retention.ms=86400000}
  max.message.bytes=1048576 sensitive=false synonyms={DEFAULT_CONFIG:message.max.bytes=1048576}
DUMP
}

mocks() { # mocks <currentPolicy> <scoringPolicy> <scoringBytes> [extraRetentionBytesLine]
  mkdir -p "$TMP/bin"
  cat > "$TMP/bin/kafka-topics" <<KT
#!/usr/bin/env bash
args="\$*"
case "\$args" in
  *--list*) echo "options.spx.strike-hold.current"; echo "options.spx.strike-hold.scoring" ;;
  *--describe*) echo "Topic: t  TopicId: a  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=x" ;;
esac
KT
  cat > "$TMP/bin/kafka-configs" <<KC
#!/usr/bin/env bash
if [[ "\$*" == *scoring* ]]; then
$(declare -f dump)
  dump "$2" "$3"
  $4
else
$(declare -f dump)
  dump "$1" "-1"
fi
KC
  chmod +x "$TMP/bin/kafka-topics" "$TMP/bin/kafka-configs"
}

run() { PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1; }

# 1) THE REGRESSION: the real dump shape, correct topics, must PASS.
mocks "compact,delete" "delete" "268435456" ""
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then ok "a real --describe --all dump with synonyms is accepted (the 3247 regression)"
else bad "the real dump shape was refused:"; printf '%s\n' "$out" | grep -E "^FAIL" | head -3; fi

# 2) a GENUINE duplicate must still be refused — the guard must not have been loosened away.
mocks "compact,delete" "delete" "268435456" 'echo "  retention.bytes=999 sensitive=false synonyms={}"'
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "carries 2 retention.bytes values"; then
  ok "two REAL retention.bytes values are still refused, not chosen between"
else bad "a genuine duplicate was accepted — the guard is now useless"; fi

# 3) the evidence topic must never be compacted.
mocks "compact,delete" "compact,delete" "268435456" ""
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "expected exactly 'delete'"; then
  ok "a COMPACTED scoring topic is refused (it is the evidence record)"
else bad "a compacted evidence topic was accepted"; fi

# 4) the wrong byte cap is refused.
mocks "compact,delete" "delete" "999" ""
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "retention.bytes=999, expected exactly 268435456"; then
  ok "a wrong retention.bytes is refused"
else bad "a wrong retention.bytes was accepted"; fi

# 5) DEV ONLY, fail-closed.
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "DEV ONLY"; then ok "refuses outside dev"
else bad "did not refuse outside dev"; fi

printf '\nensure-strike-hold-topics-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
