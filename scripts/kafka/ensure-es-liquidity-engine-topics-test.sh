#!/usr/bin/env bash
# ensure-es-liquidity-engine-topics-test.sh — drives the REAL engine barrier against mocked Kafka
# CLIs, the way ensure-strike-hold-topics-test.sh and ensure-es-liquidity-wall-topics-test.sh do.
#
# WHY IT EXISTS. The barrier had no mocked suite at all: the only coverage was a Python test
# asserting that the Jenkinsfile mentions the script and that the script mentions the shared
# routine. Both pass however the shapes are wrong. So when es.mbo.coverage was added to the barrier,
# nothing proved it was created, nothing proved it was verified, and nothing proved a drifted shape
# was refused — the new topic was asserted TEXTUALLY and no more (Codex deploy r2).
#
# THE COVERAGE TOPIC IS THE ONE THAT MATTERS HERE. It is pure compact, keyed by contract, because
# the newest statement per contract IS the whole answer; created as a delete-retention log instead,
# it would still accept writes and still be read, and the oldest run would outlive the newest.
# Nothing downstream could tell.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BAR="$HERE/ensure-es-liquidity-engine-topics.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }

# ONE LINE on purpose: this string is interpolated into the generated mock, so a line break in it
# lands inside that script's printf and silently breaks --list — which the first run did, reporting
# "cannot list topics" for every arm.
ALL="es.mbo.level-flow es.mbo.snapshot es.mbo.coverage es.mbo.closures es.mbo.liquidity-events es.mbo.level-flow-final es.mbo.level-base es.mbo.health"
WITHOUT_COVERAGE="es.mbo.level-flow es.mbo.snapshot es.mbo.closures es.mbo.liquidity-events es.mbo.level-flow-final es.mbo.level-base es.mbo.health"

# A dev dump in the REAL shape, synonyms included: a mock printing only the keys the script reads
# would pass whatever the duplicate and boundary guards did.
write_mocks() { # write_mocks <listedTopics> <coveragePolicy> <coverageRetMs> <coverageRetBytes> <coveragePartitions>
  local listed="$1" cpol="$2" cms="$3" cbytes="$4" cparts="$5"
  mkdir -p "$TMP/bin"; : > "$TMP/calls"
  cat > "$TMP/bin/kafka-topics" <<KT
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$TMP/calls"
case "\$*" in
  *--list*) printf '%s\n' $listed ;;
  *--create*) exit 0 ;;
  *--describe*)
    parts=1
    case "\$*" in *es.mbo.coverage*) parts=$cparts ;; esac
    echo "Topic: t  TopicId: a  PartitionCount: \$parts  ReplicationFactor: 1  Configs: cleanup.policy=x"
    ;;
esac
KT
  cat > "$TMP/bin/kafka-configs" <<KC
#!/usr/bin/env bash
echo "kafka-configs \$*" >> "$TMP/calls"
pol=delete; ms=86400000; bytes=268435456
case "\$*" in
  *es.mbo.coverage*)        pol=$cpol;  ms=$cms;   bytes=$cbytes ;;
  *es.mbo.snapshot*)        pol=compact; ms=-1;    bytes=-1 ;;
  *es.mbo.closures*)        pol=compact; ms=-1;    bytes=-1 ;;
  *es.mbo.liquidity-events*) pol=delete; ms=-1;    bytes=134217728 ;;
  *es.mbo.health*)          pol=delete;  ms=86400000; bytes=-1 ;;
esac
cat <<DUMP
All configs for topic T are:
  compression.type=producer sensitive=false synonyms={DEFAULT_CONFIG:compression.type=producer}
  min.insync.replicas=1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=1}
  local.retention.ms=-2 sensitive=false synonyms={DEFAULT_CONFIG:log.local.retention.ms=-2}
  cleanup.policy=\$pol sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=\$pol, DEFAULT_CONFIG:log.cleanup.policy=delete}
  local.retention.bytes=-2 sensitive=false synonyms={DEFAULT_CONFIG:log.local.retention.bytes=-2}
  retention.bytes=\$bytes sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=\$bytes, DEFAULT_CONFIG:log.retention.bytes=-1}
  retention.ms=\$ms sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=\$ms}
  delete.retention.ms=86400000 sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.delete.retention.ms=86400000}
  max.message.bytes=1048576 sensitive=false synonyms={DEFAULT_CONFIG:message.max.bytes=1048576}
DUMP
KC
  chmod +x "$TMP/bin/kafka-topics" "$TMP/bin/kafka-configs"
}

run() { PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1; }

# 1) EVERY declared topic present and correct must PASS — the baseline, so a later refusal means
#    something, and so the real dump shape with synonyms is exercised.
write_mocks "$ALL" compact -1 -1 1
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then ok "the declared shapes, in a real dump with synonyms, are accepted"
else bad "the correct shapes were refused:"; printf '%s\n' "$out" | grep -E '^FAIL' | head -3; fi

# 2) FRESH CLUSTER: the coverage topic is CREATED, and created with its declared shape — one
#    partition, compact, retention.ms=-1, retention.bytes=-1. Not merely "created".
write_mocks "$WITHOUT_COVERAGE" compact -1 -1 1
out="$(run)"; rc=$?
create="$(grep -- '--create' "$TMP/calls" | grep -- 'es.mbo.coverage' || true)"
if [ "$rc" -eq 0 ] && [ -n "$create" ] \
   && grep -q -- '--partitions 1' <<<"$create" \
   && grep -q -- 'cleanup.policy=compact' <<<"$create" \
   && grep -q -- 'retention.ms=-1' <<<"$create" \
   && grep -q -- 'retention.bytes=-1' <<<"$create"; then
  ok "on a fresh cluster the coverage topic is created pure compact, 1 partition, -1/-1"
else
  bad "the fresh-cluster create was absent or the wrong shape: ${create:-<no create call>}"
fi

# 3) a DELETE-retention coverage topic is refused. This is the drift that matters: it would accept
#    writes and be read, while the oldest run outlived the newest.
write_mocks "$ALL" delete -1 -1 1
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "cleanup.policy"; then
  ok "a coverage topic with cleanup.policy=delete is refused"
else bad "a delete-retention coverage topic was accepted"; fi

# 4) a BYTE CAP on the coverage topic is refused: a cap silently drops the oldest contract's run.
write_mocks "$ALL" compact -1 268435456 1
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "retention.bytes=268435456, expected exactly -1"; then
  ok "a retention.bytes cap on the coverage topic is refused"
else bad "a capped coverage topic was accepted"; fi

# 5) a TIME retention on the coverage topic is refused.
write_mocks "$ALL" compact 86400000 -1 1
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ]; then ok "a finite retention.ms on the coverage topic is refused"
else bad "a time-retained coverage topic was accepted"; fi

# 6) MORE THAN ONE PARTITION is refused. Kafka can add partitions but never remove them, so this is
#    the one drift that cannot be undone — and the interval is keyed by contract, so a second
#    partition would split one contract's runs across two ordered logs.
write_mocks "$ALL" compact -1 -1 2
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -qi "partition"; then
  ok "a coverage topic with two partitions is refused"
else bad "a two-partition coverage topic was accepted"; fi

# 7) DEV ONLY, fail-closed: the engine has no production topics.
write_mocks "$ALL" compact -1 -1 1
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "DEV ONLY"; then ok "refuses outside dev"
else bad "did not refuse outside dev"; fi

printf '\nensure-es-liquidity-engine-topics-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
