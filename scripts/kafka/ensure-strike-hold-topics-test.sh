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

# 5) retention.bytes ABSENT entirely must be refused, not silently accepted.
mkdir -p "$TMP/bin"
cat > "$TMP/bin/kafka-configs" <<'KCX'
#!/usr/bin/env bash
echo "All configs for topic T are:"
echo "  cleanup.policy=delete sensitive=false synonyms={}"
echo "  retention.ms=-1 sensitive=false synonyms={}"
echo "  max.message.bytes=1048576 sensitive=false synonyms={}"
echo "  min.insync.replicas=1 sensitive=false synonyms={}"
echo "  local.retention.bytes=-2 sensitive=false synonyms={}"
KCX
chmod +x "$TMP/bin/kafka-configs"
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ]; then ok "a MISSING retention.bytes is refused (local.retention.bytes is not it)"
else bad "a topic with no retention.bytes was accepted"; fi

# 6) a PartitionCount lookalike must be refused, and its digits never read as the count.
#
# HONEST SCOPE: this arm does NOT distinguish the boundary read from the old loose `sed`, because
# the SHARED routine ensure_topic refuses this dump first — its own awk requires the exact token
# `PartitionCount:` and reports "PartitionCount=unknown" under either. So the local boundary in
# check_partitions is a second line of defence, not what catches this case, and the arm claims only
# what it checks: the dump is refused, and the lookalike's digits appear nowhere as a read count.
# Writing it as a regression test for the boundary would have been a test asserting what it does
# not check — it passed identically with the loose sed restored, which is how this was found.
mocks "compact,delete" "delete" "268435456" ""
cat > "$TMP/bin/kafka-topics" <<'KTX'
#!/usr/bin/env bash
case "$*" in
  *--list*) echo "options.spx.strike-hold.current"; echo "options.spx.strike-hold.scoring" ;;
  *--describe*) echo "Topic: t  NotPartitionCount: 9  ReplicationFactor: 1  Configs: cleanup.policy=x" ;;
esac
KTX
chmod +x "$TMP/bin/kafka-topics"
out="$(run)"; rc=$?
if [ "$rc" -ne 0 ] && ! printf '%s' "$out" | grep -q "PartitionCount=9"; then
  ok "a 'NotPartitionCount' lookalike is refused, and its 9 is never read as the count"
else
  bad "the lookalike was accepted or its value was read:"; printf '%s\n' "$out" | grep -E "PartitionCount" | head -2
fi

# 8) THE COVERAGE INPUT, when the deployment names it.
#
# Kafka Streams will not start without a source topic, so a strike-hold rolled before the engine's
# barrier created es.mbo.coverage CRASH-LOOPS instead of reporting
# ABSORPTION_COVERAGE_UNAVAILABLE — and schema sync registers the subject without creating the
# topic, so nothing else on that path catches it (Codex deploy r4).
cov_mocks() { # cov_mocks <listCoverage: yes|no> <coveragePolicy> <coverageRetentionMs> [coverageRetentionBytes=-1]
  mkdir -p "$TMP/bin"
  cat > "$TMP/bin/kafka-topics" <<KT
#!/usr/bin/env bash
case "\$*" in
  *--list*)
    echo "options.spx.strike-hold.current"; echo "options.spx.strike-hold.scoring"
    # The trailing true matters: a bare test-and-echo is the LAST command of this branch, so when
    # the test does not want coverage listed the mock exits 1 and the barrier reports "cannot list
    # topics" -- a mock failing, read as a barrier passing. Same class as the line break that broke
    # the engine suite's list mock. (No backticks in this comment: the heredoc is unquoted, so a
    # backtick here is command substitution, which is how the first version of this fix printed
    # three syntax errors while still reporting every arm green.)
    if [ "$1" = yes ]; then echo "es.mbo.coverage"; fi
    ;;
  *--describe*) echo "Topic: t  TopicId: a  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=x" ;;
esac
KT
  cat > "$TMP/bin/kafka-configs" <<KC
#!/usr/bin/env bash
$(declare -f dump)
case "\$*" in
  *es.mbo.coverage*) dump "$2" "${4:--1}" | sed 's/retention.ms=-1/retention.ms=$3/' ;;
  *scoring*)         dump "delete" "268435456" ;;
  *)                 dump "compact,delete" "-1" ;;
esac
KC
  chmod +x "$TMP/bin/kafka-topics" "$TMP/bin/kafka-configs"
}
run_cov() { PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 \
            KAFKA_STRIKE_HOLD_COVERAGE_TOPIC=es.mbo.coverage bash "$BAR" 2>&1; }

cov_mocks no compact -1
out="$(run_cov)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "CRASH-LOOP"; then
  ok "a configured but ABSENT coverage topic refuses the deploy and names the engine's barrier"
else bad "strike-hold would have rolled with its coverage source missing"; fi

cov_mocks yes compact -1
out="$(run_cov)"; rc=$?
if [ "$rc" -eq 0 ]; then ok "a present, pure-compact coverage topic is accepted"
else bad "the correct coverage input was refused:"; printf '%s\n' "$out" | grep -E "^FAIL" | head -2; fi

cov_mocks yes delete -1
out="$(run_cov)"; rc=$?
if [ "$rc" -ne 0 ]; then ok "a coverage topic that is NOT compacted is refused"
else bad "a delete-retention coverage input was accepted"; fi

# A BYTE CAP on the coverage input. The shared routine does not look at retention.bytes, so this
# shape — compact, retention.ms=-1, one partition, 256 MiB cap — passed the entire check until the
# barrier gained its own exact comparison. A cap ages out the oldest contract's interval, and a
# reader finding no coverage for a contract reads it as "never observed" (Codex deploy r5).
cov_mocks yes compact -1 268435456
out="$(run_cov)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "retention.bytes=268435456, expected exactly -1"; then
  ok "a retention.bytes CAP on the coverage input is refused"
else bad "a capped coverage input was accepted — the interval can age out"; fi

# and BLANK stays legitimate: no coverage source configured is a documented configuration, in which
# the service reports ABSORPTION_COVERAGE_UNAVAILABLE and never claims an absence.
cov_mocks no compact -1
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 \
        KAFKA_STRIKE_HOLD_COVERAGE_TOPIC= bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then ok "a BLANK coverage topic is not a deploy refusal"
else bad "a blank coverage topic was turned into a refusal"; fi

# 7) DEV ONLY, fail-closed.
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "DEV ONLY"; then ok "refuses outside dev"
else bad "did not refuse outside dev"; fi

printf '\nensure-strike-hold-topics-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
