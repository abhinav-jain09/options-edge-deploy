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
  # es.mbo.coverage is listed because the barrier now reads the topic name from the DEPLOYMENT
  # MANIFEST, which names it — so every arm here runs the way a real deploy does, with the coverage
  # input required. The earlier version of these mocks omitted it and passed only because the
  # barrier was reading an environment variable the Jenkins stage never exports.
  *--list*) echo "options.spx.strike-hold.current"; echo "options.spx.strike-hold.scoring"; echo "es.mbo.coverage" ;;
  *--describe*) echo "Topic: t  TopicId: a  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=x" ;;
esac
KT
  cat > "$TMP/bin/kafka-configs" <<KC
#!/usr/bin/env bash
$(declare -f dump)
if [[ "\$*" == *scoring* ]]; then
  dump "$2" "$3"
  $4
elif [[ "\$*" == *es.mbo.coverage* ]]; then
  dump "compact" "-1"
else
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
  # es.mbo.coverage is listed because the barrier now reads the topic name from the DEPLOYMENT
  # MANIFEST, which names it — so every arm here runs the way a real deploy does, with the coverage
  # input required. The earlier version of these mocks omitted it and passed only because the
  # barrier was reading an environment variable the Jenkins stage never exports.
  *--list*) echo "options.spx.strike-hold.current"; echo "options.spx.strike-hold.scoring"; echo "es.mbo.coverage" ;;
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

# 9) THE DEPLOY PATH ITSELF: with NO environment variable set — which is how the Jenkins stage runs
#    it, since that stage sources only load-kafka-settings.sh — the barrier must still require the
#    coverage topic, because the DEPLOYMENT MANIFEST names it. This is the arm that would have
#    caught the original gap: every check was being skipped on every real deploy while the suite set
#    the variable by hand and reported them green (Codex deploy r6).
cov_mocks no compact -1
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "es.mbo.coverage"; then
  ok "with NO env var set, the manifest's coverage topic is still required (the real deploy path)"
else bad "the real deploy path skipped the coverage check entirely"; fi

# 10) THE MANIFEST PARSE, against YAML it does and does not understand.
#
# The parse is an awk anchored on '- name: <KEY>' and the 'value:' line after it. That is the style
# this repo uses, and the arms below pin what happens with anything else: a QUOTED value is read
# (quotes stripped), while a flow-style entry, a comment between the key and its value, and a
# valueFrom reference are all REFUSED — never read as "no coverage source configured", which is the
# silent skip the manifest read was introduced to replace.
with_manifest() { # with_manifest <yaml body for the env entry>
  rm -rf "$TMP/repo"
  mkdir -p "$TMP/repo/scripts/kafka" "$TMP/repo/k8s/services/strike-hold/base"
  cp "$HERE/ensure-strike-hold-topics.sh" "$HERE/topic-config-parse.sh" \
     "$HERE/ensure-topic-contract.lib.sh" "$TMP/repo/scripts/kafka/"
  {
    echo "spec:"; echo "  template:"; echo "    spec:"; echo "      containers:"
    echo "        - name: strike-hold"; echo "          env:"
    printf '%s\n' "$1"
  } > "$TMP/repo/k8s/services/strike-hold/base/strike-hold-deployment.yaml"
  PATH="$TMP/bin:$PATH" ENVIRONMENT=dev KAFKA_BOOTSTRAP_SERVERS=mock:9092 \
    bash "$TMP/repo/scripts/kafka/ensure-strike-hold-topics.sh" 2>&1
}

cov_mocks yes compact -1
out="$(with_manifest '            - name: KAFKA_STRIKE_HOLD_COVERAGE_TOPIC
              value: "es.mbo.coverage"')"; rc=$?
if [ "$rc" -eq 0 ]; then ok "a QUOTED value is read, quotes stripped, and the topic verified"
else bad "a quoted value was not understood:"; printf '%s\n' "$out" | grep -E "^FAIL" | head -2; fi

for bad_yaml in \
  '            - {name: KAFKA_STRIKE_HOLD_COVERAGE_TOPIC, value: es.mbo.coverage}' \
  '            - name: KAFKA_STRIKE_HOLD_COVERAGE_TOPIC
              # a comment between the key and its value
              value: es.mbo.coverage' \
  '            - name: KAFKA_STRIKE_HOLD_COVERAGE_TOPIC
              valueFrom:
                configMapKeyRef: {name: cm, key: coverage}'
do
  out="$(with_manifest "$bad_yaml")"; rc=$?
  if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "could not read its value"; then
    ok "a declaration this parse cannot read is REFUSED, not treated as absent"
  else
    bad "an unreadable declaration was treated as 'no coverage source': ${bad_yaml%%$'\n'*}"
  fi
done

# And a manifest that genuinely does not mention it at all IS "no coverage source" — the one case
# where skipping is right, so the refusal above cannot be achieved by refusing everything.
out="$(with_manifest '            - name: KAFKA_STRIKE_HOLD_SPOT_TOPIC
              value: underlying.spx.index.price')"; rc=$?
if [ "$rc" -eq 0 ]; then ok "a manifest that names no coverage topic is not a refusal"
else bad "a manifest without the key was refused:"; printf '%s\n' "$out" | grep -E "^FAIL" | head -2; fi

# 7) DEV ONLY, fail-closed.
out="$(PATH="$TMP/bin:$PATH" ENVIRONMENT=production KAFKA_BOOTSTRAP_SERVERS=mock:9092 bash "$BAR" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "DEV ONLY"; then ok "refuses outside dev"
else bad "did not refuse outside dev"; fi

printf '\nensure-strike-hold-topics-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
