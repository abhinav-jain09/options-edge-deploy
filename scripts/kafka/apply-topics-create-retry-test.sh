#!/usr/bin/env bash
# apply-topics.sh must survive the Kafka admin client's own retry of CreateTopics.
#
# 2026-10-13 06:05 on prod: the broker received ONE create for delta-flow-10s and created it as declared, but the
# kafka-topics command that sent it exited 1 with "Topic 'delta-flow-10s' already exists" a second later - the
# client had retried a request whose reply it never saw. apply-topics.sh aborted, prod-clean-slate.sh left every
# mirror paused and prod down. The REAL script is driven here over a STATEFUL mock that reproduces exactly that:
# without --if-not-exists a create that succeeds is followed by the TopicExists error and exit 1.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'find "$WORK" -mindepth 1 -delete 2>/dev/null; rmdir "$WORK" 2>/dev/null' EXIT
# shellcheck source=/dev/null
source "$HERE/topics.env"
VICTIM=$(for e in $OPTIONS_EDGE_TOPICS; do t="${e%%:*}"; p="${e##*:}"; case "$p" in ''|*[!0-9]*) continue ;; esac; [ "$p" -gt 1 ] && { printf '%s %s\n' "$t" "$p"; break; }; done)
read -r NAME PARTS <<< "$VICTIM"
[ -n "${NAME:-}" ] || { echo "FAIL: no multi-partition declared topic"; exit 1; }

# run <extra env...>; mock state lives in $tmp/created
run() {
  local tmp; tmp="$(mktemp -d "$WORK/run.XXXX")"; LOG="$tmp/calls"; : > "$LOG"; STATE="$tmp/created"; : > "$STATE"
  cat > "$tmp/kafka-topics" <<MOCK
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$LOG"
name=""; prev=""; ine=0
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; [ "\$a" = "--if-not-exists" ] && ine=1; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  if [ "\$name" = "$NAME" ]; then
    grep -qx "\$name" "$STATE" && echo "Topic: \$name TopicId: ID PartitionCount: $PARTS ReplicationFactor: 1"
  else
    if [ "\$name" = "\${TOPICS_ABSENT_NAME:-}" ] && ! grep -qx "\$name" "$STATE"; then exit 0; fi
    p=\$(printf '%s' "$OPTIONS_EDGE_TOPICS" | tr ' ' '\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
    echo "Topic: \$name TopicId: ID PartitionCount: \${p:-1} ReplicationFactor: 1"
  fi
  exit 0
fi
if [[ "\$*" == *--create* ]]; then
  echo "\$name" >> "$STATE"
  # the admin client's retry after a SUCCESSFUL create: only --if-not-exists makes it harmless
  if [ "\$name" = "$NAME" ] && [ "\$ine" = 0 ] && [ "\${SIMULATE_CLIENT_RETRY:-0}" = 1 ]; then
    echo "Error while executing topic command : Topic '\$name' already exists." >&2; exit 1
  fi
fi
exit 0
MOCK
  printf '#!/usr/bin/env bash\necho "kafka-configs $*" >> "%s"\nexit 0\n' "$LOG" > "$tmp/kafka-configs"
  printf '#!/usr/bin/env bash\necho "localhost:9092 (id: 1 rack: null) -> ("\nexit 0\n' > "$tmp/kafka-broker-api-versions"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$tmp/kafka-reassign-partitions"
  chmod +x "$tmp"/kafka-*
  OUT="$tmp/out"; RESULT="$tmp/result"; : > "$RESULT"
  env PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT=dev \
      KAFKA_RECREATE_MISMATCHED_TOPICS=false KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 KAFKA_TOPIC_REPAIR_WAIT_SECONDS=2 \
      APPLY_TOPICS_RESULT_FILE="$RESULT" "$@" bash "$HERE/apply-topics.sh" > "$OUT" 2>&1
  RC=$?
}

echo "1. a create whose client retry reports 'already exists' (the 2026-10-13 prod failure)"
run SIMULATE_CLIENT_RETRY=1 TOPICS_ABSENT_NAME="$NAME"
[ "$RC" -eq 0 ] && ok "apply-topics.sh exits 0" || { bad "exit $RC"; tail -6 "$OUT" | sed 's/^/      | /'; }
grep -q -- "--create.*--if-not-exists\|--if-not-exists.*--topic $NAME\|--create --if-not-exists --topic $NAME" "$LOG" \
  && ok "the create for $NAME carries --if-not-exists" || bad "the create for $NAME has no --if-not-exists"
grep -q "already exists\." "$OUT" && bad "the TopicExists error still reached the output" || ok "no TopicExists error in the output"
grep -q -- "--entity-name $NAME --alter" "$LOG" && ok "the declared configs were still applied to $NAME" || bad "no config alter for $NAME"
[ "$(cat "$RESULT")" = "apply-topics: state=ok skipped=" ] && ok "attested state=ok" || bad "attestation is [$(cat "$RESULT")]"

echo "2. the topic really was created exactly once, with the declared partitions"
n=$(grep -c -- "--create .*--topic $NAME " "$LOG"); [ "$n" = 1 ] && ok "one create call for $NAME" || bad "$n create calls for $NAME"
grep -- "--create" "$LOG" | grep -- "--topic $NAME " | grep -q -- "--partitions $PARTS " && ok "with --partitions $PARTS" || bad "wrong partition count in the create"

echo "3. recovery compaction cadence survives a global topic reset"
REC="options.spx.market-structure.recovery"
run SIMULATE_CLIENT_RETRY=0 TOPICS_ABSENT_NAME="$REC"
[ "$RC" -eq 0 ] && ok "recovery reset reconciliation exits 0" || bad "recovery reset exit $RC"
grep -q -- "--create .*--topic $REC .*--config segment.bytes=16777216 --config max.compaction.lag.ms=300000" "$LOG" \
  && ok "recreated recovery topic has bounded compaction cadence" || bad "recovery create lacks compaction cadence"
grep -q -- "--entity-name $REC --alter .*segment.bytes=16777216,max.compaction.lag.ms=300000" "$LOG" \
  && ok "global config apply restores recovery compaction cadence" || bad "recovery compaction cadence absent"
# All other topics must keep their original configuration; the extra keys are specific to recovery.
if grep 'segment.bytes=16777216' "$LOG" | grep -v "$REC" >/dev/null; then bad "compaction cadence leaked to another topic"; else ok "cadence applies only to recovery"; fi

echo
[ "$fails" -eq 0 ] && { echo "=== apply-topics-create-retry: OK ==="; exit 0; }
echo "=== apply-topics-create-retry: $fails problem(s) ===" >&2; exit 1
