#!/usr/bin/env bash
# apply-topics.sh --only / APPLY_TOPICS_ONLY and --dry-run / APPLY_TOPICS_DRY_RUN, driven over mocked kafka CLIs
# (the pattern of apply-topics-skip-exit-test.sh). Proves:
#   1. unset filter + no dry-run: every declared topic is altered, as before;
#   2. a filter touches ONLY the named topics (no alter for any other);
#   3. an unknown name is refused (exit 2) and nothing is sent;
#   4. --dry-run sends no create/alter/delete at all, yet logs what it WOULD send;
#   5. dry-run + filter logs exactly the named topics; an absent topic is "created" only in the log.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
# shellcheck source=/dev/null
source "$HERE/topics.env"
T1=es.rithmic.depth; T2=options.rithmic.es-depth-spx
N_DECLARED=$(for e in $OPTIONS_EDGE_TOPICS; do echo "${e%%:*}"; done | sort -u | wc -l | tr -d ' ')

# run <extra args...>; ABSENT env var lists topics whose describe answers nothing
run() {
  local tmp; tmp="$(mktemp -d "$WORK/run.XXXX")"; LOG="$tmp/calls"; : > "$LOG"
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$LOG"
name=""; prev=""; for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  for a in \${ABSENT:-}; do [ "\$a" = "\$name" ] && exit 0; done
  p=\$(printf '%s' "$OPTIONS_EDGE_TOPICS" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
  [ "\$name" = "\${DESC_PARTS_TOPIC:-}" ] && p="\${DESC_PARTS:-\$p}"
  echo "Topic: \$name TopicId: ID PartitionCount: \${p:-1} ReplicationFactor: \${DESC_RF:-1}"
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
echo "kafka-configs \$*" >> "$LOG"
exit 0
EOF
  printf '#!/usr/bin/env bash\necho "localhost:9092 (id: 1 rack: null) -> ("\necho "localhost:9093 (id: 2 rack: null) -> ("\n' > "$tmp/kafka-broker-api-versions"
  printf '#!/usr/bin/env bash\necho "kafka-reassign-partitions $*" >> "%s"\nexit 0\n' "$LOG" > "$tmp/kafka-reassign-partitions"
  chmod +x "$tmp"/kafka-*
  OUT="$tmp/out"
  env PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 ENVIRONMENT=dev \
    KAFKA_TOPIC_CONFIG_RETRY_ATTEMPTS=1 KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 KAFKA_TOPIC_REPAIR_WAIT_SECONDS=1 \
    ${ENVX:-} bash "$HERE/apply-topics.sh" "$@" > "$OUT" 2>&1
  RC=$?
}
alters() { grep -c -- "--alter" "$LOG" | tr -d ' '; }
altered_names() { sed -n 's/.*--entity-name \([^ ]*\) --alter.*/\1/p' "$LOG" | sort -u; }
mutations() { grep -cE -- "^kafka-topics .*(--create|--delete|--alter)|^kafka-configs .*--alter|^kafka-reassign-partitions" "$LOG" | tr -d ' '; }

echo "1. no filter, no dry-run: today's behaviour"
run
[ "$RC" -eq 0 ] && ok "exit 0" || { bad "exit $RC"; tail -5 "$OUT"; }
[ "$(altered_names | wc -l | tr -d ' ')" -eq "$N_DECLARED" ] && ok "all $N_DECLARED declared topics altered" || bad "altered $(altered_names | wc -l) of $N_DECLARED"

echo "2. --only touches ONLY the named topics"
run --only "$T1,$T2"
[ "$RC" -eq 0 ] && ok "exit 0" || { bad "exit $RC"; tail -5 "$OUT"; }
[ "$(altered_names | tr '\n' ' ')" = "$(printf '%s\n' $T1 $T2 | sort | tr '\n' ' ')" ] && ok "altered exactly $T1 and $T2" || bad "altered: $(altered_names | tr '\n' ' ')"
grep -q "RESTRICTED to: $T1 $T2" "$OUT" && ok "prints what it will alter" || bad "no RESTRICTED line"
ENVX="APPLY_TOPICS_ONLY=$T1" run
[ "$(altered_names | tr '\n' ' ')" = "$T1 " ] && ok "APPLY_TOPICS_ONLY env var works" || bad "env var: $(altered_names | tr '\n' ' ')"
ENVX=""

echo "3. an unknown name is refused before anything is sent"
run --only "$T1,no.such.topic"
[ "$RC" -eq 2 ] && ok "exit 2" || bad "exit $RC"
grep -q "REFUSED: 'no.such.topic'" "$OUT" && ok "says why" || bad "no refusal text"
[ "$(mutations)" -eq 0 ] && ok "nothing sent" || bad "sent $(mutations) mutations"
run --only ""; [ "$RC" -eq 2 ] && [ "$(mutations)" -eq 0 ] && ok "an empty --only is refused, not read as all topics" || bad "empty --only exit $RC"
run --only ","; [ "$RC" -eq 2 ] && ok "a comma-only --only is refused" || bad "comma-only exit $RC"
ENVX="APPLY_TOPICS_ONLY=," run; [ "$RC" -eq 2 ] && ok "APPLY_TOPICS_ONLY=, is refused" || bad "env comma-only exit $RC"; ENVX=""
run --bogus; [ "$RC" -eq 2 ] && ok "unknown argument exits 2" || bad "unknown argument exit $RC"

echo "4. --dry-run sends no create/alter/delete"
run --dry-run
[ "$RC" -eq 0 ] && ok "exit 0" || { bad "exit $RC"; tail -5 "$OUT"; }
[ "$(mutations)" -eq 0 ] && ok "no mutating call reached the CLI" || bad "$(mutations) mutating calls reached the CLI"
[ "$(grep -c '^\[dry-run\] WOULD kafka-configs' "$OUT")" -eq "$N_DECLARED" ] && ok "logged one WOULD alter per declared topic" || bad "WOULD count $(grep -c 'WOULD kafka-configs' "$OUT")"

echo "5. dry-run + filter, and an absent topic is only created in the log"
ABSENT="$T2" run --dry-run --only "$T1,$T2"
[ "$RC" -eq 0 ] && ok "exit 0" || { bad "exit $RC"; tail -5 "$OUT"; }
[ "$(mutations)" -eq 0 ] && ok "nothing sent" || bad "$(mutations) sent"
grep -q "WOULD kafka-topics .*--create .*$T2" "$OUT" && ok "logs the create of the absent topic" || bad "no WOULD create"
[ "$(grep -c 'WOULD kafka-configs' "$OUT")" -eq 2 ] && ok "exactly the two named topics logged" || bad "logged $(grep -c 'WOULD kafka-configs' "$OUT")"
grep -q "retention.ms=86400000" "$OUT" && ok "the log shows the declared 86400000" || bad "no 86400000 in the log"

echo "6. dry-run on DRIFTED topics: the destructive delete/recreate and the RF reassignment paths send nothing"
exact_victim() {
  local t e
  for t in ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-}; do
    e=$(printf '%s\n' $OPTIONS_EDGE_TOPICS | awk -F: -v n="$t" '$1==n{print $2; exit}')
    case "$e" in ''|*[!0-9]*) continue ;; esac
    [ "$e" -gt 1 ] || continue
    printf '%s\n' ${OPTIONS_EDGE_NEVER_RECREATE_TOPICS:-} | grep -qx "$t" && continue
    echo "$t"; return 0
  done
  return 1
}
EXACT_VICTIM="$(exact_victim)"
if [ -n "$EXACT_VICTIM" ]; then
  ENVX="KAFKA_RECREATE_MISMATCHED_TOPICS=true DESC_PARTS_TOPIC=$EXACT_VICTIM DESC_PARTS=1" run --dry-run --only "$EXACT_VICTIM"
  grep -q "WOULD kafka-topics .*--delete" "$OUT" && ok "the destructive delete path was reached (and only logged)" || bad "delete path not reached"
  [ "$(mutations)" -eq 0 ] && ok "no delete/create/alter reached the CLI" || bad "$(mutations) mutations reached the CLI"
else
  bad "no exact-partition victim declared to drive the delete path"
fi
ENVX="KAFKA_TOPIC_REPLICATION_FACTOR=2 KAFKA_RECREATE_MISMATCHED_TOPICS=true DESC_RF=1" run --dry-run --only "$T1"
grep -q "WOULD reassign" "$OUT" && ok "the RF reassignment path was reached (and only logged)" || bad "reassign path not reached"
[ "$(mutations)" -eq 0 ] && ok "no kafka-reassign-partitions reached the CLI" || bad "$(mutations) mutations"
ENVX=""

[ "$fails" -eq 0 ] && { echo "=== apply-topics filter/dry-run test: OK ==="; exit 0; } || { echo "=== FAILED ($fails) ==="; exit 1; }
