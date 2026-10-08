#!/usr/bin/env bash
# apply-topics.sh must distinguish, IN ITS EXIT STATUS, "the run reached the end of the declared list
# and these topics could not be reconciled" from "the run did not complete".
#
# Until 2026-10-08 both were exit 1, and scripts/ops/prod-clean-slate.sh read the first as the second:
# on 2026-10-07 one drifted topic left all twelve es4->prod mirrors paused. The wrapper now acts on
# exit 9 plus the SKIPPED_TOPIC_NAMES line, so both are asserted here against the REAL script driven
# over mocked kafka CLIs -- the pattern of apply-topics-strike-safety-test.sh.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

# shellcheck source=/dev/null
source "$HERE/topics.env"
DECLARED="$OPTIONS_EDGE_TOPICS"

# A topic that is declared above 1 partition, is NOT exact-partition and NOT never-recreate, so a
# broker reading below its declared count is a plain SKIP. Derived from the declaration so this test
# cannot drift into naming a topic the file does not declare.
pick_victim() {
  local e t p
  for e in $DECLARED; do
    t="${e%%:*}"; p="${e##*:}"
    case "$p" in ''|*[!0-9]*) continue ;; esac
    [ "$p" -gt 1 ] || continue
    printf '%s\n' ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-} | tr ' ' '\n' | grep -qx "$t" && continue
    printf '%s\n' ${OPTIONS_EDGE_NEVER_RECREATE_TOPICS:-} | tr ' ' '\n' | grep -qx "$t" && continue
    printf '%s %s\n' "$t" "$p"; return 0
  done
  return 1
}
read -r VICTIM VICTIM_PARTS <<< "$(pick_victim)"
[ -n "${VICTIM:-}" ] || { echo "FAIL: no declared non-exact topic above 1 partition to drift" >&2; exit 1; }
# A SECOND victim, taken from the END of the same filtered list, so the two-topic case is not one
# topic named twice.
VICTIM2=$(for e in $DECLARED; do t="${e%%:*}"; p="${e##*:}"; case "$p" in ''|*[!0-9]*) continue ;; esac
  [ "$p" -gt 1 ] || continue; [ "$t" = "$VICTIM" ] && continue
  printf '%s\n' ${OPTIONS_EDGE_EXACT_PARTITION_TOPICS:-} | tr ' ' '\n' | grep -qx "$t" && continue
  printf '%s\n' ${OPTIONS_EDGE_NEVER_RECREATE_TOPICS:-} | tr ' ' '\n' | grep -qx "$t" && continue
  printf '%s\n' "$t"; done | tail -1)
[ -n "$VICTIM2" ] || { echo "FAIL: need two drift-able declared topics" >&2; exit 1; }
echo "victims: $VICTIM (declared $VICTIM_PARTS), $VICTIM2"

# run "<topic:parts-to-report-for-drifted-topics>" <src-dir> [env KEY=VAL ...]
# DRIFT is a space-separated list of "<topic>=<partitions the broker reports>".
run() {
  # Explicit shifts: `shift 2` with one argument FAILS and leaves "$@" holding one EMPTY string, which
  # `env ... "$@"` then reads as an empty variable assignment ("env: : No such file or directory").
  local drift="${1-}"; [ "$#" -gt 0 ] && shift
  local src="${1:-$HERE}"; [ "$#" -gt 0 ] && shift
  local tmp; tmp="$(mktemp -d "$WORK/run.XXXX")"; LOG="$tmp/calls"; : > "$LOG"
  cat > "$tmp/kafka-topics" <<EOF
#!/usr/bin/env bash
echo "kafka-topics \$*" >> "$LOG"
name=""; prev=""
for a in "\$@"; do [ "\$prev" = "--topic" ] && name="\$a"; prev="\$a"; done
if [[ "\$*" == *--describe* ]]; then
  d=\$(printf '%s' "$drift" | tr ' ' '\\n' | awk -F= -v n="\$name" '\$1 == n {print \$2; exit}')
  if [ -n "\$d" ]; then
    echo "Topic: \$name TopicId: ID PartitionCount: \$d ReplicationFactor: 1"
  else
    p=\$(printf '%s' "$DECLARED" | tr ' ' '\\n' | awk -F: -v n="\$name" '\$1 == n {print \$2; exit}')
    echo "Topic: \$name TopicId: ID PartitionCount: \${p:-1} ReplicationFactor: 1"
  fi
fi
exit 0
EOF
  cat > "$tmp/kafka-configs" <<EOF
#!/usr/bin/env bash
echo "kafka-configs \$*" >> "$LOG"
if [ -n "\${CONFIGS_FAIL_ON:-}" ] && [[ "\$*" == *"\$CONFIGS_FAIL_ON"* ]]; then
  echo "mock kafka-configs: forced failure" >&2; exit 7
fi
exit 0
EOF
  printf '#!/usr/bin/env bash\necho "localhost:9092 (id: 1 rack: null) -> ("\nexit 0\n' > "$tmp/kafka-broker-api-versions"
  printf '#!/usr/bin/env bash\necho "kafka-reassign-partitions $*" >> "%s"\nexit 0\n' "$LOG" > "$tmp/kafka-reassign-partitions"
  chmod +x "$tmp"/kafka-*
  OUT="$tmp/out"
  env PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 \
    ENVIRONMENT=dev KAFKA_RECREATE_MISMATCHED_TOPICS=false KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 \
    KAFKA_TOPIC_REPAIR_WAIT_SECONDS=1 "$@" \
    bash "$src/apply-topics.sh" > "$OUT" 2>&1
  RC=$?
}
names_line() { sed -n 's/^apply-topics\.sh: SKIPPED_TOPIC_NAMES: *//p' "$OUT" | tail -1; }

echo "1. a clean broker: exit 0 and NO skipped-names line"
run ""
[ "$RC" -eq 0 ] && ok "exit 0" || { bad "exit $RC on a clean broker"; tail -5 "$OUT" | sed 's/^/      | /'; }
[ -z "$(names_line)" ] && ok "no SKIPPED_TOPIC_NAMES line" || bad "a clean run printed: $(names_line)"

echo "2. one drifted topic: exit 9, the name on the line, and the REST of the list still reconciled"
run "$VICTIM=1"
[ "$RC" -eq 9 ] && ok "exit 9 (not 1, not 0)" || { bad "exit $RC, expected 9"; tail -8 "$OUT" | sed 's/^/      | /'; }
[ "$(names_line)" = "$VICTIM" ] && ok "SKIPPED_TOPIC_NAMES names exactly $VICTIM" || bad "names line is [$(names_line)]"
grep -q "below declared minimum" "$OUT" && ok "and the human diagnostic is still there" || bad "the human diagnostic is gone"
# The claim the exit code makes: every OTHER declared topic was still created/updated. The config
# reconcile runs once per reconciled topic, so a topic later in the list having one proves the loop
# did not abandon the rest.
last_other=$(printf '%s' "$DECLARED" | tr ' ' '\n' | sed 's/:.*//' | grep -vx "$VICTIM" | tail -1)
grep -q -- "--entity-name $last_other --alter" "$LOG" \
  && ok "the last declared topic ($last_other) was still reconciled after the skip" \
  || bad "the loop stopped at the skip: no config alter for $last_other"

echo "3. the names line carries NAMES only, and every skipped topic"
run "$VICTIM=1 $VICTIM2=1"
[ "$RC" -eq 9 ] && ok "exit 9 with two skips" || bad "exit $RC with two skips"
line="$(names_line)"
for t in "$VICTIM" "$VICTIM2"; do
  printf '%s\n' $line | grep -qx "$t" && ok "names $t" || bad "does not name $t: [$line]"
done
printf '%s' "$line" | grep -q '[()]' && bad "the names line carries a free-text reason: [$line]" \
  || ok "no parenthesised reason leaked into the machine-readable line"
[ "$(printf '%s\n' $line | wc -w | tr -d ' ')" = 2 ] && ok "exactly two names" || bad "expected 2 names, got [$line]"

echo "4. a run that does NOT complete must not exit 9"
# A failing kafka-configs is a hard failure: set -e aborts the loop, so nothing may be concluded
# about the topics after it -- which is exactly what a 9 would wrongly claim.
run "" "$HERE" CONFIGS_FAIL_ON="--entity-name $VICTIM "
[ "$RC" -ne 0 ] && [ "$RC" -ne 9 ] && ok "a mid-loop CLI failure exits $RC (non-zero, not 9)" \
  || bad "a mid-loop CLI failure exited $RC"
[ -z "$(names_line)" ] && ok "and prints no skipped-names line" || bad "an aborted run printed: $(names_line)"
# A precondition refusal, before the loop even starts.
rc=0; ( env -u KAFKA_TOPIC_REPLICATION_FACTOR KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
        bash "$HERE/apply-topics.sh" ) >/dev/null 2>&1 || rc=$?
[ "$rc" -ne 0 ] && [ "$rc" -ne 9 ] && ok "a missing replication factor exits $rc (non-zero, not 9)" \
  || bad "a missing replication factor exited $rc"

echo "5. MUTATIONS: the status and the line are each load-bearing"
mutant() { # <python old||new> -> dir
  local dir; dir="$WORK/mut.$RANDOM"; mkdir -p "$dir"
  cp "$HERE/apply-topics.sh" "$dir/"
  for s in $(bash "$HERE/apply-topics-sibling-files.sh"); do cp "$HERE/$s" "$dir/"; done
  python3 - "$dir/apply-topics.sh" "$1" <<'PY'
import sys
path, edit = sys.argv[1], sys.argv[2]
old, new = edit.split("||", 1)
src = open(path).read()
assert old in src, "the mutation did not apply: %r not found" % old
open(path, "w").write(src.replace(old, new, 1))
PY
  printf '%s\n' "$dir"
}
m="$(mutant 'exit "$SKIPPED_EXIT"||exit 1')" || { bad "mutation 1 did not apply"; m=""; }
if [ -n "$m" ]; then
  run "$VICTIM=1" "$m"
  [ "$RC" -eq 1 ] && ok "back at exit 1, case 2 would fail (mutant exit $RC)" || bad "the exit-status mutation changed nothing: $RC"
fi
m="$(mutant 'echo "apply-topics.sh: SKIPPED_TOPIC_NAMES:||echo "apply-topics.sh: NOTHING_TO_SEE:')" || { bad "mutation 2 did not apply"; m=""; }
if [ -n "$m" ]; then
  run "$VICTIM=1" "$m"
  [ -z "$(names_line)" ] && ok "without the line the wrapper gets nothing (mutant exit $RC)" || bad "the names-line mutation changed nothing"
fi

echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-skip-exit: OK ==="; exit 0; fi
echo "=== apply-topics-skip-exit: $fails problem(s) ===" >&2; exit 1
