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
if [ -n "\${TOPICS_FAIL_ON:-}" ] && [[ "\$*" == *"\$TOPICS_FAIL_ON"* ]]; then
  echo "mock kafka-topics: forced failure" >&2; exit \${TOPICS_FAIL_RC:-7}
fi
if [[ "\$*" == *--describe* ]]; then
  # An ABSENT topic answers nothing, which is how apply-topics.sh decides to CREATE it.
  for a in \${TOPICS_ABSENT:-}; do [ "\$a" = "\$name" ] && exit 0; done
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
  echo "mock kafka-configs: forced failure" >&2; exit \${CONFIGS_FAIL_RC:-7}
fi
if [ -n "\${CONFIGS_FORGE_LINE:-}" ]; then echo "\$CONFIGS_FORGE_LINE"; fi
# What this child INHERITED, and what it can do with it: the attestation path must not be in its
# environment at all (apply-topics.sh unsets it before running anything).
echo "child-saw-result-file=[\${APPLY_TOPICS_RESULT_FILE:-}]" >> "$LOG"
if [ -n "\${CONFIGS_FORGE_ATTESTATION:-}" ] && [ -n "\${APPLY_TOPICS_RESULT_FILE:-}" ]; then
  echo "apply-topics: state=skipped skipped=benign.topic" > "\$APPLY_TOPICS_RESULT_FILE"
fi
exit 0
EOF
  printf '#!/usr/bin/env bash\necho "localhost:9092 (id: 1 rack: null) -> ("\nexit 0\n' > "$tmp/kafka-broker-api-versions"
  printf '#!/usr/bin/env bash\necho "kafka-reassign-partitions $*" >> "%s"\nexit 0\n' "$LOG" > "$tmp/kafka-reassign-partitions"
  chmod +x "$tmp"/kafka-*
  OUT="$tmp/out"
  # The attestation file is created EMPTY, exactly as scripts/ops/prod-clean-slate.sh creates it, so
  # "did not reach an ending" is observable as an empty file rather than as a missing one.
  RESULT="${RESULT_PATH:-$tmp/result}"
  if [ "$RESULT" != /dev/full ]; then
    if [ -n "${RESULT_PREFILL:-}" ]; then printf '%s\n' "$RESULT_PREFILL" > "$RESULT"; else : > "$RESULT"; fi
  fi
  env PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC_REPLICATION_FACTOR=1 \
    ENVIRONMENT=dev KAFKA_RECREATE_MISMATCHED_TOPICS=false KAFKA_TOPIC_DELETE_WAIT_SECONDS=1 \
    KAFKA_TOPIC_REPAIR_WAIT_SECONDS=1 APPLY_TOPICS_RESULT_FILE="$RESULT" "$@" \
    bash "$src/apply-topics.sh" > "$OUT" 2>&1
  RC=$?
}
names_line()  { sed -n 's/^apply-topics\.sh: SKIPPED_TOPIC_NAMES: *//p' "$OUT" | tail -1; }
attested()    { cat "$RESULT" 2>/dev/null; }
att_state()   { sed -n 's/^apply-topics: state=\([a-z]*\) skipped=.*/\1/p' "$RESULT" 2>/dev/null | tail -1; }
att_names()   { sed -n 's/^apply-topics: state=skipped skipped=//p' "$RESULT" 2>/dev/null | tail -1; }

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

echo "5. THE ATTESTATION FILE: the only signal a child process cannot produce"
run ""
[ "$(att_state)" = ok ] && ok "a clean run attests state=ok" || bad "a clean run attested [$(attested)]"
[ -z "$(att_names)" ] && ok "and names no skipped topic" || bad "a clean run named [$(att_names)]"
run "$VICTIM=1"
[ "$(att_state)" = skipped ] && ok "a skip run attests state=skipped" || bad "a skip run attested [$(attested)]"
[ "$(att_names)" = "$VICTIM" ] && ok "and names exactly $VICTIM" || bad "it names [$(att_names)]"
[ "$(wc -l < "$RESULT" | tr -d ' ')" = 1 ] && ok "one line, nothing else" || bad "the file holds $(wc -l < "$RESULT") lines"

# THE COLLISION this file exists for: create_topic ends in `kafka-topics --create`, whose status the
# function returns and `set -e` then exits with. A CLI that exits 9 therefore aborts apply-topics.sh
# mid-loop with status 9 -- the SKIP status. Only the attestation tells the two apart.
run "" "$HERE" TOPICS_ABSENT="$VICTIM" TOPICS_FAIL_ON=--create TOPICS_FAIL_RC=9
[ "$RC" -eq 9 ] && ok "a kafka CLI exiting 9 makes apply-topics.sh exit 9 too (the collision is real)" \
  || { bad "the forced CLI failure exited $RC, so this case proves nothing"; tail -4 "$OUT" | sed 's/^/      | /'; }
[ -z "$(attested)" ] && ok "but the attestation file is EMPTY, so no caller may read it as a skip" \
  || bad "an aborted run attested [$(attested)]"

# ...and a child that PRINTS the stdout marker cannot fake one either.
run "" "$HERE" CONFIGS_FORGE_LINE="apply-topics.sh: SKIPPED_TOPIC_NAMES: evil.topic"
[ "$RC" -eq 0 ] && ok "a run whose child forges the stdout line still exits 0" || bad "exit $RC"
printf '%s' "$(names_line)" | grep -q 'evil.topic' \
  && ok "the forged line IS in the output (so parsing stdout would have believed it)" \
  || bad "the mock did not forge the line — this case proves nothing"
[ "$(att_state)" = ok ] && [ -z "$(att_names)" ] \
  && ok "while the attestation says state=ok and names nothing" || bad "the attestation was polluted: [$(attested)]"

# ...and a child cannot reach the attestation THROUGH THE ENVIRONMENT either: apply-topics.sh takes
# APPLY_TOPICS_RESULT_FILE out of its own environment before it runs anything, so an exported path is
# not inherited by any kafka CLI (deploy Codex round 2).
run "" "$HERE" CONFIGS_FORGE_ATTESTATION=1
grep -q 'child-saw-result-file=\[\]' "$LOG" \
  && ok "the children saw an EMPTY APPLY_TOPICS_RESULT_FILE" \
  || bad "a child inherited the attestation path: $(grep -m1 'child-saw-result-file' "$LOG")"
[ "$(att_state)" = ok ] && [ -z "$(att_names)" ] \
  && ok "so its attempt to forge a skip attestation wrote nothing" || bad "the attestation was forged: [$(attested)]"

# The whole exploit in one run: a child tries to forge the attestation AND the run then aborts, so
# nothing of apply-topics.sh's own would overwrite a forgery. The file must still be empty.
run "" "$HERE" CONFIGS_FORGE_ATTESTATION=1 TOPICS_ABSENT="$VICTIM2" TOPICS_FAIL_ON=--create TOPICS_FAIL_RC=9
[ "$RC" -eq 9 ] && ok "an aborted run with a forging child still exits 9" || bad "it exited $RC"
[ -z "$(attested)" ] && ok "and leaves NO attestation at all, so the decision answers FAIL" \
  || bad "an aborted run with a forging child attested [$(attested)]"

# A STALE attestation cannot survive into this run: apply-topics.sh empties the file itself before it
# does anything, so a caller that reuses a path (or forgets to truncate) cannot read a previous run's
# ending as this one's.
RESULT_PREFILL="apply-topics: state=skipped skipped=stale.topic" run ""
[ "$(att_state)" = ok ] && [ -z "$(att_names)" ] \
  && ok "a clean run replaces a stale attestation" || bad "the stale line survived: [$(attested)]"
RESULT_PREFILL="apply-topics: state=skipped skipped=stale.topic" run "" "$HERE" TOPICS_ABSENT="$VICTIM2" TOPICS_FAIL_ON=--create TOPICS_FAIL_RC=9
unset RESULT_PREFILL
[ "$RC" -eq 9 ] && ok "an aborted run with a stale attestation still exits 9" || bad "it exited $RC"
[ -z "$(attested)" ] \
  && ok "and the stale attestation is GONE, so the decision answers FAIL rather than reading it" \
  || bad "an aborted run left the stale attestation in place: [$(attested)]"

# An unwritable attestation path must ABORT the run, not finish quietly with no attestation.
RESULT_PATH=/dev/full run "$VICTIM=1"
unset RESULT_PATH
[ "$RC" -ne 0 ] && [ "$RC" -ne 9 ] && ok "an unwritable attestation path exits $RC (non-zero, not the skip status)" \
  || bad "an unwritable attestation path exited $RC"

echo "6. MUTATIONS: the status, the line and the attestation are each load-bearing"
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
  [ -z "$(names_line)" ] && ok "without the line a reader gets nothing (mutant exit $RC)" || bad "the names-line mutation changed nothing"
fi
m="$(mutant 'write_run_result skipped "${SKIPPED_TOPICS[*]%% *}"||true')" || { bad "mutation 3 did not apply"; m=""; }
if [ -n "$m" ]; then
  run "$VICTIM=1" "$m"
  [ -z "$(attested)" ] && ok "without the write there is no attestation, so a wrapper must refuse (mutant exit $RC)" \
    || bad "the attestation mutation changed nothing: [$(attested)]"
fi
m="$(mutant 'unset APPLY_TOPICS_RESULT_FILE||export APPLY_TOPICS_RESULT_FILE')" || { bad "mutation 4 did not apply"; m=""; }
if [ -n "$m" ]; then
  # The WHOLE exploit, not half of it: a child forges the attestation AND the run then aborts, so
  # nothing overwrites the forgery. That is the state a wrapper would read as "a partial success whose
  # only unreconciled topic is benign.topic" and resume every mirror on.
  # The abort is injected on the SECOND victim, not the first declared topic: a create failure on the
  # very first entry aborts the run before any kafka-configs child has run, so the forging child never
  # gets to run and the case proves nothing.
  run "" "$m" CONFIGS_FORGE_ATTESTATION=1 TOPICS_ABSENT="$VICTIM2" TOPICS_FAIL_ON=--create TOPICS_FAIL_RC=9
  if grep -q 'child-saw-result-file=\[\]' "$LOG"; then
    bad "the mutant still hides the path from children — the unset is not what does it"
  elif [ "$(att_state)" = skipped ] && [ "$(att_names)" = benign.topic ]; then
    ok "with the variable left exported, an aborted run ends with a child's FORGED skip attestation (the unset is load-bearing)"
  else
    bad "the mutant did not reproduce the forgery: exit $RC, attestation [$(attested)]"
  fi
fi
m="$(mutant 'if [ -n "$RESULT_FILE" ] && ! : > "$RESULT_FILE"; then||if false; then')" || { bad "mutation 5 did not apply"; m=""; }
if [ -n "$m" ]; then
  RESULT_PREFILL="apply-topics: state=skipped skipped=stale.topic" run "" "$m" TOPICS_ABSENT="$VICTIM2" TOPICS_FAIL_ON=--create TOPICS_FAIL_RC=9
  unset RESULT_PREFILL
  [ "$(att_names)" = stale.topic ] \
    && ok "without the self-truncation an aborted run leaves the STALE attestation readable (it is load-bearing)" \
    || bad "the self-truncation mutation changed nothing: [$(attested)]"
fi
m="$(mutant "write_run_result ok ''||true")" || { bad "mutation 6 did not apply"; m=""; }
if [ -n "$m" ]; then
  run "" "$m"
  [ -z "$(attested)" ] && ok "and the clean ending's attestation is load-bearing too (mutant exit $RC)" \
    || bad "the ok-attestation mutation changed nothing: [$(attested)]"
fi

echo
if [ "$fails" -eq 0 ]; then echo "=== apply-topics-skip-exit: OK ==="; exit 0; fi
echo "=== apply-topics-skip-exit: $fails problem(s) ===" >&2; exit 1
