#!/usr/bin/env bash
# The recreate decision, driven exhaustively, plus the wiring that makes prod-clean-slate.sh obey it.
#
# The rules live in ONE function (scripts/ops/clean-slate-decision.sh) and this drives that function,
# so there is no second copy of the table to agree with. What the structural half asserts is only what
# a function cannot: that the operator script reads DECISION_RESUME before starting a mirror, passes
# DECISION_HOLD, and reaches the bring-up only through DECISION_BRINGUP.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D="$HERE/clean-slate-decision.sh"
CS="$HERE/prod-clean-slate.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

for f in "$D" "$CS"; do [ -r "$f" ] || { echo "FAIL: $f missing" >&2; exit 1; }; done

# decide <apply> <ensure> <skipped> <missing> -> "verdict=... resume=... bringup=... exit=... hold=..."
decide() { bash "$D" "$1" "$2" "$3" "$4" "${5-}" 2>&1; }
expect() { # <label> <apply> <ensure> <skipped> <missing> <attested-state> <expected-line>
  local got; got="$(decide "$2" "$3" "$4" "$5" "$6")"
  [ "$got" = "$7" ] && ok "$1" || bad "$1: got [$got] want [$7]"
}

echo "1. the rules, every arm"
expect "full success resumes everything and brings up" \
  0 0 "" "" ok "verdict=OK resume=yes bringup=yes exit=0 hold="
expect "a MISSING topic is held even on a full success (auto-create would make it at the broker default)" \
  0 0 "" "es.futures.cvd.bars" ok "verdict=OK resume=yes bringup=yes exit=0 hold=es.futures.cvd.bars"
expect "a partial apply resumes the mirrors but NEVER brings up" \
  9 0 "options.spx.strike-invasion.current" "" skipped \
  "verdict=PARTIAL resume=yes bringup=no exit=9 hold=options.spx.strike-invasion.current"
expect "partial holds the skipped AND the missing topics" \
  9 0 "a.topic" "b.topic" skipped "verdict=PARTIAL resume=yes bringup=no exit=9 hold=a.topic b.topic"
expect "exit 9 with no names is a FAILURE, not a partial success" \
  9 0 "" "" skipped "verdict=FAIL resume=no bringup=no exit=9 hold="
expect "exit 9 plus a failed partition-only step is a FAILURE" \
  9 1 "a.topic" "" skipped "verdict=FAIL resume=no bringup=no exit=9 hold="
expect "a failed partition-only step alone fails, and keeps ITS status" \
  0 3 "" "" ok "verdict=FAIL resume=no bringup=no exit=3 hold="
expect "any other apply status fails and keeps ITS status" \
  2 0 "" "" ok "verdict=FAIL resume=no bringup=no exit=2 hold="
expect "exit 1 (the pre-2026-10-08 everything) still fails closed" \
  1 0 "x" "y" skipped "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "an empty status is a FAILURE, not an error" \
  "" 0 "" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "a non-numeric status is a FAILURE" \
  abc 0 "" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "a non-numeric ensure status is a FAILURE" \
  0 "x7" "" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "apply=0 WITH named skips is a contradiction, and neither input is trusted" \
  0 0 "a.topic" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "whitespace-only names do not make a partial success" \
  9 0 "   " "" skipped "verdict=FAIL resume=no bringup=no exit=9 hold="
expect "the hold set is a SET: duplicates and extra whitespace collapse" \
  9 0 "a.topic  a.topic" " b.topic  a.topic " skipped "verdict=PARTIAL resume=yes bringup=no exit=9 hold=a.topic b.topic"

expect "a zero status with NO attestation is a FAILURE (require BOTH)" \
  0 0 "" "" "" "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "a zero status attested as a SKIP is a FAILURE" \
  0 0 "" "" skipped "verdict=FAIL resume=no bringup=no exit=1 hold="
expect "the skip status with NO attestation is a FAILURE" \
  9 0 "a.topic" "" "" "verdict=FAIL resume=no bringup=no exit=9 hold="
expect "the skip status attested as a clean run is a FAILURE" \
  9 0 "a.topic" "" ok "verdict=FAIL resume=no bringup=no exit=9 hold="
expect "an unknown attested state is a FAILURE" \
  0 0 "" "" weird "verdict=FAIL resume=no bringup=no exit=1 hold="

echo "2. no input makes it both resume and refuse, or bring up without resuming"
# An exhaustive sweep over the statuses that matter, so a future arm cannot contradict the two
# invariants the operator script depends on.
for a in 0 1 2 9 127 "" zz; do for e in 0 1 9 "" zz; do for sk in "" "t.one"; do for ms in "" "t.two"; do
 for st in ok skipped "" weird; do
  line="$(decide "$a" "$e" "$sk" "$ms" "$st")"
  v="${line#verdict=}"; v="${v%% *}"
  r=$(printf '%s' "$line" | sed -n 's/.*resume=\([a-z]*\).*/\1/p')
  b=$(printf '%s' "$line" | sed -n 's/.*bringup=\([a-z]*\).*/\1/p')
  h="${line#*hold=}"
  case "$v" in OK|PARTIAL|FAIL) ;; *) bad "apply=$a ensure=$e: verdict '$v' is not one of OK/PARTIAL/FAIL"; continue ;; esac
  [ "$b" = yes ] && [ "$r" != yes ] && bad "apply=$a ensure=$e: brings up without resuming"
  [ "$v" = FAIL ] && [ "$r" = yes ] && bad "apply=$a ensure=$e: FAIL that resumes"
  [ "$v" = FAIL ] && [ -n "$h" ] && bad "apply=$a ensure=$e: FAIL with a non-empty hold set ($h)"
  [ "$v" = PARTIAL ] && [ "$b" = yes ] && bad "apply=$a ensure=$e: PARTIAL that brings up"
  [ "$v" = OK ] && [ "$(printf '%s' "$line" | sed -n 's/.*exit=\([0-9]*\).*/\1/p')" != 0 ] && bad "apply=$a ensure=$e: OK with a non-zero exit"
  [ "$v" = OK ] && [ "$a" = 0 ] && [ -n "$sk" ] && bad "apply=0 with skipped names [$sk] was called OK"
  [ "$v" = OK ] && [ "$st" != ok ] && bad "apply=$a with attested state '$st' was called OK"
  [ "$v" = PARTIAL ] && [ "$st" != skipped ] && bad "a PARTIAL with attested state '$st'"
 done
done; done; done; done
ok "the sweep found no self-contradicting verdict (560 input combinations)"

echo "3. the ATTESTATION parser: only an ending of apply-topics.sh counts"
# apply-topics.sh writes exactly one line, from one of two endings. Everything else in that file was
# written by something else — an aborted run, a truncated write, a child that got hold of the path —
# and must read as NO attestation, which with the skip status the rules above answer FAIL.
att() { # <file content, printf-style> -> "state|skipped|reason"
  local f="$WORK/att.$RANDOM"
  if [ "$1" = __ABSENT__ ]; then f="$WORK/no-such-att.$RANDOM"; else printf "$1" > "$f"; fi
  bash -c '. "$1"; read_apply_attestation "$2"; printf "%s|%s|%s\n" "$ATTEST_STATE" "$ATTEST_SKIPPED" "$ATTEST_REASON"' _ "$D" "$f"
}
att_is() { # <label> <content> <expected state> <expected skipped>
  local got; got="$(att "$2")"
  if [ "${got%%|*}" = "$3" ] && [ "$(printf '%s' "$got" | cut -d'|' -f2)" = "$4" ]; then ok "$1"
  else bad "$1: got [$got], wanted state=$3 skipped=$4"; fi
}
att_is "the clean ending parses"            'apply-topics: state=ok skipped=\n'                      ok      ""
att_is "the skip ending parses, with names" 'apply-topics: state=skipped skipped=a.topic b.topic\n'  skipped "a.topic b.topic"
att_is "an absent file is no attestation"   __ABSENT__                                               ""      ""
att_is "an empty file is no attestation"    ''                                                       ""      ""
att_is "a truncated write (no newline) is no attestation" 'apply-topics: state=skipped skipped=a.topic' "" ""
att_is "two lines are no attestation"       'apply-topics: state=ok skipped=\napply-topics: state=skipped skipped=a.topic\n' "" ""
att_is "a forged line BEFORE the real one is no attestation" 'apply-topics: state=skipped skipped=evil.topic\napply-topics: state=ok skipped=\n' "" ""
att_is "an unknown state is no attestation" 'apply-topics: state=weird skipped=a.topic\n'            ""      ""
att_is "state=ok naming a skip contradicts itself" 'apply-topics: state=ok skipped=a.topic\n'        ""      ""
att_is "state=skipped naming nothing is no attestation" 'apply-topics: state=skipped skipped=\n'     ""      ""
att_is "anything else is no attestation"    'junk\n'                                                 ""      ""
att_is "a skip list with shell metacharacters is refused" 'apply-topics: state=skipped skipped=a.topic; rm -rf /\n' "" ""
att_is "a skip list with a newline-escaped payload is refused" 'apply-topics: state=skipped skipped=$(touch /tmp/OE_NOPE)\n' "" ""
att_is "a doubled space between names is refused" 'apply-topics: state=skipped skipped=a.topic  b.topic\n' "" ""
# THE WORST LEGITIMATE LINE: apply-topics.sh builds the skipped list out of the declared topic names,
# so the longest one it can write names the WHOLE production declaration. A 4096-byte bound rejected
# exactly that (deploy Codex round 4), which would have turned a real partial recreate into a FAIL and
# held every mirror — the defect this change exists to remove.
ALLDECL="$(bash -c '. "$1"; printf "%s %s" "$OPTIONS_EDGE_TOPICS" "${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}"' _ "$HERE/../kafka/topics.env" \
  | tr " " "\n" | sed "s/:.*//" | grep . | sort -u | tr "\n" " " | sed "s/ *$//")"
ALLBYTES=$(( ${#ALLDECL} + 37 ))
att_is "an attestation naming EVERY declared topic ($ALLBYTES bytes) parses" \
  "apply-topics: state=skipped skipped=$ALLDECL\n" skipped "$ALLDECL"
att_is "a line longer than anything this writes is refused" "apply-topics: state=skipped skipped=$(python3 -c "print(' '.join('t.%06d'%i for i in range(9000)))")\n" "" ""
# A NUL is DROPPED by the shell when the line is read, so the parse would otherwise see a line the
# file does not hold. The byte-count check is what rejects it.
nulf="$WORK/att.nul"; python3 -c "open('$WORK/att.nul','wb').write(b'apply-topics: state=skipped skipped=a.topic\x00evil\n')"
nul_got="$(bash -c '. "$1"; read_apply_attestation "$2"; printf "%s|%s\n" "$ATTEST_STATE" "$ATTEST_REASON"' _ "$D" "$nulf")"
[ "${nul_got%%|*}" = "" ] && ok "an embedded NUL is refused" || bad "a NUL-bearing file parsed as [$nul_got]"
printf '%s' "$nul_got" | grep -q 'byte' && ok "and the reason names the byte mismatch" || bad "the reason is [$nul_got]"
# Trailing bytes after the newline are the same failure mode from the other side.
printf 'apply-topics: state=ok skipped=\n\0' > "$WORK/att.trail" 2>/dev/null || printf 'apply-topics: state=ok skipped=\nX' > "$WORK/att.trail"
trail_got="$(bash -c '. "$1"; read_apply_attestation "$2"; printf "%s\n" "$ATTEST_STATE"' _ "$D" "$WORK/att.trail")"
[ -z "$trail_got" ] && ok "bytes after the one line are refused" || bad "a file with trailing bytes parsed as [$trail_got]"
printf '%s' "$(att 'junk\n')" | grep -q 'not in the expected shape' && ok "and each refusal says why" \
  || bad "the refusal gives no reason: $(att 'junk\n')"
[ ! -e /tmp/OE_NOPE ] && ok "nothing in the file was ever executed" || { bad "the parser EXECUTED file content"; rm -f /tmp/OE_NOPE; }

echo "4. prod-clean-slate.sh obeys the decision"
code="$(sed 's/#.*//' "$CS")"          # comments stripped: a rule described in prose is not a rule
lineof() { printf '%s\n' "$code" | grep -n -- "$1" | head -1 | cut -d: -f1; }
printf '%s' "$code" | grep -q 'clean_slate_decide "\$ARC" "\$ERC" "\$SKIPPED_NAMES" "\$MISSING"' \
  && ok "it calls the decision with the apply status, the ensure status, the skipped names and the missing names" \
  || bad "it does not call clean_slate_decide with all four inputs"
printf '%s' "$code" | grep -q '\. "\$DEPLOY_SRC/scripts/ops/clean-slate-decision.sh"' \
  && ok "and sources it from DEPLOY_SRC" || bad "it does not source clean-slate-decision.sh from DEPLOY_SRC"
printf '%s' "$code" | grep -q '\$DECISION_HOLD' \
  && ok "it passes DECISION_HOLD to the resume path" || bad "DECISION_HOLD is never used"
printf '%s' "$code" | grep -qE '\[ "\$DECISION_RESUME" = yes \]' \
  && ok "and starts no mirror unless DECISION_RESUME is yes" || bad "the resume is not gated on DECISION_RESUME"
printf '%s' "$code" | grep -qE '\[ "\$DECISION_BRINGUP" != yes \]' \
  && ok "and reaches the bring-up only through DECISION_BRINGUP" || bad "the bring-up is not gated on DECISION_BRINGUP"
# ORDER: the bring-up gate must come BEFORE the only thing that brings prod up.
lb="$(lineof 'DECISION_BRINGUP')"; lu="$(lineof 'oe-boot-bringup')"
if [ -n "$lb" ] && [ -n "$lu" ] && [ "$lb" -lt "$lu" ]; then ok "the bring-up gate precedes oe-boot-bringup ($lb < $lu)"
else bad "the bring-up gate does not precede oe-boot-bringup (gate=$lb bringup=$lu)"; fi
# ...and apply-topics.sh and ensure-partition-only-topics.sh are no longer one `&&` behind one status.
printf '%s' "$code" | grep -q 'ARC=\$?' && printf '%s' "$code" | grep -q 'ERC=\${PIPESTATUS\[0\]}' \
  && ok "the two recreate steps keep separate statuses" || bad "the recreate statuses are not captured separately"
printf '%s' "$code" | grep -q 'apply-topics.sh ) *> *"\$APPLY_OUT"' \
  && ok "apply-topics.sh's own output is captured (for the log; the decision does not read it)" \
  || bad "apply-topics.sh's output is not captured to a file"
# WHERE THE NAMES COME FROM. Not stdout: apply-topics.sh shares that stream with every kafka CLI it
# runs, and a child that exits 9 aborts it mid-loop WITH the skip status. The names must be read from
# the attestation file apply-topics.sh writes only at its endings (deploy Codex round 1).
printf '%s' "$code" | grep -q 'APPLY_TOPICS_RESULT_FILE="\$APPLY_RESULT"' \
  && ok "it passes an attestation file to apply-topics.sh" || bad "no APPLY_TOPICS_RESULT_FILE is passed"
printf '%s' "$code" | grep -q 'clean_slate_decide "\$ARC" "\$ERC" "\$SKIPPED_NAMES" "\$MISSING" "\$ATTEST_STATE"' \
  && ok "and passes the ATTESTED STATE, so a status without its attestation cannot pass as OK" \
  || bad "the attested state is not passed to the decision"
printf '%s' "$code" | grep -q 'read_apply_attestation "\$APPLY_RESULT"' \
  && ok "and reads it through the strict parser above" || bad "the attestation is not read by read_apply_attestation"
printf '%s' "$code" | grep -E 'SKIPPED_NAMES=' | grep -q 'ATTEST_SKIPPED' \
  && ok "and the skipped names come from that parse" || bad "the skipped names do not come from the parser"
printf '%s' "$code" | grep -E 'SKIPPED_NAMES=' | grep -q '"\$APPLY_OUT"' \
  && bad "the skipped names are still parsed out of apply-topics.sh's OUTPUT" \
  || ok "and never out of its output"
# The pause half: a failed pause must stop the run before the wipe, and the stop step must be the one
# the ledger runs under its lock.
printf '%s' "$code" | grep -q 'mirror_ledger_record "\$PAUSED_LIST" "\$PAUSED_LIST.new" _bootout_paused_agents' \
  && ok "the bootout runs as the ledger's stop step (under its lock, after the rows are recorded)" \
  || bad "the bootout is not run by mirror_ledger_record"
# The discovery's own status, and the SECOND pass that is the actual gate.
printf '%s' "$code" | grep -q 'if ! rows="\$(_loaded_prod_mirror_rows)"' \
  && ok "the mirror discovery's status is checked before anything is paused" \
  || bad "the discovery status is ignored (an empty list would read as 'nothing is loaded')"
printf '%s' "$code" | grep -q 'if ! after="\$(loaded_mirror_agents_for "\$PROD_BS")"' \
  && ok "and the GATE asks launchd (loaded_mirror_agents_for), not the plist files" \
  || bad "the gate still reads the plists, so a moved or malformed plist hides a live mirror"
printf '%s' "$code" | grep -A3 'STILL LOADED after the pause' | grep -q 'return 1' \
  && ok "which refuses when anything is still loaded (so a raced or missed agent stops the wipe)" \
  || bad "the verify pass does not refuse on a loaded agent"
printf '%s' "$code" | grep -q '\. "\$DEPLOY_SRC/scripts/ops/loaded-mirror-agents.sh"' \
  && ok "and that helper is sourced from DEPLOY_SRC like the others" || bad "loaded-mirror-agents.sh is not sourced"
printf '%s' "$code" | grep -q 'pause_mirrors || {' \
  && ok "and a failed pause stops the run" || bad "a failed pause does not stop the run"
lp="$(lineof 'pause_mirrors ||')"; lw="$(lineof 'offhours-clean-slate.sh DRY_RUN')"
if [ -n "$lp" ] && [ -n "$lw" ] && [ "$lp" -lt "$lw" ]; then
  ok "and it does so BEFORE the wipe ($lp < $lw)"
else
  bad "the pause check does not precede the wipe (pause=$lp wipe=$lw)"
fi
printf '%s' "$code" | grep -q 'STILL LOADED after bootout' \
  && ok "an agent still loaded after the bootout is reported" || bad "a stuck agent is not reported"
printf '%s' "$code" | grep -A3 '\[ "\$stuck" -gt 0 \]' | grep -q 'return 1' \
  && ok "and makes the pause FAIL rather than warn" || bad "a stuck agent does not fail the pause"
printf '%s' "$code" | grep -q 'mirror_ledger_resume "\$PAUSED_LIST"' \
  && ok "the mirrors are started through the ledger helper (which keeps every agent that is still down listed)" \
  || bad "prod-clean-slate.sh does not use mirror_ledger_resume"
printf '%s' "$code" | grep -q 'if ! mirror_ledger_resume' \
  && ok "and its failure status is tested" || bad "mirror_ledger_resume's status is ignored"
# TESTED is not PROPAGATED: the first version only logged a warning and still ended with "DONE" and
# exit 0, while an agent that should have been running was not (deploy Codex round 3).
printf '%s' "$code" | grep -q 'RESUME_RC=1' \
  && ok "and recorded" || bad "a failed resume is not recorded"
lr="$(lineof 'RESUME_RC" -ne 0')"; ld="$(lineof 'prod clean-slate DONE')"
if [ -n "$lr" ] && [ -n "$ld" ] && [ "$lr" -lt "$ld" ]; then
  ok "and checked BEFORE the DONE line, so a run with a mirror down cannot end clean ($lr < $ld)"
else
  bad "the resume status is not checked before the DONE line (check=$lr done=$ld)"
fi
printf '%s' "$code" | grep -A3 'RESUME_RC" -ne 0' | grep -q 'exit 1' \
  && ok "and the script exits non-zero" || bad "a failed resume does not make the script exit non-zero"
printf '%s' "$code" | grep -q ': > "\$APPLY_RESULT"' \
  && ok "the attestation file is created EMPTY, so no attestation is observable" \
  || bad "the attestation file is not truncated before the run — a stale file could be read as this run's"

echo "5. the skip status is the SAME number apply-topics.sh exits with"
# Two files hold the number 9: apply-topics.sh's SKIPPED_EXIT and the PARTIAL arm here. Changing one
# alone would turn every partial recreate back into a FAIL (mirrors held) with nothing saying why, so
# the decision is driven with apply-topics.sh's OWN value rather than a literal.
AT="$HERE/apply-topics.sh"
[ -r "$AT" ] || AT="$HERE/../kafka/apply-topics.sh"
SKIP_EXIT="$(sed -nE 's/^SKIPPED_EXIT=([0-9]+).*/\1/p' "$AT" | head -1)"
if [ -n "$SKIP_EXIT" ]; then
  ok "apply-topics.sh declares SKIPPED_EXIT=$SKIP_EXIT"
  got="$(decide "$SKIP_EXIT" 0 "a.topic" "" skipped)"
  case "$got" in verdict=PARTIAL*) ok "the decision reads that status as PARTIAL" ;;
    *) bad "apply-topics.sh exits $SKIP_EXIT on a skip but the decision answers [$got]" ;; esac
  printf '%s' "$code" | grep -qE "\[ \"\\\$ARC\" -eq $SKIP_EXIT \]" \
    && ok "and prod-clean-slate.sh gates ensure-partition-only-topics on the same number" \
    || bad "prod-clean-slate.sh does not use $SKIP_EXIT where apply-topics.sh exits it"
else
  bad "could not read SKIPPED_EXIT out of $AT — apply-topics.sh and this decision can now drift"
fi

echo "6. MUTATIONS: each rule above is load-bearing"
run_mut() { # <label> <python edit> <apply> <ensure> <skipped> <missing> <state> <must-NOT-equal>
  local label="$1" edit="$2" a="$3" e="$4" sk="$5" ms="$6" st="$7" forbidden="$8"
  local dir="$WORK/m$RANDOM"; mkdir -p "$dir"; cp "$D" "$dir/d.sh"
  python3 - "$dir/d.sh" "$edit" <<'PY'
import sys
path, edit = sys.argv[1], sys.argv[2]
src = open(path).read()
old, new = edit.split("||", 1)
assert old in src, "the mutation did not apply: %r is not in the file" % old
open(path, "w").write(src.replace(old, new, 1))
PY
  [ $? -eq 0 ] || { bad "$label: the mutation did not apply"; return; }
  local got; got="$(bash "$dir/d.sh" "$a" "$e" "$sk" "$ms" "$st" 2>&1)"
  if [ "$got" = "$forbidden" ]; then bad "$label: the mutant still answers [$got] — the rule is not tested"
  else ok "$label (mutant answers [$got])"; fi
}
# Drop the "names must be non-empty" condition: exit 9 with no names would become a partial success.
# Targets the PARTIAL arm's condition specifically: the same `-n` test now appears twice (the
# contradiction check above uses it too), and a mutation anchored on the shorter string edited the
# wrong one and reported the rule as untested.
run_mut "the no-names refusal is load-bearing" \
  'elif [ "$arc" -eq 9 ] && [ "$erc" -eq 0 ] && [ -n "$(_cs_norm "$skipped")" ]; then||elif [ "$arc" -eq 9 ] && [ "$erc" -eq 0 ]; then' \
  9 0 "" "" skipped "verdict=FAIL resume=no bringup=no exit=9 hold="
# Let PARTIAL bring prod up: the owner rule would be gone.
run_mut "PARTIAL must not bring up" \
  'DECISION_VERDICT=PARTIAL; DECISION_RESUME=yes; DECISION_BRINGUP=no||DECISION_VERDICT=PARTIAL; DECISION_RESUME=yes; DECISION_BRINGUP=yes' \
  9 0 "a.topic" "" skipped "verdict=PARTIAL resume=yes bringup=no exit=9 hold=a.topic"
# Stop holding the missing topics on a full success: auto-create could remake one at 1 partition.
run_mut "MISSING topics are held on a full success" \
  'DECISION_HOLD="$(_cs_norm "$missing")"||DECISION_HOLD=""' \
  0 0 "" "es.futures.cvd.bars" ok "verdict=OK resume=yes bringup=yes exit=0 hold=es.futures.cvd.bars"
# Stop holding the skipped ones under PARTIAL: a mirror would produce into a wrong-shaped topic.
run_mut "SKIPPED topics are held under PARTIAL" \
  'DECISION_HOLD="$(_cs_norm "$skipped $missing")"||DECISION_HOLD="$(_cs_norm "$missing")"' \
  9 0 "a.topic" "" skipped "verdict=PARTIAL resume=yes bringup=no exit=9 hold=a.topic"
# The PARSER's strictness, the same way: a mutant that accepts any number of lines would read a
# forged line as this run's attestation.
mut_att() { # <label> <edit> <content> <must-NOT-be-this-state>
  local label="$1" edit="$2" content="$3" forbidden="$4"
  local dir="$WORK/ma$RANDOM"; mkdir -p "$dir"; cp "$D" "$dir/d.sh"
  python3 - "$dir/d.sh" "$edit" <<'PY'
import sys
path, edit = sys.argv[1], sys.argv[2]
old, new = edit.split("||", 1)
src = open(path).read()
assert old in src, "the mutation did not apply: %r not found" % old
open(path, "w").write(src.replace(old, new, 1))
PY
  [ $? -eq 0 ] || { bad "$label: the mutation did not apply"; return; }
  local f="$WORK/matt.$RANDOM"; printf "$content" > "$f"
  local got; got="$(bash -c '. "$1"; read_apply_attestation "$2"; printf "%s|%s\n" "$ATTEST_STATE" "$ATTEST_SKIPPED"' _ "$dir/d.sh" "$f")"
  if [ "${got%%|*}" = "$forbidden" ]; then ok "$label (mutant reads it as [$got])"
  else bad "$label: the mutant answers [$got] too — the check is not tested"; fi
}
# The BYTE-ACCOUNTING rule is the load-bearing one: it refuses any file whose single read line does not
# account for every byte, which covers a forged second line, a truncated write and an embedded NUL. The
# line-count check above it is a clearer diagnostic for the common case, not an independent guard — so
# the mutation targets the byte rule.
# The two file-integrity rules OVERLAP by design, so each one has to be mutated against the input only
# IT catches. A forged second line is caught by the line count AND by the byte accounting, so neutering
# either alone changes nothing; an embedded NUL is caught only by the byte accounting, because the file
# still holds exactly one complete line.
mut_att "the byte-accounting rule is load-bearing (the NUL case, which the line count does not see)" \
  'if [ "$bytes" -ne "$(( ${#line} + 1 ))" ]; then||if false; then' \
  'apply-topics: state=skipped skipped=a.topic\0evil\n' skipped
# ...and a forged SECOND line is refused by both, so only removing both shows what they are for. Two
# edits, one claim.
bothdir="$WORK/mboth"; mkdir -p "$bothdir"; cp "$D" "$bothdir/d.sh"
python3 - "$bothdir/d.sh" <<'PYEOF'
import sys
p = sys.argv[1]; s = open(p).read()
for old in ('    1) : ;;', 'if [ "$bytes" -ne "$(( ${#line} + 1 ))" ]; then'):
    assert old in s, "the mutation did not apply: %r not found" % old
s = s.replace('    1) : ;;', '    *) : ;;', 1)
s = s.replace('if [ "$bytes" -ne "$(( ${#line} + 1 ))" ]; then', 'if false; then', 1)
open(p, "w").write(s)
PYEOF
if [ $? -eq 0 ]; then
  bothf="$WORK/att.both"; printf 'apply-topics: state=skipped skipped=evil.topic\napply-topics: state=ok skipped=\n' > "$bothf"
  bothgot="$(bash -c '. "$1"; read_apply_attestation "$2"; printf "%s|%s\n" "$ATTEST_STATE" "$ATTEST_SKIPPED"' _ "$bothdir/d.sh" "$bothf")"
  [ "$bothgot" = "skipped|evil.topic" ] \
    && ok "with BOTH file-integrity rules gone, a forged first line IS read as the attestation" \
    || bad "the two-rule mutant answers [$bothgot]"
else bad "the two-rule mutation did not apply"; fi
# Neuter the PATTERN, not the subject: prefixing the subject left the wildcard matching anyway, and
# the mutant "failed" for the original reason — a mutation that did not mutate the rule.
mut_att "the name-shape rule is load-bearing" \
  '*[!A-Za-z0-9._\ -]*||__never_matches__' \
  'apply-topics: state=skipped skipped=a.topic; rm -rf /\n' skipped

# Ignore the contradiction: apply=0 with named skips would be answered OK, with the skipped topics
# NOT in the hold set, so their mirrors would start.
run_mut "the apply=0-with-skips contradiction is refused" \
  'if [ "$arc" -eq 0 ] && [ -n "$(_cs_norm "$skipped")" ]; then||if false; then' \
  0 0 "a.topic" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="
# Treat a non-numeric status as a success.
run_mut "a non-numeric status fails closed" \
  "case \"\$arc\" in ''|*[!0-9]*) DECISION_EXIT=1; _cs_emit; return 0 ;; esac||case \"\$arc\" in ''|*[!0-9]*) DECISION_VERDICT=OK; DECISION_RESUME=yes; DECISION_BRINGUP=yes; DECISION_EXIT=0; _cs_emit; return 0 ;; esac" \
  abc 0 "" "" ok "verdict=FAIL resume=no bringup=no exit=1 hold="

echo
if [ "$fails" -eq 0 ]; then echo "=== clean-slate-decision: OK ==="; exit 0; fi
echo "=== clean-slate-decision: $fails problem(s) ===" >&2; exit 1
