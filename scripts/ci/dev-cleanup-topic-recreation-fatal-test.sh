#!/usr/bin/env bash
# dev-cleanup-topic-recreation-fatal-test.sh — a failed topic recreation must STOP the clean, not be
# discarded on the way into the scale-up.
#
# THE BUG. dev-cleanup.sh ran `ensure_topics` and passed its status to
# resume_dev_mirrors_if_declared, which returns 0 whenever that status is non-zero — deliberately, so
# a failed topic pass does not strand the es4 mirrors. But the CALLER read that 0 and carried on. So
# a wipe that deleted every topic and then failed to recreate them was followed by workloads scaling
# up against topics that did not exist: Streams apps crash-loop, and anything auto-created takes the
# broker DEFAULT shape — for es.mbo.coverage, a delete-retention log where a compacted per-contract
# fact belongs. The barrier that would catch that shape runs at deploy time, not here
# (Codex deploy r15).
#
# The function is EXTRACTED and run against stubs rather than sourcing dev-cleanup.sh, which
# dispatches on its arguments at the bottom and would start doing things.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/../ops/dev-cleanup.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }

# The real function, lifted out by name.
awk '/^ensure_topics_or_refuse\(\) \{/,/^\}/' "$SCRIPT" > "$TMP/fn.sh"
if ! grep -q 'ensure_topics_or_refuse' "$TMP/fn.sh"; then
  echo "FAIL: could not extract ensure_topics_or_refuse from $SCRIPT"; exit 1
fi

run_with() { # run_with <ensure_topics exit status>
  cat > "$TMP/drive.sh" <<DRIVE
set -uo pipefail
ensure_topics() { echo "stub ensure_topics -> $1"; return $1; }
resume_dev_mirrors_if_declared() { echo "stub resume(\$1)"; return 0; }
$(cat "$TMP/fn.sh")
# The helper's own status, carried to the driver's exit. Two bugs here, both mine and both making
# every arm read as a real failure: \`exit \$?\` after the trailing echo took the ECHO's status, and
# then the replacement used unescaped \$drv_rc inside an UNQUOTED heredoc, so it was expanded when
# the driver was WRITTEN (to nothing) rather than when it ran. The escapes are load-bearing.
drv_rc=0
# NO SUBSHELL. With \`( ensure_topics_or_refuse )\` an \`exit 1\` inside the helper killed only the
# subshell, so AFTER-THE-HELPER still printed and the return-not-exit arm passed against a helper
# that exits — the arm could not see the difference it claimed to test. My own mutation caught it.
ensure_topics_or_refuse || drv_rc=\$?
[ "\$drv_rc" -eq 0 ] && echo "REACHED-THE-SCALE-UP"
echo "AFTER-THE-HELPER"
exit \$drv_rc
DRIVE
  bash "$TMP/drive.sh" 2>&1
}

# 1) A FAILED recreation must return non-zero and never reach the scale-up.
out="$(run_with 1)"; rc=$?
if [ "$rc" -ne 0 ] && ! printf '%s' "$out" | grep -q "REACHED-THE-SCALE-UP"; then
  ok "a failed ensure_topics returns non-zero and never reaches the scale-up"
else
  bad "a failed ensure_topics was discarded:"; printf '%s\n' "$out" | tail -3
fi

# 1b) RETURNS, does not EXIT. The first version called exit, which killed the process whose next act
#     on the automatic CLEAN slot is the overnight start — the script's own recovery attempt. The
#     driver echoes a line AFTER calling the helper — NOT in a subshell, which is the point: with
#     one, an exit killed only the subshell and this arm passed against a helper that exits.
if printf '%s' "$(run_with 1)" | grep -q "AFTER-THE-HELPER"; then
  ok "it RETURNS rather than exiting, so a caller's recovery path is still reachable"
else bad "the helper exited the process — the CLEAN slot's recovery attempt would be skipped"; fi

# 2) The mirror helper is called with the REAL status, and what that means is that the mirrors stay
#    PAUSED — the helper deliberately does not resume on a non-zero status. An earlier version of
#    this arm said a skipped call "would strand them", which is the opposite of what the real helper
#    does, so the arm described an outcome it was not testing (Codex deploy r16).
if printf '%s' "$(run_with 1)" | grep -q "stub resume(1)"; then
  ok "the mirror helper receives the TRUE status, so the mirrors stay paused by design"
else bad "the mirror helper was not told the real status"; fi

# 3) A SUCCESSFUL recreation still proceeds. Without this the refusal could be achieved by
#    refusing everything, which no amount of loudness would make correct.
out="$(run_with 0)"; rc=$?
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "REACHED-THE-SCALE-UP"; then
  ok "a successful ensure_topics proceeds to the scale-up"
else bad "a successful recreation was refused:"; printf '%s\n' "$out" | tail -3; fi

# 4) And every call site uses it: the helper existing proves nothing if a caller still runs the old
#    discard-the-status form.
if grep -q 'ensure_topics; resume_dev_mirrors_if_declared' "$SCRIPT"; then
  bad "a caller still uses 'ensure_topics; resume_dev_mirrors_if_declared' and discards the status"
else
  # EVERY CALL SITE MUST HANDLE THE STATUS. Counting bare calls was the wrong check the moment the
  # helper started RETURNING instead of exiting: a bare `ensure_topics_or_refuse` is now precisely
  # the bug — the status would be dropped again. So each call must be followed by `||`.
  sites="$(grep -n 'ensure_topics_or_refuse' "$SCRIPT" | grep -v 'ensure_topics_or_refuse()' | grep -v '^[0-9]*:#')"
  n="$(printf '%s\n' "$sites" | grep -c . )"
  unhandled="$(printf '%s\n' "$sites" | grep -v '||' || true)"
  if [ "$n" -ge 4 ] && [ -z "$unhandled" ]; then
    ok "all $n call sites call it AND handle its status"
  else
    bad "$n call sites, of which these drop the status:"; printf '%s\n' "$unhandled" | head -4
  fi
fi

# ---- THE REAL SHAPE CHECK, not a stub that returns 1 ------------------------------------------
#
# The arms above drive the refusing helper with `ensure_topics` stubbed as already failing, so they
# could not catch the deeper bug: reconcile_declared_topics COUNTED unresolved drift and then
# returned 0 regardless, and ensure_topics dropped its verdict — so the refusal could never fire for
# the failures it exists for (Codex deploy r17). These arms run the real function.
awk '/^reconcile_declared_topics\(\) \{/,/^\}/' "$SCRIPT" > "$TMP/rec.sh"
grep -q 'shape check' "$TMP/rec.sh" || { echo "FAIL: could not extract reconcile_declared_topics"; exit 1; }

run_reconcile() { # run_reconcile <describe output for the mock cluster>
  cat > "$TMP/rec-drive.sh" <<RECDRIVE
set -uo pipefail
KT=mock_kt; BS=mock:9092
OPTIONS_EDGE_TOPICS="es.mbo.coverage:1"
mock_kt() { printf '%s\n' "\$DESCRIBE"; }
# topic_desired sets the declared shape the real function compares against.
topic_desired() { DPARTS=1; DPOL=compact; DRET=-1; DRB=-1; DDR=""; DMB=1048576; }
$(cat "$TMP/rec.sh")
reconcile_declared_topics
RECDRIVE
  DESCRIBE="$1" bash "$TMP/rec-drive.sh" 2>&1
}

# A) The topic is ABSENT right after create: unresolved drift, and the function must say so.
out="$(DESCRIBE="Topic: something.else  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=delete" \
       run_reconcile "Topic: something.else  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=delete")"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "is ABSENT right after create"; then
  ok "a declared topic absent after create is reported as a FAILURE, not just warned about"
else bad "an absent declared topic returned success:"; printf '%s\n' "$out" | tail -3; fi

# B) Nothing describable at all: "not verified" must not read as "verified fine".
out="$(run_reconcile "")"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "NOT verified this run"; then
  ok "an undescribable cluster is a failure, not a silent pass"
else bad "an undescribable cluster returned success:"; printf '%s\n' "$out" | tail -3; fi

# C) And the correct shape still passes, so the failures above are not achieved by failing always.
out="$(run_reconcile "Topic: es.mbo.coverage  PartitionCount: 1  ReplicationFactor: 1  Configs: cleanup.policy=compact,retention.ms=-1,retention.bytes=-1,max.message.bytes=1048576")"; rc=$?
if [ "$rc" -eq 0 ]; then ok "the declared shape, present and correct, still passes"
else bad "the correct shape was reported as drift:"; printf '%s\n' "$out" | tail -4; fi

# D) ensure_topics must PROPAGATE the shape verdict. Arm A proves reconcile_declared_topics reports
#    drift; this proves ensure_topics does not then drop it — the mutation that survived arm A.
#
#    HONEST SCOPE: this one is STRUCTURAL, not executed. Driving ensure_topics for real needs a
#    mocked deploy checkout with a topics.env and a working create path, which is a larger harness
#    than this file is. So it asserts that each of the three sub-calls is status-handled and that
#    the function returns non-zero when any was — and it says so rather than implying it ran them.
body="$(awk '/^ensure_topics\(\) \{/,/^\}/' "$SCRIPT")"
missing=""
for sub in ensure_partition_only_topics reconcile_declared_topics ensure_dev_only_estimate_topics; do
  printf '%s\n' "$body" | grep -qE "^[[:space:]]*$sub \|\| rc=1$" || missing="$missing $sub"
done
if [ -z "$missing" ] && printf '%s\n' "$body" | grep -q 'return 1'; then
  ok "ensure_topics handles all three sub-call statuses and returns non-zero (structural)"
else
  bad "ensure_topics drops a sub-call's status:${missing:- none missing, but no return 1}"
fi

printf '\ndev-cleanup-topic-recreation-fatal-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
