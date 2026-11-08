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
#     driver echoes a line AFTER calling the helper in a subshell, so an exit would swallow it.
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

printf '\ndev-cleanup-topic-recreation-fatal-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
