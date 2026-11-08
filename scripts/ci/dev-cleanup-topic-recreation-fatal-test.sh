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
ensure_topics_or_refuse
echo "REACHED-THE-SCALE-UP"
DRIVE
  bash "$TMP/drive.sh" 2>&1
}

# 1) A FAILED recreation must stop the clean before anything scales.
out="$(run_with 1)"; rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "Refusing to continue" \
   && ! printf '%s' "$out" | grep -q "REACHED-THE-SCALE-UP"; then
  ok "a failed ensure_topics refuses the clean and never reaches the scale-up"
else
  bad "a failed ensure_topics was discarded:"; printf '%s\n' "$out" | tail -3
fi

# 2) The mirrors are still told, so the fix did not trade one silence for another.
if printf '%s' "$(run_with 1)" | grep -q "stub resume(1)"; then
  ok "and the mirror helper is still called with the real status"
else bad "the mirror helper was skipped — a failed topic pass would strand them"; fi

# 3) A SUCCESSFUL recreation still proceeds. Without this the refusal could be achieved by
#    refusing everything, which no amount of loudness would make correct.
out="$(run_with 0)"; rc=$?
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "REACHED-THE-SCALE-UP"; then
  ok "a successful ensure_topics proceeds to the scale-up"
else bad "a successful recreation was refused:"; printf '%s\n' "$out" | tail -3; fi

# 4) And every call site uses it: the helper existing proves nothing if a caller still runs the old
#    discard-the-status form.
if ! grep -q 'ensure_topics; resume_dev_mirrors_if_declared' "$SCRIPT"; then
  n="$(grep -c '^\s*ensure_topics_or_refuse$' "$SCRIPT")"
  if [ "$n" -ge 4 ]; then
    ok "all $n call sites use the refusing form, and none uses the discarding one"
  else bad "only $n call sites use the refusing form"; fi
else
  bad "a caller still uses 'ensure_topics; resume_dev_mirrors_if_declared' and discards the status"
fi

printf '\ndev-cleanup-topic-recreation-fatal-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
