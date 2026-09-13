#!/usr/bin/env bash
# Tests for scripts/ci/lib/concurrent-suites.sh — the scheduler validate-services.sh runs its suites through.
#
# A scheduler in a deploy gate has exactly one way to be dangerous: turn a failing suite green. So these cases pin
# what that would take — a failure after a success, a failure that finishes first, a quiet failure, a missing
# script — plus the two cleanup properties: an abort signals the suites still running, and never a PID that was
# already reaped (Codex #1045 r1). Every case runs under each bash on this host, because the service-deploy agents
# are macOS and /bin/bash there is 3.2.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="$HERE/lib/concurrent-suites.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fail=0

bashes="$(command -v bash)"
[ -x /bin/bash ] && [ "$(command -v bash)" != /bin/bash ] && bashes="$bashes /bin/bash"

suite() { # name exit-status body...
  local f="$work/$1.sh" st="$2"; shift 2
  printf '#!/usr/bin/env bash\n%s\nexit %s\n' "$*" "$st" > "$f"; chmod +x "$f"
}
driver() { # file body — a set -euo pipefail script that sources the lib
  printf '#!/usr/bin/env bash\nset -euo pipefail\n. "%s"\nsuites_init "%s"\n%s\n' "$LIB" "$work" "$2" > "$1"
}
ok()  { printf '  ok   %-60s [%s]\n' "$1" "$B"; }
bad() { printf '  FAIL %-60s [%s]\n' "$1" "$B"; fail=1; }

suite slow-ok     0 'sleep 1; echo slow-ok-output'
suite fast-fail   3 'echo fast-fail-output'
suite quiet-ok    0 'echo quiet-ok-output'
suite loud-fail   5 'echo loud-fail-output'
suite pass-a      0 'echo pass-a-output'
suite pass-b      0 'true'
suite long        0 'echo $$ > "'"$work"'/long.pid"; exec sleep 30'

for B in $bashes; do
  "$B" -c 'echo "$BASH_VERSION"' | grep -qE '^[0-9]' || { bad "cannot run $B"; continue; }

  # 1) failures are aggregated, never short-circuited, and reported in START order whatever the finish order
  driver "$work/d1.sh" "
suites_start loud  'slow ok'   '$work/slow-ok.sh'
suites_start quiet 'contract'  '$work/fast-fail.sh'
suites_start quiet 'contract'  '$work/quiet-ok.sh'
suites_start loud  'loud fail' '$work/loud-fail.sh'
if suites_collect; then echo COLLECT=0; else echo COLLECT=1; fi
echo LIVE=\${#_suites_live[@]}"
  out="$("$B" "$work/d1.sh" 2>&1)" || true
  want="=== slow ok ===
slow-ok-output
FAIL: $work/fast-fail.sh
      fast-fail-output
=== loud fail ===
loud-fail-output
FAIL: $work/loud-fail.sh exited 5
COLLECT=1
LIVE=0"
  if [ "$out" = "$want" ]; then ok "mixed results: every failure reported, start order, quiet success silent"
  else bad "mixed results"; printf '%s\n' "--- got:" "$out" "--- want:" "$want" | sed 's/^/      /'; fi

  # 2) all green collects 0 and quiet successes print nothing
  driver "$work/d2.sh" "
suites_start quiet 'contract' '$work/pass-a.sh'
suites_start quiet 'contract' '$work/pass-b.sh'
suites_collect; echo COLLECT=\$?"
  out="$("$B" "$work/d2.sh" 2>&1)" || true
  [ "$out" = "COLLECT=0" ] && ok "all green: status 0, no output" || { bad "all green (got: $out)"; }

  # 3) a missing or non-executable suite fails closed at start
  chmod -x "$work/pass-b.sh"
  driver "$work/d3.sh" "
suites_start quiet 'contract' '$work/pass-a.sh'
suites_start quiet 'contract' '$work/pass-b.sh'
echo UNREACHED"
  st=0; out="$("$B" "$work/d3.sh" 2>&1)" || st=$?
  chmod +x "$work/pass-b.sh"
  if [ "$st" = 1 ] && printf '%s' "$out" | grep -qF "FAIL: $work/pass-b.sh missing or not executable" && ! printf '%s' "$out" | grep -q UNREACHED
  then ok "non-executable suite: exit 1 before anything is collected"; else bad "non-executable suite (st=$st: $out)"; fi

  # 4) the kill set is empty before any suite and after collection (set -u, bash 3.2 empty arrays)
  driver "$work/d4.sh" "
suites_kill_live; echo EMPTY-OK
suites_start quiet 'contract' '$work/pass-a.sh'
suites_collect
suites_kill_live; echo AFTER-OK"
  out="$("$B" "$work/d4.sh" 2>&1)" || true
  [ "$out" = "EMPTY-OK
AFTER-OK" ] && ok "kill with nothing live is a no-op under set -u" || bad "empty kill set (got: $out)"

  # 5) abort mid-collection: the trap signals ONLY the suite still running, never the reaped one
  # No sleep-and-hope: the first suite is LOUD, and suites_collect prints its header only after it has waited on it
  # and dropped its PID from the live set — so that line on the driver's stdout IS the reaped-state handshake.
  rm -f "$work/long.pid" "$work/live.txt" "$work/reaped.txt" "$work/d5.out"
  driver "$work/d5.sh" "
trap 'printf \"%s\" \"\${_suites_live[*]-}\" > \"$work/live.txt\"; suites_kill_live' EXIT
suites_start loud  'first' '$work/pass-a.sh'
suites_start quiet 'contract' '$work/long.sh'
echo \"\${_suites_pids[0]}\" > '$work/reaped.txt'
suites_collect"
  "$B" "$work/d5.sh" >"$work/d5.out" 2>&1 & dpid=$!
  ready=0
  for _ in $(seq 1 600); do   # up to 60 s on a loaded agent; normally the first poll or two
    if [ -s "$work/long.pid" ] && grep -qx '=== first ===' "$work/d5.out" 2>/dev/null; then ready=1; break; fi
    sleep 0.1
  done
  [ "$ready" = 1 ] || bad "abort: driver never reached the reaped state (out: $(cat "$work/d5.out" 2>/dev/null))"
  kill -TERM "$dpid"; wait "$dpid" 2>/dev/null || true
  lpid="$(cat "$work/long.pid" 2>/dev/null || echo none)"; live="$(cat "$work/live.txt" 2>/dev/null || echo none)"
  gone=0; for _ in $(seq 1 30); do kill -0 "$lpid" 2>/dev/null || { gone=1; break; }; sleep 0.1; done
  if [ "$live" = "$lpid" ] && [ "$gone" = 1 ] && [ "$live" != "$(cat "$work/reaped.txt")" ]
  then ok "abort: only the live suite is signalled, and it dies"
  else bad "abort (live set '$live', running pid $lpid, gone=$gone, reaped $(cat "$work/reaped.txt"))"; kill "$lpid" 2>/dev/null || true; fi
done

[ $fail -eq 0 ] && echo "=== concurrent-suites-test: OK ===" || { echo "=== concurrent-suites-test: FAILED ==="; exit 1; }
