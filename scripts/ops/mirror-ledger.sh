#!/usr/bin/env bash
# The paused-mirror LEDGER: start the agents a reset paused, keep the ones that must stay paused, and
# never lose an agent that is still down.
#
#   mirror_ledger_resume <paused-list> <filter-script> [<hold-topic> ...]
#
#     <paused-list>    the file pause_mirrors wrote: one "<launchd label> <plist path>" row per agent
#     <filter-script>  scripts/ops/mirror-topic-filter.sh — asked, per agent, whether it copies a
#                      held topic. Only consulted when at least one hold topic is given.
#     <hold-topic>...  topics an agent may NOT produce into right now (unreconciled, or missing from
#                      the broker and therefore auto-creatable at the wrong partition count)
#
#   returns 0 only when every row was either started or DELIBERATELY held; non-zero when an agent that
#   should be running is not, or when the ledger could not be rewritten.
#
# THE LEDGER IS THE ONLY RECORD that an agent is down, so the invariant is: a row leaves the file ONLY
# when its agent is confirmed loaded. Everything else keeps it -- held by a topic, failed to load,
# plist missing right now, or a bookkeeping failure that makes the new ledger untrustworthy (then the
# OLD file is left exactly as it was, which over-lists rather than under-lists: a later run bootstraps
# an agent that is already loaded, sees it loaded, and drops the row then). Deploy Codex rounds 1-2
# found both halves of this: a dropped missing-plist row, and unchecked writes to the scratch file that
# could end with the original deleted.
#
# Lives in its own file because scripts/ops/prod-clean-slate.sh is driven by ssh and sshpass and cannot
# be run by a test, while this can: scripts/ops/mirror-ledger-test.sh drives it over a fake ledger with
# a stubbed launchctl.

# Prefer the caller's logger (prod-clean-slate.sh's `say` tees to its log file).
_ml_say() { if declare -F say >/dev/null 2>&1; then say "$*"; else printf '%s\n' "$*"; fi; }

mirror_ledger_resume() {
  local list="${1-}" filter="${2-}"
  [ -n "$list" ] || { _ml_say "mirror_ledger_resume: no ledger path given"; return 1; }
  shift 2 2>/dev/null || shift $#
  [ -s "$list" ] || { _ml_say "no paused mirror agents listed ($list)"; return 0; }

  local keep="$list.keep" uid n=0 held=0 failed=0 broken=0 seen="" label plist verdict rc=0
  uid="$(id -u)"
  if ! : > "$keep" 2>/dev/null; then
    _ml_say "WARN: cannot write $keep — the ledger is left untouched and NO agent is started"
    return 1
  fi
  # A row that must stay paused. A failed append makes the new ledger incomplete, which is why it is
  # recorded instead of ignored: an incomplete ledger must not replace a complete one.
  _ml_hold_row() { printf '%s %s\n' "$1" "$2" >> "$keep" || broken=1; }

  while read -r label plist _rest; do
    [ -n "$label" ] || continue
    # A duplicate row would bootstrap the same agent twice; the second attempt looks like a failure.
    case " $seen " in *" $label "*) _ml_say "   (duplicate ledger row ignored): $label"; continue ;; esac
    seen="$seen $label"

    if [ -z "$plist" ]; then
      _ml_say "   KEPT paused (ledger row has no plist path): $label"; _ml_hold_row "$label" ""; held=$((held+1)); continue
    fi
    if [ ! -f "$plist" ]; then
      # Not a resumed agent: it stays listed so a later run picks it up when the plist is back.
      _ml_say "   KEPT paused (plist not found): $label"; _ml_hold_row "$label" "$plist"; held=$((held+1)); continue
    fi
    if [ "$#" -gt 0 ]; then
      if [ -z "$filter" ] || [ ! -x "$filter" ]; then
        _ml_say "   HELD paused (the topic filter $filter is missing or not executable): $label"
        _ml_hold_row "$label" "$plist"; held=$((held+1)); continue
      fi
      if ! verdict="$("$filter" "$plist" "$@" 2>&1)"; then
        _ml_say "   HELD paused: ${verdict:-$label (the filter failed to run)}"
        _ml_hold_row "$label" "$plist"; held=$((held+1)); continue
      fi
    fi
    launchctl bootstrap "gui/$uid" "$plist" >/dev/null 2>&1
    if launchctl list "$label" >/dev/null 2>&1; then
      n=$((n+1))
    else
      _ml_say "   WARN: mirror agent $label did not load"
      _ml_hold_row "$label" "$plist"; failed=$((failed+1))
    fi
  done < "$list"

  if [ "$broken" -ne 0 ]; then
    rm -f "$keep"
    _ml_say "WARN: could not record which agents stay paused — $list is left as it was (it now over-lists: a later run will drop the rows it finds loaded)"
    return 1
  fi
  if [ -s "$keep" ]; then
    if ! mv "$keep" "$list"; then
      rm -f "$keep"
      _ml_say "WARN: could not replace $list — it is left as it was (over-listing, see above)"
      return 1
    fi
  else
    rm -f "$keep" "$list"
  fi

  local extra=""
  [ "$held" -gt 0 ]   && extra="$extra, HELD $held still paused (a topic they copy was not reconciled)"
  [ "$failed" -gt 0 ] && extra="$extra, $failed failed to load"
  _ml_say "resumed $n mirror agent(s)$extra"
  [ "$failed" -eq 0 ] || rc=1
  return "$rc"
}
