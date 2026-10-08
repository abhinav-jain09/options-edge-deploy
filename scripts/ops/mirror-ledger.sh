#!/usr/bin/env bash
# The paused-mirror LEDGER: start the agents a reset paused, keep the ones that must stay paused, and
# never lose an agent that is still down.
#
#   mirror_ledger_resume <paused-list> <filter-script> [<hold-topic> ...]
#
#     <paused-list>    the file pause_mirrors wrote: one "<launchd label> <plist path>" row per agent.
#                      The label is the first field; EVERYTHING after it is the path, so a path with
#                      spaces round-trips (re-parsing a row into fields and re-serialising it is how a
#                      ledger silently rewrites somebody's path -- deploy Codex round 3).
#     <filter-script>  scripts/ops/mirror-topic-filter.sh — asked, per agent, whether it copies a
#                      held topic. Only consulted when at least one hold topic is given.
#     <hold-topic>...  topics an agent may NOT produce into right now (unreconciled, or missing from
#                      the broker and therefore auto-creatable at the wrong partition count)
#
#   returns 0 only when every row was either started or DELIBERATELY held; non-zero when an agent that
#   should be running is not, or when the ledger could not be read or rewritten.
#
# THE LEDGER IS THE ONLY RECORD that an agent is down, so the invariant is: a row leaves the file ONLY
# when its agent is confirmed loaded. Everything else keeps it -- held by a topic, failed to load,
# plist missing right now, or any failure that makes the new ledger untrustworthy (then the OLD file is
# left exactly as it was, which over-lists rather than under-lists: a later run bootstraps an agent
# that is already loaded, sees it loaded, and drops the row then).
#
# Three ways that invariant was broken before, each found by a deploy Codex round and each now a case
# in scripts/ops/mirror-ledger-test.sh:
#   r1  a missing plist dropped the row, so a briefly absent plist left an agent paused and unlisted.
#   r2  the writes to the scratch file were unchecked, so a failed append could end with the original
#       ledger deleted and nothing recording the agents that stayed down.
#   r3  an UNREADABLE non-empty ledger passed the -s test, the read loop ran zero times, and the empty
#       scratch file then replaced... nothing: the original was deleted as "everything resumed".
# So the rows are read ONCE, up front, with the read's own status checked, and the count of rows
# processed must match the count read before anything replaces the file.
#
# CONCURRENCY: a lock directory beside the ledger. Two clean slates working one ledger would interleave
# bootstraps and rewrites; the second one refuses instead. A stale lock (from a crash) also refuses,
# which is the safe direction -- it leaves every row listed.
#
# Lives in its own file because scripts/ops/prod-clean-slate.sh is driven by ssh and sshpass and cannot
# be run by a test, while this can.

# Prefer the caller's logger (prod-clean-slate.sh's `say` tees to its log file).
_ml_say() { if declare -F say >/dev/null 2>&1; then say "$*"; else printf '%s\n' "$*"; fi; }

# mirror_ledger_record <ledger> <rows-file> [command ...] — put rows INTO the ledger before anything is
# stopped, and run <command> (the stopping) while the ledger LOCK is still held.
#
# The other half of the invariant, and the half that was missing (deploy Codex round 5): pause_mirrors
# merged its freshly-discovered agents into the ledger with `sort -u old new > merged && mv merged old`
# and then booted every one of them out REGARDLESS of whether that merge had worked. A failed merge
# therefore stopped agents that nothing recorded, and a later resume could report success with them
# still down.
#
# So this merges AND VERIFIES: after the move, every row of <rows-file> must be readable back out of
# the ledger. Anything less is a non-zero return, and the caller must then stop nothing.
#
# AND THE STOPPING RUNS UNDER THE SAME LOCK. Releasing it first left a window in which a concurrent
# resume could see an agent loaded, drop its row, and then this caller would boot that agent out with
# nothing recording it (deploy Codex round 6). So the caller passes its stop step as <command>: it runs
# after the verification, before the lock is released, and its status is this function's status.
mirror_ledger_record() {
  local list="${1-}" rows="${2-}" lock merged missing=0 n=0 row hook_rc=0
  [ -n "$list" ] && [ -n "$rows" ] || { _ml_say "mirror_ledger_record: need <ledger> <rows-file>"; return 1; }
  shift 2 2>/dev/null || shift $#
  if [ -L "$list" ]; then
    _ml_say "WARN: $list is a SYMLINK — refusing to record into it; nothing may be stopped"
    return 1
  fi
  [ -r "$rows" ] || { _ml_say "WARN: cannot read $rows — nothing may be stopped"; return 1; }
  n="$(grep -c . -- "$rows" 2>/dev/null)"; n="${n:-0}"
  [ "$n" -gt 0 ] || { _ml_say "no mirror agents to record"; return 0; }

  lock="$list.lock"
  if ! mkdir "$lock" 2>/dev/null; then
    if [ -d "$lock" ]; then _ml_say "WARN: $lock exists — another run holds this ledger; nothing may be stopped"
    else _ml_say "WARN: cannot create $lock (is $(dirname -- "$list") writable?) — nothing may be stopped"; fi
    return 1
  fi
  touch "$list" 2>/dev/null || { _ml_say "WARN: cannot create $list — nothing may be stopped"; rmdir "$lock" 2>/dev/null; return 1; }
  if ! merged="$(mktemp "$list.rec.XXXXXX" 2>/dev/null)"; then
    _ml_say "WARN: cannot create a scratch file beside $list — nothing may be stopped"; rmdir "$lock" 2>/dev/null; return 1
  fi
  if ! sort -u -- "$list" "$rows" > "$merged"; then
    rm -f "$merged"; _ml_say "WARN: could not merge $rows into $list — nothing may be stopped"; rmdir "$lock" 2>/dev/null; return 1
  fi
  if ! mv "$merged" "$list"; then
    rm -f "$merged"; _ml_say "WARN: could not replace $list — nothing may be stopped"; rmdir "$lock" 2>/dev/null; return 1
  fi
  # READ IT BACK. A merge that "succeeded" into a file that does not hold the rows is the case this
  # exists for; every row must be there, byte for byte.
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    grep -qxF -- "$row" "$list" 2>/dev/null || missing=$((missing+1))
  done < "$rows"
  if [ "$missing" -ne 0 ]; then
    rmdir "$lock" 2>/dev/null
    _ml_say "WARN: $missing of $n row(s) are NOT in $list after the merge — nothing may be stopped"
    return 1
  fi
  _ml_say "recorded $n mirror agent(s) in $list"
  # The caller's stop step, under the lock. Its failure is this function's failure: an agent that is
  # still running after it was supposed to be stopped must not look like a successful pause.
  if [ "$#" -gt 0 ]; then
    "$@" || hook_rc=$?
  fi
  rmdir "$lock" 2>/dev/null
  return "$hook_rc"
}

mirror_ledger_resume() {
  local list="${1-}" lock rc
  [ -n "$list" ] || { _ml_say "mirror_ledger_resume: no ledger path given"; return 1; }
  # A SYMLINK is refused outright. A DANGLING one satisfies `! -e` and would otherwise read as "nothing
  # was paused", which is the same silent all-clear as the unreadable file below -- prod comes up while
  # the mirrors stay down (deploy Codex round 4). A live one raises a second question this file should
  # not answer: whether replacing the ledger should write through the link or over it. The real ledger
  # is a regular file (~/oe-ops/.prod-mirrors-paused), so refusing costs nothing.
  if [ -L "$list" ]; then
    _ml_say "WARN: $list is a SYMLINK — refusing to work it; nothing is started and every row stays listed"
    return 1
  fi
  # No ledger at all = nothing was paused = nothing to do, and nothing to lock. (An EMPTY ledger is
  # handled inside, after the read, so that an unreadable one cannot be mistaken for it.)
  if [ ! -e "$list" ]; then _ml_say "no paused mirror agents listed ($list)"; return 0; fi
  lock="$list.lock"
  if ! mkdir "$lock" 2>/dev/null; then
    # Two different causes, and saying the wrong one sends the operator looking for a process that is
    # not there: the directory beside the ledger may simply not be writable.
    if [ -d "$lock" ]; then
      _ml_say "WARN: $lock exists — another run holds this ledger (or one crashed holding it). Nothing is started; every row stays listed."
    else
      _ml_say "WARN: cannot create $lock (is $(dirname -- "$list") writable?) — nothing is started; every row stays listed."
    fi
    return 1
  fi
  _ml_resume_locked "$@"; rc=$?
  rmdir "$lock" 2>/dev/null
  return "$rc"
}

_ml_resume_locked() {
  local list="$1" filter="${2-}"
  shift 2 2>/dev/null || shift $#

  # ONE read, status checked. `-s` says "not empty", which an unreadable file also satisfies.
  local rows read_rc
  rows="$(cat -- "$list" 2>/dev/null)"; read_rc=$?
  if [ "$read_rc" -ne 0 ] || [ ! -r "$list" ]; then
    _ml_say "WARN: cannot READ $list — nothing is started and the file is left untouched"
    return 1
  fi
  if [ -z "$rows" ]; then _ml_say "no paused mirror agents listed ($list)"; return 0; fi

  local total processed=0
  total="$(printf '%s\n' "$rows" | grep -c . )"

  # A per-run scratch name, so two runs cannot write each other's (the lock above makes that moot for
  # this ledger, but a fixed name also collides with a stale file left by a crash).
  local keep
  if ! keep="$(mktemp "$list.keep.XXXXXX" 2>/dev/null)"; then
    _ml_say "WARN: cannot create a scratch file beside $list — nothing is started and the file is left untouched"
    return 1
  fi

  local uid n=0 held=0 failed=0 broken=0 seen="" label plist verdict
  uid="$(id -u)"
  # A row that must stay paused. A failed append makes the new ledger incomplete, which is recorded
  # rather than ignored: an incomplete ledger must not replace a complete one.
  _ml_hold_row() { printf '%s %s\n' "$1" "$2" >> "$keep" || broken=1; }

  while IFS= read -r row; do
    [ -n "$row" ] || continue
    processed=$((processed+1))
    # First field is the label; the REST is the path, verbatim.
    label="${row%% *}"
    if [ "$label" = "$row" ]; then plist=""; else plist="${row#* }"; fi
    case " $seen " in *" $label "*) _ml_say "   (duplicate ledger row ignored): $label"; continue ;; esac
    seen="$seen $label"

    if [ -z "$plist" ]; then
      _ml_say "   KEPT paused (ledger row has no plist path): $label"; _ml_hold_row "$label" ""; held=$((held+1)); continue
    fi
    # The path must be ABSOLUTE, which is how pause_mirrors writes it. A row whose label contains a
    # space would otherwise hand "<rest of the label> <path>" to launchctl as a path: not a
    # representable row, so it is kept and reported rather than acted on or rewritten.
    case "$plist" in
      /*) : ;;
      *)  _ml_say "   KEPT paused (ledger row is not '<label> <absolute plist path>'): $label"
          _ml_hold_row "$label" "$plist"; held=$((held+1)); continue ;;
    esac
    if [ ! -f "$plist" ]; then
      _ml_say "   KEPT paused (plist not found): $label"; _ml_hold_row "$label" "$plist"; held=$((held+1)); continue
    fi
    if [ "$#" -gt 0 ]; then
      if [ -z "$filter" ] || [ ! -x "$filter" ]; then
        _ml_say "   HELD paused (the topic filter ${filter:-<none>} is missing or not executable): $label"
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
  done <<< "$rows"

  if [ "$broken" -ne 0 ] || [ "$processed" -ne "$total" ]; then
    rm -f "$keep"
    _ml_say "WARN: the new ledger is incomplete ($processed of $total row(s) handled, write-failures=$broken) — $list is left as it was (it now over-lists: a later run drops the rows it finds loaded)"
    return 1
  fi
  if [ -s "$keep" ]; then
    if ! mv "$keep" "$list"; then
      rm -f "$keep"
      _ml_say "WARN: could not replace $list — it is left as it was (over-listing, see above)"
      return 1
    fi
  else
    rm -f "$keep"
    rm -f "$list"
  fi

  local extra=""
  [ "$held" -gt 0 ]   && extra="$extra, HELD $held still paused (a topic they copy was not reconciled)"
  [ "$failed" -gt 0 ] && extra="$extra, $failed failed to load"
  _ml_say "resumed $n mirror agent(s)$extra"
  [ "$failed" -eq 0 ]
}
