#!/usr/bin/env bash
# What a prod clean-slate recreate permits: the decision, as one function, so it can be driven by a
# test instead of being read out of the middle of an ssh-driven operator script.
#
#   clean_slate_decide <apply-rc> <ensure-rc> "<skipped topic names>" "<missing topic names>"
#
# sets, and (run directly) prints:
#   DECISION_VERDICT  OK | PARTIAL | FAIL
#   DECISION_RESUME   yes | no   -- may the es4->prod mirrors be started again at all
#   DECISION_BRINGUP  yes | no   -- may prod be brought up
#   DECISION_EXIT     the status prod-clean-slate.sh should exit with
#   DECISION_HOLD     the topics whose mirrors must STAY paused (space separated, may be empty)
#
# THE RULES, and what each one is protecting:
#
#   apply=0, ensure=0   OK       the whole declaration reconciled. Mirrors start; prod comes up. Any
#                                topic still MISSING from the broker is held anyway -- auto-create
#                                would otherwise let a mirror's first produce create it at the broker
#                                default partition count.
#   apply=0, named skips FAIL     a CONTRADICTION: apply-topics.sh cannot both reconcile everything and
#                                attest a skip ending. One of the two inputs is wrong, so neither is
#                                trusted.
#   apply=0, ensure!=0  FAIL     the partition-only topics did not get made. Nothing starts.
#   apply=9, no names   FAIL     exit 9 is DEFINED to carry the SKIPPED_TOPIC_NAMES line. Without it
#                                there is no way to tell which mirrors are safe, so none are.
#   apply=9, names      PARTIAL  apply-topics.sh reached the END of the declared list; the named topics
#                                could not be reconciled and every other one WAS created/updated.
#                                Mirrors with no stake in a named (or missing) topic start. Prod does
#                                NOT come up: a reset that did not fully reconcile stays DOWN (owner
#                                rule 2026-10-02). Exit stays non-zero.
#   apply=9, ensure!=0  FAIL     as above -- a failed partition-only step is not a partial success.
#   anything else       FAIL     the run did not complete. Nothing may be inferred about the rest of
#                                the declared list, so nothing starts.
#
# WHY PARTIAL IS NOT A BRING-UP. Starting a mirror is not the same act as bringing prod up. The mirrors
# are a copy path prod-clean-slate.sh paused itself before the wipe; their offsets live on es4; and
# every mirror started under PARTIAL is one whose own topics all reconciled. Before this existed, one
# drifted topic held all twelve es4->prod mirrors (2026-10-07, options.spx.strike-invasion.current).
#
# A non-numeric or empty status is FAIL, not an error: this is the last thing standing between a
# half-finished reset and a bring-up, and it must have an answer for every input.
clean_slate_decide() {
  local arc="${1-}" erc="${2-}" skipped="${3-}" missing="${4-}"
  DECISION_VERDICT=FAIL; DECISION_RESUME=no; DECISION_BRINGUP=no; DECISION_HOLD=""; DECISION_EXIT=1

  case "$arc" in ''|*[!0-9]*) DECISION_EXIT=1; _cs_emit; return 0 ;; esac
  case "$erc" in ''|*[!0-9]*) DECISION_EXIT=1; _cs_emit; return 0 ;; esac

  if [ "$arc" -eq 0 ] && [ -n "$(_cs_norm "$skipped")" ]; then
    # Impossible by construction, which is exactly why it is refused rather than ignored.
    DECISION_EXIT=1; _cs_emit; return 0
  fi

  if [ "$arc" -eq 0 ] && [ "$erc" -eq 0 ]; then
    DECISION_VERDICT=OK; DECISION_RESUME=yes; DECISION_BRINGUP=yes; DECISION_EXIT=0
    DECISION_HOLD="$(_cs_norm "$missing")"
  elif [ "$arc" -eq 9 ] && [ "$erc" -eq 0 ] && [ -n "$(_cs_norm "$skipped")" ]; then
    DECISION_VERDICT=PARTIAL; DECISION_RESUME=yes; DECISION_BRINGUP=no; DECISION_EXIT=9
    DECISION_HOLD="$(_cs_norm "$skipped $missing")"
  elif [ "$arc" -eq 0 ]; then
    DECISION_EXIT="$erc"
  else
    DECISION_EXIT="$arc"
  fi
  _cs_emit
}

# Collapse whitespace and drop duplicates, so the hold set is a set and the log line is readable.
_cs_norm() { printf '%s\n' ${1-} | awk 'NF && !seen[$0]++' | tr '\n' ' ' | sed 's/ *$//'; }

_cs_emit() {
  [ "${CLEAN_SLATE_DECISION_PRINT:-false}" = true ] || return 0
  printf 'verdict=%s resume=%s bringup=%s exit=%s hold=%s\n' \
    "$DECISION_VERDICT" "$DECISION_RESUME" "$DECISION_BRINGUP" "$DECISION_EXIT" "$DECISION_HOLD"
}

# Run directly: the SAME function, printing its answer. The test drives this, so there is no second
# copy of the rules to drift from the one above.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  set -uo pipefail
  CLEAN_SLATE_DECISION_PRINT=true
  clean_slate_decide "${1-}" "${2-}" "${3-}" "${4-}"
fi
