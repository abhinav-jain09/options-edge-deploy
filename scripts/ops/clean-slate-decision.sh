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
# read_apply_attestation <file> -> ATTEST_STATE (ok|skipped|empty), ATTEST_SKIPPED, ATTEST_REASON
#
# apply-topics.sh writes EXACTLY ONE line, from one of its two endings, in one shape. So anything else
# in that file -- nothing, two lines, a truncated line, an unknown state, a name with characters a
# topic cannot contain -- was not written by an ending, and is read as NO attestation. The caller then
# has empty skipped names, which with the skip status is a FAIL.
#
# `wc -l` counts COMPLETE lines, which is deliberate: a write cut off midway leaves no newline and is
# therefore rejected rather than parsed.
read_apply_attestation() {
  ATTEST_STATE=""; ATTEST_SKIPPED=""; ATTEST_REASON=""
  local f="${1-}" lines
  if [ -z "$f" ] || [ ! -f "$f" ]; then ATTEST_REASON="no attestation file"; return 0; fi
  lines="$(wc -l < "$f" 2>/dev/null | tr -d ' ')"; lines="${lines:-0}"
  case "$lines" in
    1) : ;;
    0) ATTEST_REASON="the attestation file holds no complete line (an aborted or truncated write)"; return 0 ;;
    *) ATTEST_REASON="the attestation file holds $lines lines; exactly one is expected"; return 0 ;;
  esac
  # `read`, not `$(head -1)`: command substitution on a file with a NUL makes bash print a warning
  # about it, and the byte check below is what rejects it either way.
  local line=""; IFS= read -r line < "$f" || true
  # The line must account for EVERY byte in the file (itself plus its newline). Command substitution
  # DROPS embedded NULs, so without this a file carrying NULs could parse as a valid attestation out
  # of a line that is not what the file holds (deploy Codex round 3). It also rejects a stray CR at the
  # end, trailing bytes after the newline, and anything else the shape check would not see.
  local bytes; bytes="$(wc -c < "$f" 2>/dev/null | tr -d ' ')"; bytes="${bytes:-0}"
  if [ "$bytes" -ne "$(( ${#line} + 1 ))" ]; then
    ATTEST_REASON="the attestation file holds $bytes byte(s) but its one line accounts for $(( ${#line} + 1 ))"
    return 0
  fi
  # A bound, so nothing downstream is handed an unbounded list. The real line is ~40 bytes plus the
  # skipped names; 4096 is far past any plausible declaration and far below anything worth parsing.
  if [ "$bytes" -gt 4096 ]; then ATTEST_REASON="the attestation line is $bytes bytes, which is not one this script writes"; return 0; fi
  case "$line" in
    "apply-topics: state=ok skipped=") ATTEST_STATE=ok; return 0 ;;
    "apply-topics: state=ok skipped="*) ATTEST_REASON="state=ok names skipped topics, which contradicts itself"; return 0 ;;
    "apply-topics: state=skipped skipped="*)
      ATTEST_SKIPPED="${line#apply-topics: state=skipped skipped=}"
      if [ -z "$ATTEST_SKIPPED" ]; then
        ATTEST_REASON="state=skipped names no topic"; ATTEST_SKIPPED=""; return 0
      fi
      # Topic names only: letters, digits, dot, dash, underscore, separated by SINGLE spaces, with no
      # leading or trailing space. Anything else is not a list apply-topics.sh built out of
      # $OPTIONS_EDGE_TOPICS -- including a doubled space, which the first version of this rule allowed
      # while the comment beside it said single (deploy Codex round 3).
      case "$ATTEST_SKIPPED" in
        *[!A-Za-z0-9._\ -]*|" "*|*" "|*"  "*) ATTEST_REASON="the skipped list is not a plain list of topic names"; ATTEST_SKIPPED=""; return 0 ;;
      esac
      ATTEST_STATE=skipped; return 0 ;;
    *) ATTEST_REASON="the attestation line is not in the expected shape"; return 0 ;;
  esac
}

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
