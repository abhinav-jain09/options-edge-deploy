#!/usr/bin/env bash
# evaluator-mutation-audit.sh — which of the A5 evaluator's protections does its suite actually BIND?
#
# NOT wired into the deploy job: it runs the whole suite once per mutation, so it costs minutes rather
# than seconds. It is a tool for whoever changes the evaluator or its tests, and the result belongs in
# the PR that changes them.
#
# The result line that used to sit here said "22 mutations, 22 killed". It was not evidence: the harness
# did not require a GREEN BASELINE, and its seed omitted oe-archive.crontab, so the copied suite failed
# case 41 unmutated — every "killed" was indistinguishable from a copy that was never going to pass. A
# harness that cannot tell those apart is the same defect as a test that greps for a string. Both are
# fixed; the run below is the first one whose result means anything.
#
# Last full run (2026-09-09, deploy b384f2bf + the redundancy removals): 22 mutations, 22 killed, none
# survived. The three that survived the run before were not weakened protections — they were
# REDUNDANT ones: the same property held in two places, so deleting either left the other catching
# everything and neither was testable on its own. That redundancy is what the audit is for.
#
# Which of the evaluator's protections does the suite actually bind?
#
# Disable one at a time and run the suite. A protection whose removal the suite does not notice is a
# protection no test is holding — which is how `shared_defects = R.corpus_defects(...)` turned out to be
# decorative: a duplicate check beside it reached the same verdict, so deleting the call changed nothing.
#
# Runs on a COPY of the unit, never the repo: the first version of this mutated the working tree while a
# review was reading it, which is its own small lesson about isolation.
set -uo pipefail
# The unit under audit is THIS checkout's, resolved from the script's own location — a hardcoded path
# would happily audit a different working tree than the one being reviewed and report about it (r17 #4).
HERE_CI="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO="${REPO:-$HERE_CI/../ops/archive}"
REPO="$(cd "$REPO" && pwd)"
WORKROOT=$(mktemp -d)
# EVERY file the suite reads, not just the ones with a .sh/.py/.env suffix. Omitting oe-archive.crontab
# made case 41 fail on an UNMUTATED copy — so every "killed" below was indistinguishable from a broken
# baseline, and the 22/22 result it produced was not evidence of anything (r17 #4).
seed() {
  rm -rf "$WORKROOT/u"; mkdir -p "$WORKROOT/u"
  cp "$REPO"/*.sh "$REPO"/*.py "$REPO"/*.env "$REPO"/oe-archive.crontab "$WORKROOT/u/"
  cp "$HERE_CI/../jenkins/market_calendar.py" "$WORKROOT/u/" 2>/dev/null || true
}
apply() {   # apply <file> <python-regex> <replacement>
  FILE="$WORKROOT/u/$1" PAT="$2" REP="$3" python3 -c "
import os, re, sys
p = os.environ['FILE']; s = open(p).read()
s2 = re.sub(os.environ['PAT'], os.environ['REP'], s, count=1)
if s2 == s: sys.exit(1)
open(p, 'w').write(s2)"
}
# A GREEN BASELINE FIRST. Without it "killed" means "the suite failed", which is also what a copy that
# was never going to pass looks like — and that is how a harness reports 22 kills while proving nothing.
baseline() {
  seed
  if bash "$WORKROOT/u/push-validation-artifact-test.sh" >/dev/null 2>&1; then
    echo "baseline  green (an unmutated copy passes)"
    return 0
  fi
  echo "BASELINE FAILED — the copied suite does not pass unmutated, so no result below would mean anything" >&2
  bash "$WORKROOT/u/push-validation-artifact-test.sh" 2>&1 | grep -E "^  FAIL" | head -5 >&2
  exit 2
}

check() {   # check <label> <file> <regex> <replacement>
  seed
  if ! apply "$2" "$3" "$4"; then printf 'SKIP      %s (pattern did not match)\n' "$1"; return; fi
  if bash "$WORKROOT/u/push-validation-artifact-test.sh" >/dev/null 2>&1; then
    printf 'SURVIVED  %s\n' "$1"
  else
    printf 'killed    %s\n' "$1"
  fi
}
baseline
E=oe-push-validation-artifact.sh
R=oe_corpus_reader.py
check "version_ok (pin, manifest, calendar, start, generation)" $E 'version_ok = \(manifest is not None[\s\S]*?\)\n' 'version_ok = True\n'
check "complete_ok (the whole COMPLETENESS clause)"             $E 'complete_ok = \([\s\S]*?\)\n'                    'complete_ok = True\n'
check "THRESHOLDS_FROZEN"                                        $E 'frozen = \(thresholds_state == "FROZEN"\)'       'frozen = True'
check "outcome/call pinned-field binding"                        $E 'mismatched\.append\('                            'None and ('
check "records outside the manifest"                             $E 'unmanifested = sum\(1 for k in logical if k not in manifest_entries\)' 'unmanifested = 0'
check "conflict detection"                                       $R 'conflicts_by_session\[sd_of\] = conflicts_by_session\.get\(sd_of, 0\) \+ 1' 'pass'
check "owed-day enumeration"                                     $E 'missing_days\.append\(day\)'                     'None'
check "stopping boundary is an RTH close"                        $E 'elif not R\.is_rth_close\(stopping, _cal\):'       'elif False:'
check "seal-vs-manifest generation"                              $E 'relabelled\.append\('                           'None and ('
check "-1 coordinates refused"                                   $E 'uncoordinated = 0 if manifest is None else sum\([\s\S]*?\)\n' 'uncoordinated = 0\n'
check "shared corpus_defects call"                               $E 'shared_defects = R\.corpus_defects\([\s\S]*?\)\n' 'shared_defects = []\n'
check "cell key (per-cell counter)"                              $R 'return "%s\|%s\|%s" % \(call\.get\("enteredState"\), roles, call\.get\("regime"\)\)' 'return "X|X|X"'
check "chain recomputation"                                      $R 'def chain\(domain, recs\):\n(    """[\s\S]*?"""\n)' 'def chain(domain, recs):\n\1    return None\n'
# A mutation must CHANGE BEHAVIOUR. This one appended a comment and changed nothing, so its "killed" was
# reporting the harness's own noise (r17 #4). It now makes every session COMPLETE, which is the thing the
# status is for.
check "session status: COMPLETE only when it is"                 $R 'status, why = "CORRUPT", "; "\.join\(errs\[:3\]\)' 'status, why = "COMPLETE", ""'
# --- the round-14 protections -----------------------------------------------------------------------
# The "live coordinate" mutation that used to sit here is gone with the code it targeted: the guard it
# disabled was fully redundant with the comparison below it, which is why it survived every run. One
# comparison now, and "the archive must still match the manifest" is the mutation that covers it.
check "outcome-only lineages get a session row"                  $R 'for c in list\(calls\) \+ list\(outcomes\):'      'for c in list(calls):'
check "seal semanticStamp must AGREE with its records"           $R 'stamp_bad = bool\(stamp_split\) and \(len\(stamp_split\) > 1 or stamp_split\[0\] != seal\.get\("semanticStamp"\)\)' 'stamp_bad = False'
check "cohort admits only the declared semantic stamp"           $E 'and c\.get\("semanticStamp"\) == semantic_stamp
'  '
'
check "attrition shape validation"                               $R 'def attrition_violations\(seal\):
(    """[\s\S]*?"""
)' 'def attrition_violations(seal):
    return []
'
check "a session that graded nothing is NOT a zero rate"         $R 'if graded == 0:
        return None'            'if False:
        return None'
check "the reader recomputes the digest from the payload"        $R 'dig = hashlib\.sha256\(canonical\(body\)\.encode\("utf-8"\)\)\.hexdigest\(\)' 'dig = rec.get("semanticDigest") or hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()'
check "COVERAGE is computed, not assumed"                        $E 'coverage = \(float\(len\(obs\)\) / float\(len\(prim\)\)\) if prim else None' 'coverage = 1.0 if prim else None'
check "lowest-offset collapse for replays"                       $R 'elif off is not None and \(prev\[2\] is None or off < prev\[2\]\):' 'elif False:'
echo done
