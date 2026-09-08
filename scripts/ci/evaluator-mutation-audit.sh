#!/usr/bin/env bash
# evaluator-mutation-audit.sh — which of the A5 evaluator's protections does its suite actually BIND?
#
# NOT wired into the deploy job: it runs the whole suite once per mutation, so it costs minutes rather
# than seconds. It is a tool for whoever changes the evaluator or its tests, and the result belongs in
# the PR that changes them. Last full run (2026-09-08, deploy cf2c2227): 13 mutations, 13 killed, none
# survived.
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
REPO=/private/tmp/oe-deploy-a5/scripts/ops/archive
WORKROOT=$(mktemp -d)
seed() { rm -rf "$WORKROOT/u"; mkdir -p "$WORKROOT/u"; cp "$REPO"/*.sh "$REPO"/*.py "$REPO"/*.env "$WORKROOT/u/"; }
apply() {   # apply <file> <python-regex> <replacement>
  FILE="$WORKROOT/u/$1" PAT="$2" REP="$3" python3 -c "
import os, re, sys
p = os.environ['FILE']; s = open(p).read()
s2 = re.sub(os.environ['PAT'], os.environ['REP'], s, count=1)
if s2 == s: sys.exit(1)
open(p, 'w').write(s2)"
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
E=oe-push-validation-artifact.sh
R=oe_corpus_reader.py
check "version_ok (pin, manifest, calendar, start, generation)" $E 'version_ok = \(manifest is not None[\s\S]*?\)\n' 'version_ok = True\n'
check "complete_ok (the whole COMPLETENESS clause)"             $E 'complete_ok = \([\s\S]*?\)\n'                    'complete_ok = True\n'
check "THRESHOLDS_FROZEN"                                        $E 'frozen = \(thresholds_state == "FROZEN"\)'       'frozen = True'
check "outcome/call pinned-field binding"                        $E 'mismatched\.append\('                            'None and ('
check "records outside the manifest"                             $E 'unmanifested = sum\(1 for k in logical if k not in manifest_entries\)' 'unmanifested = 0'
check "conflict detection"                                       $E 'conflicts_total = read\.get\("conflictsTotal", 0\)' 'conflicts_total = 0'
check "owed-day enumeration"                                     $E 'missing_days\.append\(day\)'                     'None'
check "seal-vs-manifest generation"                              $E 'relabelled\.append\('                           'None and ('
check "-1 coordinates refused"                                   $E 'uncoordinated = 0 if manifest is None else sum\([\s\S]*?\)\n' 'uncoordinated = 0\n'
check "shared corpus_defects call"                               $E 'shared_defects = R\.corpus_defects\([\s\S]*?\)\n' 'shared_defects = []\n'
check "cell key (per-cell counter)"                              $R 'return "%s\|%s\|%s" % \(call\.get\("enteredState"\), roles, call\.get\("regime"\)\)' 'return "X|X|X"'
check "chain recomputation"                                      $R 'def chain\(domain, recs\):\n(    """[\s\S]*?"""\n)' 'def chain(domain, recs):\n\1    return None\n'
check "session status: COMPLETE only when it is"                 $R 'status, why = "COMPLETE", ""'                    'status, why = "COMPLETE", ""  # noqa'
echo done
