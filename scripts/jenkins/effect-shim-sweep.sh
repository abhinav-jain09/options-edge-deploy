#!/bin/bash
# THE SWEEP, WITH ITS INVENTORY TAKEN FROM THE IMPLEMENTATION.
#
# For every protection in the shim and the integrity checker, remove that protection and re-run the
# suite. A protection whose removal leaves the suite green has no regression coverage; a CASE that never
# goes red under any removal is decorative.
#
# WHY IT ENUMERATES THE CODE RATHER THAN A LIST. The first two sweeps were driven by a list the author
# maintained by hand, and each time review removed a protection that was not on it -- the argument
# vector, the exec-vs-wrap status, the signal traps, the empty-directory clause. A hand-written
# inventory has exactly the failure mode as the tests it is auditing: it covers what someone remembered.
#
# THREE THINGS THIS SWEEP GOT WRONG WHILE CLAIMING OTHERWISE, and what each is now:
#
#   1. IT REFUSED ON NOTHING. The header said it "REFUSES to report success if it cannot parse a line
#      that looks like a protection" and no such path existed. The inventory was seven LINE-shaped
#      regexes; a refusal branch written in any other shape was simply not in it, and therefore silently
#      exempt from the audit. `*) usage "unknown argument '$1'" ;;` -- a real refusal, mid-case-branch,
#      already in the tree -- was one of them. The scan is now two parts: a CLASSIFIER that recognises
#      protection SITES by token, and a RESIDUAL SCAN that looks for the tokens a protection is made of
#      and reports UNCLASSIFIED for any occurrence no site took. UNCLASSIFIED is a failure, not a
#      silence, and adding a refusal branch in a shape this cannot classify stops the sweep.
#
#   2. A SYNTAX ERROR IS NOT A REMOVED PROTECTION. Mutation used to prefix `true || ` to the whole LINE.
#      On `--dir)  [ $# -ge 2 ] || usage "--dir needs a path";  dir="$2";  shift 2 ;;` that ate the case
#      label, the file stopped parsing, every integrity case went red for that reason, and the sweep
#      recorded "covered". Of three mappings a reviewer then checked by hand, two were this. Mutation is
#      now SITE-precise -- the refusal CALL becomes `true` and the line around it is untouched -- every
#      mutant is `bash -n`-checked before it is run, and a mutant that does not parse is reported
#      UNMUTABLE and counted as NOT covered. It is never evidence.
#
#   3. RED IS NOT COVERAGE UNLESS THE PROGRAM STILL RUNS. Removing one protection must leave a working
#      program that no longer refuses one thing. The suite's own POSITIVE controls are the anchor: a
#      positive control asserts the shim does NOT refuse when it should not, so REMOVING a protection
#      cannot make one fail. If one does fail, the mutant is broken rather than neutralised and its reds
#      are attributable to nothing; that is reported NOT ATTRIBUTABLE and counted as NOT covered.
#
# WHAT THIS DOES NOT DO. It audits the coverage of the protections the shim HAS. It says nothing about
# the four cases the shim does not cover -- a deleted wrapper, an absolute-path invocation, a copy
# outside the checkout, and THE SHIM'S OWN INTEGRITY. Those are LIMITS, pinned by LIMIT cases in the
# suite, and a green sweep is not evidence about any of them. In particular: effect-shim-integrity.sh,
# effect-shim-digest.txt and _shim.sh all live in the workspace the job owns, so the integrity checks
# catch an ACCIDENT -- a wrapper deleted, a permission dropped, an entry added, a file edited by
# something that did not also edit the digest -- and nothing more. Two LIMIT cases assert the green
# result a coordinated edit produces. The count this script prints (32 of 32 at the time of writing,
# and whatever the inventory finds when you run it) is a statement about coverage of the protections
# in these two files; it is not a statement that the shim was intact.
#
# Usage: effect-shim-sweep.sh [--verbose]
set -u
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
verbose=false; [ "${1:-}" = "--verbose" ] && verbose=true

# THE SWEEP NEVER TOUCHES THE CHECKOUT. It used to mutate `scripts/jenkins/` in place and restore on
# EXIT, which is fine until the run is killed: review interrupted a long sweep and found a mutant
# _shim.sh and a MATCHING re-recorded digest left behind in the reviewed workspace, where the next
# integrity check reports green against altered payload. A trap cannot cover SIGKILL, a panic, or a
# closed laptop, so the payload is COPIED and every mutation happens to the copy. What is left behind
# on any exit is a temporary directory.
work="$(mktemp -d)"
PAYLOAD="$work/jenkins"
cp -R "$SRC" "$PAYLOAD"
trap 'cp "$work/_shim.sh" "$PAYLOAD/effect-shim/_shim.sh" 2>/dev/null || true' EXIT

HERE="$PAYLOAD"
SHIM="$PAYLOAD/effect-shim/_shim.sh"
INTEG="$PAYLOAD/effect-shim-integrity.sh"
SUITE="$PAYLOAD/effect-shim-test.sh"
DIGEST="$PAYLOAD/effect-shim-digest.txt"
cp "$SHIM" "$work/_shim.sh"; cp "$INTEG" "$work/integ"; cp "$DIGEST" "$work/digest"

restore() { cp "$work/_shim.sh" "$SHIM"; cp "$work/integ" "$INTEG"; cp "$work/digest" "$DIGEST"; }
# RE-RECORD THE DIGEST for the mutated shim. Without this, every mutation of _shim.sh also breaks the
# digest, every integrity case goes red, and the sweep calls the protection "covered" when what went red
# was the checksum -- the same passing-for-the-wrong-reason the sweep exists to find.
redigest() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$SHIM" | cut -d' ' -f1 > "$DIGEST"
  else shasum -a 256 "$SHIM" | cut -d' ' -f1 > "$DIGEST"; fi
}
run_suite() { KIND_LOG="${KIND_LOG:-/dev/null}" bash "$SUITE" 2>&1; }

# --- the inventory, read out of the two files ----------------------------------------------------
# SITES are byte spans, not lines: the mutation replaces exactly the protection and leaves the syntax
# around it alone. RESIDUE is the same tokens seen from the other side -- anything that looks like a
# protection and is not inside a site, a function-definition header (`refuse() {`) or a comment.
scan() {   # scan <file> ; prints "start<TAB>end<TAB>kind<TAB>lineno<TAB>text"
  python3 - "$1" <<'PY'
import re, sys
from pathlib import Path

src = Path(sys.argv[1]).read_text()

SITES = [
    # a refusal CALL: `refuse "..."` / `usage "..."`, wherever it sits on the line
    ("refusal",  re.compile(r'(?<![A-Za-z0-9_])(?:refuse|usage)[ \t]+(?:"(?:[^"\\]|\\.)*"|\'[^\']*\')')),
    # the dispatch that hands control to the real binary
    ("dispatch", re.compile(r'(?<![A-Za-z0-9_])exec[ \t]+"\$real"[ \t]+"\$@"')),
    # a signal turned into a refusal
    ("trap",     re.compile(r'(?<![A-Za-z0-9_])trap[ \t]+\'[^\']*on_signal[^\']*\'.*$', re.M)),
    # the non-zero status a refusal leaves behind
    ("status",   re.compile(r'(?<![A-Za-z0-9_])exit[ \t]+[1-9][0-9]*')),
    # the PATH element this shim was found through, made absolute for descendants. Not a refusal,
    # so the refusal patterns above never saw it, and a protection the inventory cannot see is a
    # protection outside the coverage claim this script prints.
    ("pathabs",  re.compile(r'PATH="\$abs_path"')),
    # pathname expansion off while the declarations are split
    ("globbing", re.compile(r'(?<![A-Za-z0-9_])set[ \t]+-f(?![A-Za-z0-9_])')),
    # hidden entries included in the directory listing
    ("hidden",   re.compile(r'(?<![A-Za-z0-9_])shopt[ \t]+-s[ \t]+nullglob[ \t]+dotglob')),
]
RESIDUE = re.compile(
    r'(?<![A-Za-z0-9_])(?:refuse|usage|on_signal)(?![A-Za-z0-9_])'
    r'|(?<![A-Za-z0-9_])exec[ \t]+"\$real'
    r'|(?<![A-Za-z0-9_])exit[ \t]+[1-9]'
    r'|(?<![A-Za-z0-9_])set[ \t]+-f(?![A-Za-z0-9_])'
    r'|(?<![A-Za-z0-9_])shopt[ \t]+-s')
# The one shape that carries a protection's name without being a call to it.
DEFN = re.compile(r'(?<![A-Za-z0-9_])(?:refuse|usage|on_signal)\(\)')

pos = 0
for lineno, line in enumerate(src.split("\n"), 1):
    at = pos
    pos += len(line) + 1
    if re.match(r'\s*#', line):
        continue
    found = []
    for kind, rx in SITES:
        for m in rx.finditer(line):
            found.append((m.start(), m.end(), kind))
    found.sort()
    sites = []
    for s, e, k in found:
        if sites and s < sites[-1][1]:
            continue            # already inside a larger site (a trap swallows its own on_signal)
        sites.append((s, e, k))
    for s, e, k in sites:
        print("%d\t%d\t%s\t%d\t%s" % (at + s, at + e, k, lineno, line.strip()))
    for m in RESIDUE.finditer(line):
        if any(s <= m.start() < e for s, e, _ in sites):
            continue
        if any(d.start() <= m.start() < d.end() for d in DEFN.finditer(line)):
            continue
        print("%d\t%d\t%s\t%d\t%s" % (at + m.start(), at + m.end(), "UNCLASSIFIED", lineno, line.strip()))
PY
}

neutralise() {  # neutralise <file> <start> <end> <kind>
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys
from pathlib import Path
f, s, e, kind = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
REPL = {
    "refusal":  "true",                 # the branch is still taken; it no longer refuses
    "status":   ":",                    # the refusal no longer leaves a non-zero status
    "trap":     ":",                    # the signal is no longer turned into a refusal
    "hidden":   ":",                    # the listing no longer includes hidden entries
    "globbing": "set +f",               # the declarations are pathname-expanded again
    "pathabs":  ":",                    # the PATH element is left as it was found, relative and all
    "dispatch": '"$real"; exit 0',      # the classic wrapper mistakes: lose the argument vector, and
                                        # wrap instead of exec
}
# A KIND WITH NO REPLACEMENT USED TO READ AS "UNCOVERED". The dict was indexed directly, so adding a
# SITE pattern without adding its replacement raised, the mutation never happened, the suite stayed
# green, and the sweep reported the protection as having no isolating case - blaming the test suite
# for a hole in this script. An unknown kind is now an ERROR that names itself.
if kind not in REPL:
    sys.stderr.write(f"effect-shim-sweep: no neutralisation defined for site kind {kind!r}; "
                     f"add one to REPL - a site this script cannot mutate says nothing about coverage\n")
    sys.exit(2)
repl = REPL[kind]
src = Path(f).read_text()
Path(f).write_text(src[:s] + repl + src[e:])
PY
}

# --- the baseline run: which cases exist, and which of them are declared non-discriminating --------
kinds="$work/kinds"; : > "$kinds"
KIND_LOG="$kinds" run_suite > "$work/base" 2>&1
if ! grep -q 'ALL PASS' "$work/base"; then
  echo "effect-shim-sweep: the suite is not green BEFORE any mutation; nothing below would mean anything" >&2
  sed -n '/^FAIL/,+4p' "$work/base" >&2
  exit 1
fi
all_cases="$(grep '^ok   \[' "$work/base" | sed 's/^ok   \[//; s/\]$//')"
positives="$work/positives"
grep '^kind POSITIVE' "$kinds" | sed 's/^kind POSITIVE[[:space:]]*//' > "$positives"

total=0; uncovered=0; unclassified=0
declare -a REDS

for file in "$SHIM" "$INTEG"; do
  base="${file##*/}"
  while IFS=$'\t' read -r s e kind n text; do
    [ -n "${s:-}" ] || continue
    short="$(printf '%s' "$text" | cut -c1-70)"
    if [ "$kind" = "UNCLASSIFIED" ]; then
      unclassified=$((unclassified+1))
      printf 'UNCLASSIFIED  %s:%s  looks like a protection and this sweep cannot classify it: %s\n' "$base" "$n" "$short"
      continue
    fi
    total=$((total+1))
    restore
    neutralise "$file" "$s" "$e" "$kind"
    redigest
    if ! bash -n "$file" 2>/dev/null; then
      uncovered=$((uncovered+1))
      printf 'UNMUTABLE     %s:%s (%s)  the mutant does not parse, so its reds prove nothing  %s\n' \
        "$base" "$n" "$kind" "$short"
      continue
    fi
    out="$(run_suite)"
    red="$(printf '%s' "$out" | grep '^FAIL' | sed 's/^FAIL \[//; s/\]$//')"
    # A POSITIVE control cannot fail because a protection was REMOVED. If one did, the mutant is broken
    # rather than neutralised and nothing that went red is attributable to this protection.
    broke=""
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      printf '%s\n' "$red" | grep -qxF -- "$p" && broke="$p"
    done < "$positives"
    if [ -n "$broke" ]; then
      uncovered=$((uncovered+1))
      printf 'NOT ATTRIBUTABLE  %s:%s (%s)  a POSITIVE control went red, so the mutant is broken, not neutralised: %s\n' \
        "$base" "$n" "$kind" "$(printf '%s' "$broke" | cut -c1-60)"
      continue
    fi
    if [ -z "$red" ]; then
      uncovered=$((uncovered+1))
      printf 'UNCOVERED     %s:%s (%s)  %s\n' "$base" "$n" "$kind" "$short"
    else
      $verbose && printf 'covered       %s:%s (%s) -> %s\n' "$base" "$n" "$kind" "$(printf '%s' "$red" | head -1 | cut -c1-70)"
      while IFS= read -r r; do REDS+=("$r"); done <<< "$red"
    fi
  done < <(scan "$file")
done
restore

# --- the inverse question: which shipped CASES never discriminate? --------------------------------
# A case that no removal turns red is passing for a reason other than the protection it names.
# Cases the suite itself declares cannot discriminate -- POSITIVE, STRUCTURAL and LIMIT -- are exempt,
# and the exemption is visible beside each case in the suite rather than kept here. Every OTHER case must
# go red for some removal; one that never does is passing for a reason other than the protection it names.
never=0
while IFS= read -r c; do
  [ -n "$c" ] || continue
  grep -qF -- "$c" "$kinds" && continue
  hit=false
  for r in ${REDS[@]+"${REDS[@]}"}; do [ "$r" = "$c" ] && { hit=true; break; }; done
  $hit || { printf 'NEVER RED  %s\n' "$c"; never=$((never+1)); }
done <<< "$all_cases"
printf 'effect-shim-sweep: %s case(s) declared non-discriminating (positive, structural or documented limit)\n' "$(grep -c '^kind ' "$kinds")"

# A case whose PASS label and FAIL label differ cannot be matched to its own red result, so the sweep
# would report it as never-red forever. That is how two cases hid here: the mismatch, not the coverage.
mismatch="$(comm -23 \
  <(grep -oE '\bbad "[^"]+"' "$SUITE" | sed 's/^bad "//; s/"$//' | sort -u) \
  <(grep -oE '\bok(_positive|_structural|_limit)? "[^"]+"' "$SUITE" | sed 's/^ok[a-z_]* "//; s/"$//' | sort -u))"
if [ -n "$mismatch" ]; then
  printf 'LABEL MISMATCH  %s\n' "$mismatch"
  never=$((never+1))
fi

# WHAT THIS COUNT IS NOT. The inventory is the set of protections the SITE patterns above can NAME:
# refusals, the dispatch, the signal traps, the refusal status, globbing and hidden entries, and the
# PATH element rewrite. It is not every line that matters. Two decisions are deliberately OUTSIDE it:
#
#   * the CANONICAL stripping that removes this directory before the real binary is resolved, and
#   * the empty-element handling in that same pass (an empty element is the current directory).
#
# Neutralising either produces an exec loop -- the lookup finds this script again and execs it -- or
# silently resolves a DIFFERENT binary, and a harness cannot tell a hung mutant from a protected one.
# They are covered BEHAVIOURALLY instead, by cases 11b, 11g and 11h in effect-shim-test.sh, which
# assert what a descendant and a real-binary lookup resolve to under a relative entry, an empty
# entry, and an empty entry that must survive. This script says so rather than counting them, because
# a coverage number that quietly excludes two decisions is the defect this script exists to find.
printf 'effect-shim-sweep: NOT INVENTORIED (covered behaviourally by cases 11b, 11g, 11h): the canonical PATH stripping and its empty-element handling\n'
printf 'effect-shim-sweep: %d inventoried protections, %d with an isolating case, %d without; %d unclassified; %d shipped cases never red\n' \
  "$total" "$((total-uncovered))" "$uncovered" "$unclassified" "$never"
[ "$uncovered" -eq 0 ] && [ "$never" -eq 0 ] && [ "$unclassified" -eq 0 ] \
  && echo "effect-shim-sweep: ALL INVENTORIED PROTECTIONS COVERED" || exit 1
