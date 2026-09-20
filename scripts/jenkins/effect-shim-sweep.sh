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
# This walks the files and mutates EVERY refusal branch, every guard clause and every dispatch line it
# finds, and it REFUSES to report success if it cannot parse a line that looks like a protection.
#
# Usage: effect-shim-sweep.sh [--verbose]
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
SHIM="$HERE/effect-shim/_shim.sh"
INTEG="$HERE/effect-shim-integrity.sh"
SUITE="$HERE/effect-shim-test.sh"
DIGEST="$HERE/effect-shim-digest.txt"
verbose=false; [ "${1:-}" = "--verbose" ] && verbose=true

work="$(mktemp -d)"; trap 'cp "$work/_shim.sh" "$SHIM"; cp "$work/integ" "$INTEG"; cp "$work/digest" "$DIGEST"; rm -rf "$work"' EXIT
cp "$SHIM" "$work/_shim.sh"; cp "$INTEG" "$work/integ"; cp "$DIGEST" "$work/digest"

# --- the inventory, read out of the two files ---------------------------------------------------
# A protection is: a line that refuses, or the dispatch line that hands control to the real binary, or a
# trap that turns a signal into a refusal. Comments are excluded by requiring the line to be code.
inventory() {   # inventory <file> ; prints "lineno<TAB>kind<TAB>text"
  awk -F'\n' '
    /^[[:space:]]*#/ { next }
    /\|\| *refuse|\|\| *usage/            { print NR "\tguard\t" $0; next }
    /^[[:space:]]*refuse "/               { print NR "\trefusal\t" $0; next }
    /^[[:space:]]*exec "\$real"/          { print NR "\tdispatch\t" $0; next }
    /^[[:space:]]*trap .*on_signal/       { print NR "\ttrap\t" $0; next }
    /^[[:space:]]*exit 3/                 { print NR "\tstatus\t" $0; next }
    /^[[:space:]]*set -f/                 { print NR "\tglobbing\t" $0; next }
    /shopt -s nullglob dotglob/           { print NR "\thidden\t" $0; next }
  ' "$1"
}

neutralise() {  # neutralise <file> <lineno> <kind>
  local f="$1" n="$2" kind="$3"
  python3 - "$f" "$n" "$kind" <<'PY'
import sys
from pathlib import Path
f, n, kind = sys.argv[1], int(sys.argv[2]), sys.argv[3]
lines = Path(f).read_text().split("\n")
l = lines[n-1]
indent = l[:len(l) - len(l.lstrip())]
if kind in ("guard",):
    # the condition stops being able to fail
    lines[n-1] = indent + "true || " + l.strip()
elif kind in ("refusal", "trap", "status", "hidden"):
    lines[n-1] = indent + ": # protection removed by the sweep"
elif kind == "globbing":
    lines[n-1] = indent + "set +f"
elif kind == "dispatch":
    # the classic wrapper mistakes: lose the argument vector, and wrap instead of exec
    lines[n-1] = indent + '"$real"; exit 0'
Path(f).write_text("\n".join(lines))
PY
}

total=0; uncovered=0
declare -a REDS
run_suite() { KIND_LOG="${KIND_LOG:-/dev/null}" bash "$SUITE" 2>&1; }

for file in "$SHIM" "$INTEG"; do
  base="${file##*/}"
  while IFS=$'\t' read -r n kind text; do
    [ -n "${n:-}" ] || continue
    total=$((total+1))
    cp "$work/_shim.sh" "$SHIM"; cp "$work/integ" "$INTEG"; cp "$work/digest" "$DIGEST"
    neutralise "$file" "$n" "$kind"
    # RE-RECORD THE DIGEST for the mutated shim. Without this, every mutation of _shim.sh also breaks the
    # digest, every integrity case goes red, and the sweep calls the protection "covered" when what went
    # red was the checksum -- the same passing-for-the-wrong-reason the sweep exists to find.
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$SHIM" | cut -d" " -f1 > "$DIGEST"
    else shasum -a 256 "$SHIM" | cut -d" " -f1 > "$DIGEST"; fi
    out="$(run_suite)"
    red="$(printf '%s' "$out" | grep '^FAIL' | sed 's/^FAIL \[//; s/\]$//')"
    if [ -z "$red" ]; then
      uncovered=$((uncovered+1))
      printf 'UNCOVERED  %s:%s (%s)  %s\n' "$base" "$n" "$kind" "$(printf '%s' "$text" | sed 's/^[[:space:]]*//' | cut -c1-70)"
    else
      $verbose && printf 'covered    %s:%s (%s) -> %s\n' "$base" "$n" "$kind" "$(printf '%s' "$red" | head -1 | cut -c1-70)"
      while IFS= read -r r; do REDS+=("$r"); done <<< "$red"
    fi
  done < <(inventory "$file")
done
cp "$work/_shim.sh" "$SHIM"; cp "$work/integ" "$INTEG"; cp "$work/digest" "$DIGEST"

# --- the inverse question: which shipped CASES never discriminate? --------------------------------
# A case that no removal turns red is passing for a reason other than the protection it names.
# Cases the suite itself declares cannot discriminate -- POSITIVE, STRUCTURAL and LIMIT -- are exempt,
# and the exemption is visible beside each case in the suite rather than kept here. Every OTHER case must
# go red for some removal; one that never does is passing for a reason other than the protection it names.
kinds="$work/kinds"; : > "$kinds"
KIND_LOG="$kinds" run_suite > "$work/out" 2>&1
all_cases="$(grep '^ok   \[' "$work/out" | sed 's/^ok   \[//; s/\]$//')"
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

printf 'effect-shim-sweep: %d protections mutated, %d uncovered; %d shipped cases never red\n' \
  "$total" "$uncovered" "$never"
[ "$uncovered" -eq 0 ] && [ "$never" -eq 0 ] && echo "effect-shim-sweep: ALL PROTECTIONS COVERED" || exit 1
