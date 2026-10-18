#!/usr/bin/env bash
# Mutation test: each arm of validate-clean-reset-registers-schemas.sh must refuse its own violation.
# One mutation at a time, always on a COPY. A mutation that changes nothing FAILS the arm rather than
# letting it pass on an unmodified file -- str.replace is silent when it matches nothing.
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD
GUARD=scripts/ci/validate-clean-reset-registers-schemas.sh
FILE=Jenkinsfile.es4-deploy
fails=0
pass() { echo "  ok   $1"; }
fail() { echo "  FAIL $1"; fails=$((fails + 1)); }

if (cd "$REPO" && bash "$GUARD" >/dev/null 2>&1); then pass "real tree passes"; else fail "real tree passes"; fi

run() { # $1=label $2=expect $3=python mutation body (q = a single quote)
  local label=$1 expect=$2 body=$3 work out status
  work=$(mktemp -d)
  (cd "$REPO" && git archive HEAD | tar -x -C "$work") 2>/dev/null
  mkdir -p "$work/scripts/ci"; cp "$REPO/$GUARD" "$work/$GUARD"; cp "$REPO/$FILE" "$work/$FILE"
  if ! ( cd "$work" && FILE="$FILE" python3 -c "
import os, re, sys
q = chr(39)
p = os.environ['FILE']
before = open(p).read()
s = before
$body
if s == before:
    print('mutation changed nothing', file=sys.stderr); sys.exit(1)
open(p, 'w').write(s)
" ); then fail "$label (mutation did not apply)"; rm -rf "$work"; return; fi
  out=$( cd "$work" && bash "$GUARD" 2>&1 ) && status=0 || status=$?
  rm -rf "$work"
  [ "$status" = 0 ] && { fail "$label (guard PASSED a violation)"; return; }
  case "$out" in *"$expect"*) pass "$label" ;; *) fail "$label (refused for another reason: $out)" ;; esac
}

run "dropping clean-reset from the when is refused" "does not run for ACTION=clean-reset" \
  's = s.replace(" || params.ACTION == " + q + "clean-reset" + q, "", 1)'

run "dropping the standalone action is refused" "no longer runs for ACTION=register-cvd-schemas" \
  's = s.replace("params.ACTION == " + q + "register-cvd-schemas" + q + " || ", "", 1)'

run "removing the stage entirely is refused" "no stage named" \
  's = s.replace("stage(" + q + "Register existing CVD schemas" + q + ")", "stage(" + q + "Register something else" + q + ")", 1)'

run "a stage with no when is refused" "has no when expression" '
lines = s.split("\n")
start = next(i for i, l in enumerate(lines) if "stage(" + q + "Register existing CVD schemas" + q + ")" in l)
end = next(i for i, l in enumerate(lines[start:], start) if re.search(r"steps\s*\{", l))
kept = [l for l in lines[start:end] if "when" not in l and "params.ACTION" not in l]
s = "\n".join(lines[:start] + kept + lines[end:])'

echo
[ "$fails" = 0 ] && { echo "all clean-reset schema-registration guard arms refuse their own violation"; exit 0; }
echo "$fails arm(s) did not hold"; exit 1
