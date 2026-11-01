#!/usr/bin/env bash
# Mutation test: validate-realm-configmaps.sh must refuse its own violations, and must not refuse the
# real tree. The guard itself has existed since the SECOND Keycloak import outage, and it does catch the
# defect — but nothing ran it, so on 2026-10-25 prod shipped a realm JSON carrying a "//" key for the
# THIRD time and oe-keycloak crash-looped 32 times until the ConfigMap was corrected by hand. A guard
# that no pipeline executes is not a guard, so this test exists to be WIRED (see
# .github/workflows/deploy-validation.yml and Jenkinsfile.common-infra) alongside the guard.
#
# One mutation at a time, each in a throwaway copy of the repo, so a passing arm cannot be masked by
# another arm also firing.
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD
GUARD=scripts/keycloak/validate-realm-configmaps.sh

fails=0
pass() { echo "  ok   $1"; }
fail() { echo "  FAIL $1"; fails=$((fails + 1)); }

# The real tree must pass, or every "refused" below proves nothing.
if (cd "$REPO" && bash "$GUARD" >/dev/null 2>&1); then pass "real tree passes"; else fail "real tree passes"; fi

# The guard walks every realm JSON in every keycloak configmap, so the mutation has to land in a real
# one rather than a fixture; which file that is must not be hard-coded here (configmaps get added).
# ⚠️Match ONLY the embedded-JSON marker: the guard's own file list also matches '--import-realm', which
# selects keycloak-deployment.yaml — a file with no realm JSON, where every mutation silently no-ops and
# the guard then "passes a violation" that was never planted.
TARGET=$(cd "$REPO" && grep -rl --include='*.yaml' -- 'realm.json: |' k8s | sort -u | head -1)
[ -n "$TARGET" ] || { echo "FAIL: no keycloak realm configmap found to mutate"; exit 1; }
grep -q 'realm.json: |' "$REPO/$TARGET" || { echo "FAIL: $TARGET carries no embedded realm JSON"; exit 1; }

run_mutation() { # $1=label $2=expect $3=sed-style python mutation over $TARGET
  local label=$1 expect=$2 mutation=$3 work out status
  work=$(mktemp -d)
  # A copy, never the real tree: a mutation left behind would be committed.
  (cd "$REPO" && git archive HEAD | tar -x -C "$work") 2>/dev/null
  # Uncommitted work in progress has to be carried over too, or the mutation lands on the old file.
  for f in "$GUARD" "$TARGET"; do
    mkdir -p "$work/$(dirname "$f")"; cp "$REPO/$f" "$work/$f"
  done
  ( cd "$work" && TARGET="$TARGET" python3 -c "$mutation" ) || { fail "$label (mutation did not apply)"; rm -rf "$work"; return; }
  out=$( cd "$work" && bash "$GUARD" 2>&1 ) && status=0 || status=$?
  rm -rf "$work"
  if [ "$status" = 0 ]; then fail "$label (guard PASSED a violation)"; return; fi
  case "$out" in *"$expect"*) pass "$label" ;; *) fail "$label (refused, but not for the stated reason: $out)" ;; esac
}

# The exact shape that broke prod: a "//" key inside the realm's client list. It is VALID JSON, which is
# why a parse-only check sails past it — Keycloak rejects it semantically ("Unrecognized field").
run_mutation 'a "//" commentary key is refused' 'illegal commentary key "//"' '
import os, re
p = os.environ["TARGET"]
s = open(p).read()
# Insert the key into the first object inside the embedded JSON block, at the block indent (4 spaces).
new, n = re.subn(r"(\n    \{\n)", "\\1      \"//\": \"prose belongs in a YAML comment\",\n", s, count=1)
assert n == 1 and new != s, "mutation did not apply"
open(p, "w").write(new)'

run_mutation 'a "comment" key is refused' 'illegal commentary key' '
import os, re
p = os.environ["TARGET"]
s = open(p).read()
new, n = re.subn(r"(\n    \{\n)", "\\1      \"comment\": \"prose belongs in a YAML comment\",\n", s, count=1)
assert n == 1 and new != s, "mutation did not apply"
open(p, "w").write(new)'

run_mutation 'JSON that does not parse is refused' 'JSON does not parse' '
import os, re
p = os.environ["TARGET"]
s = open(p).read()
new, n = re.subn(r"(\n    \{\n)", "\\1      \"realm\": ,\n", s, count=1)
assert n == 1 and new != s, "mutation did not apply"
open(p, "w").write(new)'

[ "$fails" = 0 ] || { echo "$fails arm(s) failed"; exit 1; }
echo "all arms refused their own violation"
