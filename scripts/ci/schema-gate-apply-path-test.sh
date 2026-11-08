#!/usr/bin/env bash
# schema-gate-apply-path-test.sh — GATE must actually RUN schema-sync.sh, and NOT_GATED must not.
#
# WHY. Two rounds of review found the connection between the scope classifier and the gate asserted
# only TEXTUALLY: a test could pass while service-deploy.sh called the classifier and then skipped
# schema-sync.sh for GATE, with every asserted string surviving in a comment or in dead code
# (Codex deploy r15). Running the classifier proved the classifier; it proved nothing about what the
# deploy does with the answer.
#
# So the gate BLOCK is extracted from scripts/deploy/service-deploy.sh and executed against stubs:
# a stub classifier that prints whichever verdict the arm wants, and a stub schema-sync.sh that
# records being called. Nothing real is deployed and no registry is touched.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SRC="$ROOT/scripts/deploy/service-deploy.sh"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }

# The block, from where it names the pinned tool through the `fi` that closes the decision. It
# starts at SCHEMA_JAR and not at the classifier call because the jar and its PIN are set just above
# and the gate's own integrity check reads them — extracting from the call alone left them empty and
# the GATE arm failed on "is missing or does not match the sha256 pinned", which is the extraction
# being wrong rather than the gate.
awk '/^SCHEMA_JAR=/{f=1} f{print} f&&/^fi$/{exit}' "$SRC" > "$TMP/gate.sh"
if ! grep -q 'schema-sync.sh' "$TMP/gate.sh" || ! grep -q 'NOT_GATED_UNCOVERED' "$TMP/gate.sh"; then
  echo "FAIL: could not extract the schema gate block from $SRC — the parser and the script have diverged."
  echo "      (That is the failure mode this test exists to avoid, so it refuses rather than passing.)"
  exit 1
fi

run_gate() { # run_gate <verdict the classifier prints>
  mkdir -p "$TMP/bin" "$TMP/scripts/ci" "$TMP/tools/schema-governance"
  : > "$TMP/called"
  printf '#!/usr/bin/env bash\necho "%s"\n' "$1" > "$TMP/scripts/ci/schema-sync-scope.sh"
  printf '#!/usr/bin/env bash\necho CALLED >> "%s"\n' "$TMP/called" > "$TMP/scripts/ci/schema-sync.sh"
  chmod +x "$TMP/scripts/ci/"*.sh
  # a jar whose sha256 matches its PIN, so the gate's own integrity check passes
  echo jar > "$TMP/tools/schema-governance/schema-governance-tool.jar"
  echo "sha256=$(shasum -a 256 "$TMP/tools/schema-governance/schema-governance-tool.jar" | cut -d' ' -f1)" \
    > "$TMP/tools/schema-governance/PIN"
  # The gate reads the registry URL from the cluster's configmap and REFUSES to guess it, so the
  # stub has to answer that one query. Arm 5 below asserts the refusal when it does not.
  printf '#!/usr/bin/env bash\ncase "$*" in *SCHEMA_REGISTRY_URL*) echo "http://localhost:8081" ;; esac\nexit 0\n' \
    > "$TMP/bin/kubectl"; chmod +x "$TMP/bin/kubectl"
  ( cd "$TMP"
    PATH="$TMP/bin:$PATH" SERVICE=es-liquidity-engine ENVIRONMENT=dev PINNED_IMAGE=reg/img@sha256:dead \
      DEPLOY_PLATFORM=linux/arm64 DEPLOY_DRY_RUN=false bash "$TMP/gate.sh" 2>&1 )
}

# 1) GATE must invoke schema-sync.sh.
out="$(run_gate GATE)"
if grep -q CALLED "$TMP/called" 2>/dev/null; then
  ok "GATE runs schema-sync.sh"
else bad "GATE did NOT run schema-sync.sh:"; printf '%s\n' "$out" | tail -3; fi

# 2) NOT_GATED_UNCOVERED must not, and must say so out loud.
out="$(run_gate NOT_GATED_UNCOVERED)"
if ! grep -q CALLED "$TMP/called" 2>/dev/null && printf '%s' "$out" | grep -q "NOT GATED"; then
  ok "NOT_GATED_UNCOVERED skips it and says so"
else bad "the uncovered verdict did not behave:"; printf '%s\n' "$out" | tail -3; fi

# 3) A declared exemption must not run it either — the third verdict, so the branch is a decision.
out="$(run_gate "EXEMPT: bound in other environments only")"
if ! grep -q CALLED "$TMP/called" 2>/dev/null && printf '%s' "$out" | grep -q "NOT GATED"; then
  ok "an EXEMPT verdict skips it and says so"
else bad "the exempt verdict did not behave:"; printf '%s\n' "$out" | tail -3; fi

# 4) An UNREADABLE classifier must stop the deploy, not fall through to ungated.
mkdir -p "$TMP/scripts/ci"
printf '#!/usr/bin/env bash\nexit 3\n' > "$TMP/scripts/ci/schema-sync-scope.sh"
chmod +x "$TMP/scripts/ci/schema-sync-scope.sh"
: > "$TMP/called"
out="$( cd "$TMP" && PATH="$TMP/bin:$PATH" SERVICE=es-liquidity-engine ENVIRONMENT=dev \
        PINNED_IMAGE=reg/img@sha256:dead DEPLOY_PLATFORM=linux/arm64 DEPLOY_DRY_RUN=false \
        bash "$TMP/gate.sh" 2>&1 )"; rc=$?
if [ "$rc" -ne 0 ] && ! grep -q CALLED "$TMP/called" 2>/dev/null; then
  ok "a classifier that cannot decide refuses the deploy rather than proceeding ungated"
else bad "an undecidable scope did not refuse (rc=$rc)"; fi

# 5) AND IT MUST NOT GUESS THE REGISTRY. A gate that invented a registry URL would register the
#    manifest's schemas somewhere nobody reads and report success.
mkdir -p "$TMP/bin"
printf '#!/usr/bin/env bash\nexit 0\n' > "$TMP/bin/kubectl"; chmod +x "$TMP/bin/kubectl"
printf '#!/usr/bin/env bash\necho GATE\n' > "$TMP/scripts/ci/schema-sync-scope.sh"
chmod +x "$TMP/scripts/ci/schema-sync-scope.sh"
: > "$TMP/called"
out="$( cd "$TMP" && PATH="$TMP/bin:$PATH" SERVICE=es-liquidity-engine ENVIRONMENT=dev \
        PINNED_IMAGE=reg/img@sha256:dead DEPLOY_PLATFORM=linux/arm64 DEPLOY_DRY_RUN=false \
        bash "$TMP/gate.sh" 2>&1 )"; rc=$?
if [ "$rc" -ne 0 ] && ! grep -q CALLED "$TMP/called" 2>/dev/null \
   && printf '%s' "$out" | grep -q "refusing to guess the registry"; then
  ok "an unreadable registry URL refuses the gate rather than guessing one"
else bad "the gate proceeded without a registry (rc=$rc)"; fi

printf '\nschema-gate-apply-path-test: %s (%d passed, %d failed)\n' \
  "$([ "$fail" -eq 0 ] && echo OK || echo FAILED)" "$pass" "$fail"
[ "$fail" -eq 0 ]
