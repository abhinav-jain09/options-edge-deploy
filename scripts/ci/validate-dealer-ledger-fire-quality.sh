#!/usr/bin/env bash
# DEPENDENCY-SCOPED FIRE-INPUT QUALITY knobs (processing PR #594, b704ba42) — render guard.
#
# WHY THIS EXISTS. The two knobs below decide whether a G-FIRE emission is gated on the CHAIN-WIDE
# greeks aggregate (legacy) or on a verdict scoped to the quantities G-FIRE actually consumes
# (proposed), and — when the proposed verdict is live — how much off-span positioning may be dropped
# before the verdict blocks. They were merged with NO deploy-side registration at all, which is
# exactly the failure this guard prevents from recurring: a knob nobody can grep for is a knob nobody
# reviews. Both are registered in k8s/base/dealer-ledger-deployment.yaml.
#
# FOUR ENVIRONMENTS, TWO SOURCES. dev / production / experiment are asserted on the KUSTOMIZE RENDER
# of k8s/overlays/<env>. es4 is asserted on the COMMITTED k8s/es4/services/dealer-ledger.yaml,
# because Jenkinsfile.es4-deploy applies that file directly and never renders an overlay.
#
# WHAT IT ASSERTS, per env (Codex 2026-08-13, question (d)):
#   1. Both variables are present EXACTLY ONCE in the rendered dealer-ledger container. In practice
#      this guards ABSENCE (count 0): measured against this repo's renders, kustomize collapses two
#      env entries of the same name inside one container down to ONE, keeping the LAST. So a
#      count of 2 is not reachable through a kustomize overlay, and the count is asserted anyway
#      because the render path is not the only way a manifest reaches a cluster. The "which one won"
#      question — the env/19 index-patch class of defect recorded in the production overlay — is
#      caught by the VALUE assertions below, not by the count.
#   2. DEALER_LEDGER_FIRE_QUALITY_DEPENDENCY_SCOPED parses as a boolean, and is `false` unless this
#      script is told otherwise. The allowed-true set is declared HERE, in one place, so turning the
#      flag on in an env is a deliberate two-file change (overlay + this list) that a reviewer sees.
#   3. DEALER_LEDGER_FIRE_OFFSPAN_MATERIALITY_FRAC parses as a FINITE, NON-NEGATIVE decimal, in TWO
#      steps: a lexical check on the string, THEN an actual IEEE-754 double parse. The second step is
#      not redundant — a plain decimal of 400 nines is lexically fine and still overflows to
#      +Infinity. Junk is rejected here rather than at pod start: the service's own floor is 0.0, so
#      it would not crash-loop — it would silently land in ProfileEngine.offSpanDropIsImmaterial,
#      where a non-positive OR non-finite tolerance means BLOCK EVERY off-span drop.
#
# POSITIVE CONTROLS (run 2026-08-13 by mutating the base manifest and the es4 manifest, each restored
# afterwards): flag flipped to true -> FAIL; flag set to "TRUE" -> FAIL (not a boolean); flag entry
# deleted -> FAIL (count 0); frac "abc" / "-0.5" / "1.2.3" / "1e400" -> FAIL lexically; frac set to
# 400 nines -> passes the regex and FAILS the double parse; container name mutated so the deployment
# does not match -> FAIL in all four envs. The clean tree passes dev, production, experiment and es4.
#
# It does NOT assert a particular fraction. Changing 0.01 is a legitimate calibration decision; it
# just has to arrive as an explicit deploy PR with the per-env dollar tolerance recomputed
# (tolerance = FRAC * DEALER_LEDGER_UNWIND_ARM_MIN, which differs between dev and production).
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v kubectl >/dev/null 2>&1 || { echo "FATAL: kubectl is required" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
# python3 is already a deploy dependency (scripts/es4/render_es4_manifests.py; the intent-dedup
# heredoc in Jenkinsfile.service-deploy). Used ONLY to reproduce IEEE-754 double parsing.
command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 is required" >&2; exit 1; }

CONTAINER="dealer-ledger"
DEPLOYMENT="dealer-ledger-service"
FLAG="DEALER_LEDGER_FIRE_QUALITY_DEPENDENCY_SCOPED"
FRAC="DEALER_LEDGER_FIRE_OFFSPAN_MATERIALITY_FRAC"

# Envs in which the dependency-scoped verdict is allowed to be TRUE. EMPTY until the shadow evidence
# clears the rollout gate: N>=500 legacy-pass G-FIRE rows over >=5 RTH sessions / >=3 trading dates,
# proposed-blocks-legacy-pass <= 1.0% (Wilson 95% upper bound <= 2.0%), and BOTH
# counterfactual-fire|CONTRIBUTOR-DEFECT and FIRE_INPUT_INVARIANT_VIOLATIONS exactly 0.
# That evidence cannot be produced yet: the counters have no readout path from a running pod.
ALLOW_TRUE_ENVS=""

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
failed=0

# Assert the two knobs on ONE already-extracted dealer-ledger container document.
# $1 = label used in messages, $2 = path to the container YAML.
check_container() {
  local label="$1" doc="$2" var count flag_value frac_value
  # Emptiness is asserted explicitly, NOT via `yq -e`: over a multi-document stream `-e` reflects the
  # LAST document evaluated, so a render that dropped the deployment entirely would still exit 0 and
  # this guard would then silently "pass" an env it never inspected.
  if ! grep -q "^name: $CONTAINER\$" "$doc"; then
    echo "FAIL[$label]: deployment $DEPLOYMENT container $CONTAINER does not render" >&2
    failed=1
    return
  fi

  for var in "$FLAG" "$FRAC"; do
    count="$(yq -r "[.env[] | select(.name == \"$var\")] | length" "$doc")"
    if [ "$count" != "1" ]; then
      echo "FAIL[$label]: $var appears $count times in the rendered $CONTAINER container (want exactly 1)" >&2
      failed=1
    fi
  done

  flag_value="$(yq -r "[.env[] | select(.name == \"$FLAG\") | .value] | .[-1] // \"\"" "$doc")"
  case "$flag_value" in
    true|false) ;;
    *)
      echo "FAIL[$label]: $FLAG must be the string \"true\" or \"false\", got '${flag_value:-unset}'" >&2
      failed=1
      ;;
  esac
  if [ "$flag_value" = "true" ] && ! printf ' %s ' "$ALLOW_TRUE_ENVS" | grep -q " $label "; then
    echo "FAIL[$label]: $FLAG is true, but $label is not in ALLOW_TRUE_ENVS." >&2
    echo "       Enabling the dependency-scoped verdict changes PRODUCTION SIGNAL LOGIC and is gated" >&2
    echo "       on shadow evidence (see this script's header). Add the env here in the SAME PR that" >&2
    echo "       flips it, and cite the counter readout in the PR body." >&2
    failed=1
  fi

  frac_value="$(yq -r "[.env[] | select(.name == \"$FRAC\") | .value] | .[-1] // \"\"" "$doc")"
  # STEP 1 — lexical. Asserted on the STRING before any arithmetic, so "nan", "inf", "-0.5",
  # "1e400" and "1.2.3" are rejected as text rather than silently becoming a float.
  case "$frac_value" in
    ''|*[!0-9.]*)
      echo "FAIL[$label]: $FRAC must be a finite non-negative decimal, got '${frac_value:-unset}'" >&2
      failed=1
      return
      ;;
  esac
  # Reject "1.2.3" and a bare "." — one optional dot, at least one digit.
  if ! printf '%s' "$frac_value" | grep -Eq '^([0-9]+(\.[0-9]*)?|\.[0-9]+)$'; then
    echo "FAIL[$label]: $FRAC is not a well-formed decimal: '$frac_value'" >&2
    failed=1
    return
  fi
  # STEP 2 — NUMERIC. Lexical validity is NOT finiteness: a plain decimal of 400 nines matches the
  # regex above and still parses to +Infinity as an IEEE-754 double, which is exactly the value
  # ProfileEngine.offSpanDropIsImmaterial treats as "no bound can be established" -> BLOCK. Python
  # floats are IEEE-754 doubles, the same representation Java's Double.parseDouble produces, so this
  # reproduces the service's own parse rather than approximating it.
  if ! FRAC_VALUE="$frac_value" python3 -c '
import math, os, sys
raw = os.environ["FRAC_VALUE"]
try:
    v = float(raw)
except ValueError:
    sys.exit(1)
sys.exit(0 if math.isfinite(v) and v >= 0.0 else 1)
'; then
    echo "FAIL[$label]: $FRAC='$frac_value' does not parse as a FINITE non-negative double" >&2
    echo "       (a lexically valid decimal can still overflow to +Infinity; ProfileEngine reads a" >&2
    echo "        non-finite tolerance as 'no bound' and BLOCKS every off-span drop)" >&2
    failed=1
    return
  fi
  echo "ok[$label]: $FLAG=$flag_value $FRAC=$frac_value"
}

# --- the three kustomize-rendered envs -------------------------------------------------------
for env_name in dev production experiment; do
  overlay="k8s/overlays/$env_name"
  [ -d "$overlay" ] || { echo "FATAL: missing overlay $overlay" >&2; exit 1; }
  kubectl kustomize "$overlay" >"$tmp/$env_name.yaml"
  # The whole env array of THIS container in THIS deployment. Rendering the container (not grepping
  # the file) is what makes "exactly once" meaningful: a grep would also match the calibration
  # sidecars, the generated slices, or a commented example.
  yq "select(.kind == \"Deployment\" and .metadata.name == \"$DEPLOYMENT\")
      | .spec.template.spec.containers[] | select(.name == \"$CONTAINER\")" \
     "$tmp/$env_name.yaml" >"$tmp/$env_name.container"
  check_container "$env_name" "$tmp/$env_name.container"
done

# --- es4 --------------------------------------------------------------------------------------
# The FOURTH environment. es4 is NOT a kustomize overlay: Jenkinsfile.es4-deploy applies the
# committed k8s/es4/services/*.yaml directly (ACTION=deploy-service SERVICE=dealer-ledger, or
# deploy-all). Those files are generated by scripts/es4/render_es4_manifests.py FROM the production
# slice, so they inherit this registration — but "inherits by generator" is not an assertion, and
# the es4 job never renders an overlay this guard could have caught. Read the committed file.
ES4_MANIFEST="k8s/es4/services/dealer-ledger.yaml"
if [ ! -f "$ES4_MANIFEST" ]; then
  echo "FATAL: missing $ES4_MANIFEST" >&2
  exit 1
fi
yq "select(.kind == \"Deployment\" and .metadata.name == \"$DEPLOYMENT\")
    | .spec.template.spec.containers[] | select(.name == \"$CONTAINER\")" \
   "$ES4_MANIFEST" >"$tmp/es4.container"
check_container "es4" "$tmp/es4.container"

if [ "$failed" -ne 0 ]; then
  exit 1
fi
echo "dealer-ledger FIRE-quality knob registration invariants passed"
