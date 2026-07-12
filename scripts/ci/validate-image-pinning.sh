#!/usr/bin/env bash
# IMAGE-PINNING COMPLETENESS (deploy §13.10 — prevention).
#
# Guards against: a new service's Deployment added to the manifests but NOT wired into the
# deploy's image resolution, so its image renders as a bare tag and apply.sh's fail-closed
# digest gate aborts the ENTIRE options-edge-deploy. (What es-open-direction / strike-invasion
# / spread-skew + their postgres-writers did.)
#
# For EVERY options-edge Deployment/initContainer image the overlays render, this BEHAVIORALLY
# proves resolution emits the exact ref, in every code path a real deploy uses:
#   (A1) branch 1  — dev (IMAGE_TAG=dev) and tag-based prod (IMAGE_TAG=prod): resolve-images.sh
#        must emit exactly <registry>/<image>:<tag> for the image's <NAME>_IMAGE var.
#   (A2) branch 2  — the PROMOTED prod path (ENVIRONMENT=production, IMAGE_TAG unset, image vars
#        seeded from the Jenkinsfile env block): resolve-images.sh must echo each seeded var back
#        (so a var missing from the branch-2 heredoc — which would abort a real promotion under
#        `set -u` — fails here instead).
#   (B)  the var must also be in apply.sh (pin loop) + image-lock.sh (all_image_vars) +
#        image-preflight.sh, so the digest-pin + preflight gates cover it.
# A token in a comment or the dev-only section does NOT satisfy any of these.
#
# VAR convention: options-edge-<name> -> <NAME upper, - to _>_IMAGE. Experiment is out of scope
# (it uses Jenkinsfile.experiment-deploy, not this resolver/pinning flow).
# Requires: kubectl (kustomize), yq. Hermetic: resolve-images.sh with an empty lock file +
# REQUIRE_IMAGE_LOCK=false does no registry/network/cluster I/O (branch 1 builds strings only).
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

APPLY=scripts/deploy/apply.sh
LOCK=scripts/deploy/image-lock.sh
PREFLIGHT=scripts/deploy/image-preflight.sh
JF=Jenkinsfile
for _f in scripts/deploy/resolve-images.sh "$APPLY" "$LOCK" "$PREFLIGHT"; do
  [ -f "$_f" ] || { echo "FATAL: $_f not found" >&2; exit 1; }
done

REG=probe.registry.invalid
fail=0
PROBE="$(mktemp -d)"; trap 'rm -rf "$PROBE"' EXIT

img_to_var() { printf '%s_IMAGE' "$(printf '%s' "${1#options-edge-}" | tr 'a-z-' 'A-Z_')"; }
rendered_images() {  # $1=env -> unique options-edge image basenames (registry prefix optional)
  kubectl kustomize "k8s/overlays/$1" 2>/dev/null \
    | yq -r 'select(.kind=="Deployment") | (.spec.template.spec.containers[].image), (.spec.template.spec.initContainers[]?.image)' \
    | { grep -E '(^|/)options-edge-' || true; } \
    | sed -E 's#^.*/##; s#(options-edge-[^:@]+).*#\1#' | sort -u
}
# prints the value ONLY if the var is assigned EXACTLY once (a duplicate would let sourcing
# pick the wrong last value); 0 or >1 assignments -> empty -> caller reports a mismatch.
emitted() {
  local n; n="$(grep -c "^$1=" "$2" 2>/dev/null || true)"
  [ "${n:-0}" = "1" ] || return 0
  grep "^$1=" "$2" | cut -d= -f2-
}
# Structural extraction of the EXACT lists the deploy iterates — so a var in a comment, a
# DEPLOY_TARGET block, or anywhere outside the real list does NOT satisfy the check.
#   apply.sh: the vars named in the `for _img_var in ... ; do` pin loops (main + dev-only).
# $2: 'main' = only the FIRST (all-env) pin loop; 'all' = main + dev-only loops.
_apply_pin_vars() {
  awk -v mode="${1:-all}" '
       /^[[:space:]]*for _img_var in/{seen++; f=(mode=="main")?(seen==1):1}
       f{for(i=1;i<=NF;i++){t=$i; sub(/;$/,"",t); if(t ~ /^[A-Z][A-Z0-9_]*_IMAGE$/) print t}}
       f && /do[[:space:]]*$/{f=0}' "$APPLY"
}
#   image-lock.sh: the vars listed inside all_image_vars() (its heredocs).
# $1: 'main' = only vars before the dev-only `if ENVIRONMENT=dev` guard; 'all' = whole function.
_lock_vars() {
  awk -v mode="${1:-all}" '
    /all_image_vars\(\)/{f=1}
    f && mode=="main" && /if \[.*ENVIRONMENT.*=.*"?dev"?.*\]/{f=0}
    f && /^[A-Z][A-Z0-9_]*_IMAGE$/{print}
    /^}/{f=0}' "$LOCK"
}
#   image-preflight.sh: the vars in the MAIN images list (before the DEPLOY_TARGET case).
_preflight_vars() {
  sed '/case .*DEPLOY_TARGET/q' "$PREFLIGHT" | grep -oE '[A-Z][A-Z0-9_]*_IMAGE=' | tr -d '='
}
APPLY_MAIN_VARS="$(_apply_pin_vars main)"; APPLY_ALL_VARS="$(_apply_pin_vars all)"
LOCK_MAIN_VARS="$(_lock_vars main)"; LOCK_ALL_VARS="$(_lock_vars all)"; PREFLIGHT_VARS="$(_preflight_vars)"
in_list() { printf '%s\n' "$2" | grep -qx "$1"; }
# fixed-string search of the Jenkinsfile with Groovy line-comments stripped (a commented-out
# statement must not satisfy a wiring check). plain grep (not -q) to avoid pipefail SIGPIPE.
jf_has() { grep -v '^[[:space:]]*//' "$JF" | grep -F "$1" >/dev/null; }

# Run branch-1 resolution for BOTH envs up front and cache the env files. Dev-only status is
# then derived from resolve-images.sh's OWN behavior — a var it emits for dev but NOT for prod
# is dev-only by definition. This needs no services.yaml/yq/kustomize parsing, so it is immune
# to CI kustomize/yq version differences (a dev-only service wrongly rendered into the prod
# overlay by some kustomize version is still correctly treated as dev-only).
declare EF_dev EF_production
for e in dev production; do
  tag=$([ "$e" = dev ] && echo dev || echo prod); wd="$PROBE/b1-$e"; mkdir -p "$wd"
  if ! ( set -euo pipefail
         export ENVIRONMENT="$e" IMAGE_TAG="$tag" OE_REGISTRY="$REG" \
                JENKINS_WORK_DIR="$wd" REQUIRE_IMAGE_LOCK=false IMAGE_LOCK_FILE=""
         bash scripts/deploy/resolve-images.sh >/dev/null 2>&1 ); then
    echo "FAIL: resolve-images.sh failed to run (branch 1) for ENVIRONMENT=$e" >&2; fail=1
  fi
  eval "EF_$e=\"$wd/options-edge-images.env\""
done
# var emitted for dev but not for prod == dev-only
is_dev_only() { grep -q "^$1=" "$EF_dev" 2>/dev/null && ! grep -q "^$1=" "$EF_production" 2>/dev/null; }

# --- (A1) branch-1 resolution: dev + tag-based prod, exact ref per rendered image -----------
for pair in dev:dev production:prod; do
  e="${pair%%:*}"; tag="${pair#*:}"; eval "ef=\"\$EF_$e\""; n=0
  for img in $(rendered_images "$e"); do
    var="$(img_to_var "$img")"
    # dev-only services never render in production; some CI kustomize versions wrongly emit them
    # there — skip them in any non-dev probe (dev-only derived from resolve-images emission).
    [ "$e" != "dev" ] && is_dev_only "$var" && continue
    n=$((n+1)); want="$REG/$img:$tag"; got="$(emitted "$var" "$ef")"
    if [ "$got" != "$want" ]; then
      echo "FAIL[$e/branch1]: resolve-images.sh emitted '${got:-<none>}' for $var (expected '$want') — image '$img' is not correctly wired." >&2
      fail=1
    fi
    miss=""
    # A prod service MUST be in the main (all-env) pin loop / all_image_vars — production never
    # runs the dev-only loop. A dev-only service may be in either (dev-only sections included).
    if is_dev_only "$var"; then
      in_list "$var" "$APPLY_ALL_VARS" || miss="$miss apply.sh(pin-loop)"
      in_list "$var" "$LOCK_ALL_VARS"  || miss="$miss image-lock.sh(all_image_vars)"
    else
      in_list "$var" "$APPLY_MAIN_VARS" || miss="$miss apply.sh(main-pin-loop)"
      in_list "$var" "$LOCK_MAIN_VARS"  || miss="$miss image-lock.sh(main-all_image_vars)"
      # Image Preflight verifies prod image existence before promotion — prod services only.
      in_list "$var" "$PREFLIGHT_VARS"  || miss="$miss image-preflight.sh(main-list)"
    fi
    [ -z "$miss" ] || { echo "FAIL[$e]: image '$img' (var $var) not in the pin/preflight LIST of:$miss" >&2; fail=1; }
  done
  echo "ok: $e branch-1 ($n images resolved + pinned + preflighted)"
done

# --- (A2) branch-2 resolution: the PROMOTED prod path (IMAGE_TAG unset, vars seeded) --------
wd="$PROBE/b2-prod"; mkdir -p "$wd"; ef="$wd/options-edge-images.env"
# Seed EVERY *_IMAGE var referenced anywhere in resolve-images.sh (a superset of the branch-2
# heredoc), so `set -u` does not abort on a var whose service is not among the rendered images
# (e.g. a retired service still listed). Emission of each RENDERED image's var is checked below.
_seed() { for v in $(grep -oE '[A-Z][A-Z0-9_]*_IMAGE' scripts/deploy/resolve-images.sh | sort -u); do printf 'export %s=%s\n' "$v" "SENTINEL:$v"; done; }
if ! ( set -euo pipefail
       eval "$(_seed)"
       export ENVIRONMENT=production OE_REGISTRY="$REG" JENKINS_WORK_DIR="$wd" \
              REQUIRE_IMAGE_LOCK=false IMAGE_LOCK_FILE=""
       unset IMAGE_TAG
       bash scripts/deploy/resolve-images.sh >/dev/null 2>&1 ); then
  echo "FAIL[production/branch2]: resolve-images.sh aborted on the promoted (IMAGE_TAG-unset) prod path — a branch-2 heredoc var is unbound (missing var, or one not backed by a rendered image)." >&2
  fail=1
else
  for img in $(rendered_images production); do
    var="$(img_to_var "$img")"; is_dev_only "$var" && continue
    want="SENTINEL:$var"; got="$(emitted "$var" "$ef")"
    if [ "$got" != "$want" ]; then
      echo "FAIL[production/branch2]: promoted-path resolve did NOT echo $var (expected '$want' got '${got:-<none>}') — add it to resolve-images.sh's branch-2 heredoc AND the deploy Jenkinsfile (param+env+promotion)." >&2
      fail=1
    fi
    # The promoted prod path relies on the Jenkins env-default (params.VAR ?: oeProfile.image(...))
    # supplying the ref for branch 2. Require all THREE Jenkins wiring points or a real promotion
    # receives the var unset (aborting resolve-images under set -u).
    # The env-default (params.VAR ?: oeProfile.image(...)) is what actually supplies the ref on
    # the promoted (unset-param) path, so it is required for every prod-rendered service; the
    # string param must also exist to make params.VAR valid. (The promotion-forward line is NOT
    # required — databento-feed forwards via the feed build path, and the env-default already
    # provides the value.)
    if [ -f "$JF" ]; then
      jmiss=""; svc="${img#options-edge-}"
      jf_has "name: '${var}', defaultValue"                            || jmiss="$jmiss param"
      jf_has "${var} = \"\${params.${var} ?: oeProfile.image('${svc}'" || jmiss="$jmiss env-default(->${svc})"
      [ -z "$jmiss" ] || { echo "FAIL[production/jenkins]: prod-rendered image '$img' (var $var) missing from active Jenkinsfile wiring:$jmiss" >&2; fail=1; }
    fi
  done
  echo "ok: production branch-2 (promoted path, seeded exact-match + Jenkins wiring)"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== validate-image-pinning: FAILED ===" >&2
  exit 1
fi
echo "=== validate-image-pinning: OK ==="
