#!/usr/bin/env bash
# EXACTLY-ONE PRE-OPEN GEX PUBLISHER ASSERTION (incident 2026-08-24).
#
# Two publishers can own the pre-open (before-09:30) gamma surface and they are mutually
# exclusive by construction:
#
#   IBKR       IbkrPreOpenService, built only when IBKR_GEX_CONSUME_ENABLED=true; publishes
#              only when IBKR_GEX_PUBLISH_ENABLED=true; BOOT-REFUSES if the legacy
#              PREOPEN_GEX_ENABLED publisher is also on. (IbkrPreOpenConfig also throws on
#              PUBLISH=true with CONSUME=false — mirrored below.)
#   DATABENTO  the legacy in-service capture+compute scheduler (PREOPEN_GEX_ENABLED=true).
#
# Because BOTH selections are individually valid, a render that silently selects the other
# publisher is a syntax error nowhere: it deploys cleanly and pre-market GEX simply stops
# appearing. On 2026-08-24 dev had run the IBKR window since 08-21 while main carried an
# undeployed 08-20 commit selecting DATABENTO; a routine config-only service-deploy applied
# that stale intent and switched the working publisher OFF mid pre-market.
#
# This gate closes that hole: every rendered overlay must match the selection DECLARED in
# k8s/preopen-publisher.env, so changing the publisher REQUIRES editing that file in the
# same PR — visible in the diff, reviewed — and can never again be the silent side effect of
# an unrelated patch, a merge order, or a stale branch.
#
# FAIL CLOSED: a missing Deployment, a duplicated env entry, or a value that is not exactly
# "true"/"false" fails. A render this assertion cannot fully see is never proof of anything.
# An absent flag is the CODE default false (IbkrPreOpenConfig#boolEnv), which is why absent
# is read as false rather than refused.
#
# Usage:
#   validate-preopen-single-publisher.sh                    # render + check every overlay
#   validate-preopen-single-publisher.sh <env> <render.yaml> # check one pre-rendered file
# Requires: yq (and kubectl when it renders the overlays itself).
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }

DECL="k8s/preopen-publisher.env"
[ -f "$DECL" ] || { echo "FATAL: missing declaration file $DECL" >&2; exit 1; }

DEPLOY_NAME="databento-gex-service"
FAILED=0

# Echo one env var's value out of the rendered gex Deployment. Prints "<absent>" when the
# key is not set and "<dup>" when the render carries it more than once (never guess which
# one k8s would win with).
flag_value() {
  local render="$1" name="$2" vals count
  # NO `//` HERE. The alternative operator is evaluated PER DOCUMENT, and a rendered overlay is
  # a multi-document stream: on yq 4.44 (the version CI installs) every document that is not the
  # gex Deployment produced a "<novalue>" line of its own, so the count was always greater than
  # one and this gate reported "<dup>" for all three flags in all three environments — a
  # permanently red check that says nothing about the manifests. yq 4.53 happens not to, which is
  # why it passed locally and failed in CI. Absent values print as "null" instead and are mapped
  # to <novalue> in the shell, where the behaviour does not depend on the tool's version.
  vals="$(yq -r "select(.kind==\"Deployment\" and .metadata.name==\"$DEPLOY_NAME\") | .spec.template.spec.containers[] | select(.name==\"databento-gex\") | .env[]? | select(.name==\"$name\") | .value" "$render" 2>/dev/null || true)"
  vals="$(printf '%s' "$vals" | sed 's/^null$/<novalue>/')"
  count="$(printf '%s' "$vals" | grep -c . || true)"
  if [ "$count" -eq 0 ]; then echo "<absent>"; return; fi
  if [ "$count" -gt 1 ]; then echo "<dup>"; return; fi
  printf '%s' "$vals"
}

# "true"/"false" only; absent is the code default false. Anything else is refused.
as_bool() {
  case "$1" in
    true)      echo true ;;
    false)     echo false ;;
    "<absent>") echo false ;;
    *)         echo "<bad>" ;;
  esac
}

check_env() {
  local env_name="$1" render="$2"

  if ! yq -e "select(.kind==\"Deployment\" and .metadata.name==\"$DEPLOY_NAME\") | .metadata.name" "$render" >/dev/null 2>&1; then
    echo "FAIL: $env_name — $DEPLOY_NAME is absent from the render; the pre-open publisher cannot be established"
    FAILED=1; return
  fi

  local raw_preopen raw_consume raw_publish preopen consume publish
  raw_preopen="$(flag_value "$render" PREOPEN_GEX_ENABLED)"
  raw_consume="$(flag_value "$render" IBKR_GEX_CONSUME_ENABLED)"
  raw_publish="$(flag_value "$render" IBKR_GEX_PUBLISH_ENABLED)"
  preopen="$(as_bool "$raw_preopen")"; consume="$(as_bool "$raw_consume")"; publish="$(as_bool "$raw_publish")"

  if [ "$preopen" = "<bad>" ] || [ "$consume" = "<bad>" ] || [ "$publish" = "<bad>" ]; then
    echo "FAIL: $env_name — unreadable publisher flags (PREOPEN_GEX_ENABLED='$raw_preopen'" \
         "IBKR_GEX_CONSUME_ENABLED='$raw_consume' IBKR_GEX_PUBLISH_ENABLED='$raw_publish');" \
         "each must be exactly \"true\" or \"false\" and appear at most once"
    FAILED=1; return
  fi

  # Mirror IbkrPreOpenConfig's own boot check so the pod never has to be the one to find it.
  if [ "$publish" = true ] && [ "$consume" != true ]; then
    echo "FAIL: $env_name — IBKR_GEX_PUBLISH_ENABLED=true requires IBKR_GEX_CONSUME_ENABLED=true (the service throws on boot)"
    FAILED=1; return
  fi

  local actual
  if   [ "$consume" = true ] && [ "$publish" = true ]  && [ "$preopen" = false ]; then actual=IBKR
  elif [ "$preopen" = true ] && [ "$consume" = false ] && [ "$publish" = false ]; then actual=DATABENTO
  elif [ "$preopen" = false ] && [ "$consume" = false ] && [ "$publish" = false ]; then actual=NONE
  elif [ "$preopen" = true ] && { [ "$consume" = true ] || [ "$publish" = true ]; }; then
    echo "FAIL: $env_name — BOTH pre-open publishers are enabled (PREOPEN_GEX_ENABLED=$preopen," \
         "IBKR_GEX_CONSUME_ENABLED=$consume, IBKR_GEX_PUBLISH_ENABLED=$publish); the service boot-refuses this"
    FAILED=1; return
  else
    echo "FAIL: $env_name — the publisher flags select no coherent publisher (PREOPEN_GEX_ENABLED=$preopen," \
         "IBKR_GEX_CONSUME_ENABLED=$consume, IBKR_GEX_PUBLISH_ENABLED=$publish); pre-open GEX would be silently absent"
    FAILED=1; return
  fi

  local key declared
  key="PREOPEN_PUBLISHER_$(printf '%s' "$env_name" | tr '[:lower:]' '[:upper:]')"
  declared="$(grep -E "^${key}=" "$DECL" | tail -1 | cut -d= -f2- | tr -d '[:space:]' || true)"
  if [ -z "$declared" ]; then
    echo "FAIL: $env_name — no $key declared in $DECL; every rendered environment must declare its publisher"
    FAILED=1; return
  fi
  case "$declared" in IBKR|DATABENTO|NONE) ;; *)
    echo "FAIL: $env_name — $key='$declared' in $DECL is not one of IBKR / DATABENTO / NONE"
    FAILED=1; return ;;
  esac

  if [ "$actual" != "$declared" ]; then
    echo "FAIL: $env_name — the render selects the $actual pre-open publisher but $DECL declares $declared."
    echo "      Deploying this would silently switch which publisher owns pre-market GEX."
    echo "      If the change IS intended, update $key in $DECL in this same change so the switch is explicit and reviewed."
    FAILED=1; return
  fi

  echo "ok: $env_name pre-open publisher = $actual (declared $declared)"
}

if [ "$#" -eq 2 ]; then
  check_env "$1" "$2"
else
  command -v kubectl >/dev/null 2>&1 || { echo "FATAL: kubectl is required to render the overlays" >&2; exit 1; }
  TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
  for OVERLAY in k8s/overlays/*/; do
    ENV_NAME="$(basename "$OVERLAY")"
    if ! kubectl kustomize "$OVERLAY" > "$TMP/$ENV_NAME.yaml" 2>"$TMP/$ENV_NAME.err"; then
      echo "FAIL: $ENV_NAME — kustomize render failed:"; sed 's/^/      /' "$TMP/$ENV_NAME.err" | head -5
      FAILED=1; continue
    fi
    check_env "$ENV_NAME" "$TMP/$ENV_NAME.yaml"
  done
fi

if [ "$FAILED" -ne 0 ]; then
  echo "=== validate-preopen-single-publisher: FAILED ==="
  exit 1
fi
echo "=== validate-preopen-single-publisher: OK ==="
