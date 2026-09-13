#!/usr/bin/env bash
# vix-pause-marker.sh claim <marker> <replicas> | owned <marker> | replicas <marker>
#
# The recovery marker of service-deploy's vix-option-inteligence pause, and WHO OWNS IT.
#
# Deployment Permission Rule: post{always} of service-deploy restores the VIX deployment from this
# marker. A marker is a promise that THIS build paused the service. A marker another build left behind
# (it paused, then died before its own restore) is not this build's to act on: this build's permission
# covers its own run, not a restore to a replica count recorded by someone else's. So the marker
# records the run that wrote it, and recovery acts only on a marker whose record is this run's.
#
#   claim <marker> <replicas>  — before the pause. Refuses (exit 1, nothing written) when ANY marker
#                                already exists: a stranded marker is preserved and reported, never
#                                adopted or overwritten, and this build does not pause on top of it.
#                                Refuses outside a permitted, non-dry VIX run (the four facts below).
#                                Writes the marker atomically BEFORE the caller scales down.
#   owned <marker>             — exit 0 iff the marker exists, is well formed, and was written by THIS
#                                run: SERVICE=vix-option-inteligence, JOB_NAME, BUILD_ID and
#                                PERMITTED_SHA equal to this build's, REPLICAS a number. Prints why not.
#   replicas <marker>          — prints REPLICAS of an owned marker.
#
# This run = the environment Jenkins gives the build: JOB_NAME, BUILD_ID, PERMITTED_SHA, SERVICE_PARAM,
# DEPLOY_DRY_RUN_PARAM. A dry run never owns a marker.
set -uo pipefail
cmd="${1:?usage: vix-pause-marker.sh claim <marker> <replicas> | owned <marker> | replicas <marker>}"
mark="${2:?marker path}"
SERVICE_NAME=vix-option-inteligence

field() { sed -n "s/^$1=//p" "$mark" 2>/dev/null | head -1; }

this_run_problem() {
  [ "${SERVICE_PARAM:-}" = "$SERVICE_NAME" ] || { echo "this build deploys '${SERVICE_PARAM:-}', not $SERVICE_NAME"; return; }
  [ "${DEPLOY_DRY_RUN_PARAM:-false}" != "true" ] || { echo "this build is a dry run"; return; }
  [ -n "${JOB_NAME:-}" ] && [ -n "${BUILD_ID:-}" ] || { echo "JOB_NAME/BUILD_ID are not set"; return; }
  case "${PERMITTED_SHA:-}" in
    *[!0-9a-f]*|'') echo "PERMITTED_SHA is not a commit id"; return ;;
  esac
  [ "${#PERMITTED_SHA}" -eq 40 ] || { echo "PERMITTED_SHA is not a full commit id"; return; }
}

owned_problem() {
  [ -f "$mark" ] || { echo "no marker"; return; }
  local p
  p="$(this_run_problem)"
  [ -z "$p" ] || { echo "$p"; return; }
  [ "$(field VIX_PAUSE_MARKER_FORMAT)" = "2" ] || { echo "marker is not a recorded-owner marker (written by an older definition or by hand)"; return; }
  [ "$(field SERVICE)" = "$SERVICE_NAME" ] || { echo "marker names service '$(field SERVICE)'"; return; }
  [ "$(field JOB_NAME)" = "$JOB_NAME" ] || { echo "marker was written by job '$(field JOB_NAME)', not $JOB_NAME"; return; }
  [ "$(field BUILD_ID)" = "$BUILD_ID" ] || { echo "marker was written by build $(field BUILD_ID), not this build $BUILD_ID"; return; }
  [ "$(field PERMITTED_SHA)" = "$PERMITTED_SHA" ] || { echo "marker was written under permission $(field PERMITTED_SHA), not $PERMITTED_SHA"; return; }
  case "$(field REPLICAS)" in
    ''|*[!0-9]*) echo "marker REPLICAS '$(field REPLICAS)' is not a number"; return ;;
  esac
}

case "$cmd" in
  claim)
    replicas="${3:?replicas}"
    case "$replicas" in ''|*[!0-9]*) echo "vix-pause-marker: REFUSED — replicas '$replicas' is not a number" >&2; exit 1 ;; esac
    p="$(this_run_problem)"
    [ -z "$p" ] || { echo "vix-pause-marker: REFUSED to claim — $p; nothing paused" >&2; exit 1; }
    if [ -e "$mark" ]; then
      echo "HUMAN REQUIRED: a recovery marker already exists at $mark (it is not this build's):" >&2
      sed 's/^/  marker: /' "$mark" >&2 2>/dev/null || true
      echo "Another run paused $SERVICE_NAME and never restored it. This build will NOT pause on top of it, adopt it or overwrite it." >&2
      echo "Restore the deployment to the recorded size by hand (or confirm it), remove the marker, then re-run." >&2
      exit 1
    fi
    mkdir -p "$(dirname "$mark")"
    tmp="$mark.tmp.$$"
    {
      echo "VIX_PAUSE_MARKER_FORMAT=2"
      echo "SERVICE=$SERVICE_NAME"
      echo "JOB_NAME=$JOB_NAME"
      echo "BUILD_ID=$BUILD_ID"
      echo "PERMITTED_SHA=$PERMITTED_SHA"
      echo "REPLICAS=$replicas"
    } > "$tmp" && mv "$tmp" "$mark" || { rm -f "$tmp"; echo "vix-pause-marker: could not write $mark — refusing to pause" >&2; exit 1; }
    echo "vix-pause-marker: claimed $mark for $JOB_NAME #$BUILD_ID (replicas=$replicas, permitted $PERMITTED_SHA)"
    ;;
  owned)
    p="$(owned_problem)"
    if [ -z "$p" ]; then
      echo "vix-pause-marker: $mark is this build's ($JOB_NAME #$BUILD_ID, replicas=$(field REPLICAS))"
      exit 0
    fi
    echo "vix-pause-marker: not this build's marker — $p"
    exit 1
    ;;
  replicas)
    p="$(owned_problem)"
    [ -z "$p" ] || { echo "vix-pause-marker: REFUSED — $p" >&2; exit 1; }
    field REPLICAS
    ;;
  *) echo "vix-pause-marker: unknown command '$cmd'" >&2; exit 2 ;;
esac
