#!/usr/bin/env bash
# schema-sync.sh - the deploy gate of docs/schema-governance (options-edge-processing). Before a service's workload changes, make the
# target environment's Schema Registry hold exactly the manifest's schemas for that service, or fail.
#
# WHY: a service whose Avro subject is absent from the registry crash-loops on a 40401 while the build reports success (prod
# early-auction-verdict-service, 2026-11-03: 96 restarts, 9 h unnoticed). This stage moves that failure in front of the rollout.
#
# Inputs (environment):
#   SERVICE              manifest service name = the dir under k8s/services (required)
#   ENVIRONMENT          dev | production | es4 (required)
#   SCHEMA_REGISTRY_URL  the registry of that environment (required; never guessed)
#   SCHEMA_GOV_JAR       path to options-edge-schema-governance-tool-*-all.jar (required)
#   SCHEMA_IMAGE_REF     the image that will roll, PINNED BY DIGEST (host/repo@sha256:...). Its options-edge.schema-requirements
#                        label is read from the registry here and must match the manifest bindings exactly (R5e). A caller can
#                        not supply a label file: the gate judges the artifact, not a claim about it. An image WITHOUT the label
#                        is accepted only for a service the manifest lists in declaredByManifest (R5e transition); otherwise fail.
#   SCHEMA_IMAGE_PLATFORM  os/arch used when the ref is an image index (default linux/amd64)
#   DEPLOY_DRY_RUN       true = report only. Prints PLAN_SHA256.
#   PLAN_SHA256          REQUIRED for a production apply: the hash a dry run of the SAME deploy commit, manifest, snapshots and
#                        registry printed (R13a). Any difference aborts with nothing written.
# Only ensure-fixed-id-schemas.sh writes to the registry; this script builds the map it is given.
set -euo pipefail
cd "$(dirname "$0")/../.."

SERVICE="${SERVICE:?SERVICE unset}"
ENVIRONMENT="${ENVIRONMENT:?ENVIRONMENT unset}"
REG="${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL unset - refusing to guess a registry}"
JAR="${SCHEMA_GOV_JAR:?SCHEMA_GOV_JAR unset}"
DRY="${DEPLOY_DRY_RUN:-false}"
[ -f "$JAR" ] || { echo "FAIL: SCHEMA_GOV_JAR $JAR is not a file"; exit 1; }
case "$ENVIRONMENT" in
  dev) ENVN=dev ;;
  production) ENVN=prod ;;
  es4) ENVN=es4 ;;
  *) echo "FAIL: ENVIRONMENT must be dev, production or es4 (got '$ENVIRONMENT')"; exit 1 ;;
esac
MANIFEST=schema-manifest/manifest.json
SNAP=schema-manifest/schemas
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
gov() { java -jar "$JAR" "$@" 2> >(grep -v '^SLF4J' >&2); }

echo "schema-sync: service=$SERVICE environment=$ENVN registry=$REG dry_run=$DRY"
gov validate --manifest "$MANIFEST" --snapshots "$SNAP"

LABEL_ARGS=()
if [ "$SERVICE" = "@all" ]; then
  # Whole-environment mode (R3f): every MIRRORED entry the manifest lists for this environment, bound to no service and so to no image label.
  # It is how a registry gets a schema that a byte-copy mirror carries but no deployed service owns (the es4->prod aggressor-flow case). Same
  # validate, same read-only pre-pass, same PLAN_SHA256 requirement for a production write; the engine still refuses any collision.
  [ -z "${SCHEMA_IMAGE_REF:-}" ] || { echo "FAIL: SERVICE=@all takes no SCHEMA_IMAGE_REF"; exit 1; }
  echo "schema-sync: whole-environment plan for $ENVN (no service, no image label)"
  gov plan --all --manifest "$MANIFEST" --env "$ENVN" --out "$WORK"
else
  # R5e: the label comes from the selected image, by digest, never from a caller-supplied file.
  if [ -n "${SCHEMA_IMAGE_REF:-}" ]; then
    rc=0
    gov image-label --image "$SCHEMA_IMAGE_REF" --platform "${SCHEMA_IMAGE_PLATFORM:-linux/amd64}" > "$WORK/label.json" || rc=$?
    case "$rc" in
      0) LABEL_ARGS=(--label "$WORK/label.json"); echo "schema-sync: image label read from $SCHEMA_IMAGE_REF" ;;
      3) if gov declared --manifest "$MANIFEST" --service "$SERVICE"; then
           echo "schema-sync: UNVERIFIED image - $SERVICE has no baked label and is declaredByManifest (R5e transition)"
         else
           echo "FAIL: image $SCHEMA_IMAGE_REF has no options-edge.schema-requirements label and $SERVICE is not declaredByManifest (R5e)"; exit 1
         fi ;;
      *) echo "FAIL: could not read the image label from $SCHEMA_IMAGE_REF (rc=$rc)"; exit 1 ;;
    esac
  else
    if gov declared --manifest "$MANIFEST" --service "$SERVICE"; then
      echo "schema-sync: UNVERIFIED image - no SCHEMA_IMAGE_REF and $SERVICE is declaredByManifest (R5e transition)"
    else
      echo "FAIL: SCHEMA_IMAGE_REF (digest-pinned) is required: $SERVICE is not declaredByManifest, so its image label must be read and matched (R5e)"; exit 1
    fi
  fi
  gov plan --manifest "$MANIFEST" --env "$ENVN" --service "$SERVICE" --out "$WORK" "${LABEL_ARGS[@]}"
fi

# Read-only registry state recorded with the plan (R13a step 2). An unreadable registry is a failure, not an empty answer.
MODE="$(curl -sS -m 15 --fail "${REG%/}/mode")" || { echo "FAIL: registry $REG unreachable"; exit 1; }
CONF="$(curl -sS -m 15 --fail "${REG%/}/config")" || { echo "FAIL: registry $REG unreachable"; exit 1; }
echo "schema-sync: registry mode=$MODE config=$CONF"

# The plan hash binds registry URL, the rows, EVERY snapshot byte that would be registered, and the deploy commit (R13a), so a
# snapshot or manifest edited after review changes it even if the rows look the same.
DEPLOY_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
PLAN_SHA="$( {
  printf 'registry=%s\ncommit=%s\n' "${REG%/}" "$DEPLOY_HEAD"
  cat "$WORK/plan.map"
  cut -d'|' -f3 "$WORK/plan.map" | sort -u | while read -r f; do printf '%s ' "$f"; shasum -a 256 "$SNAP/$f" | cut -d' ' -f1; done
  shasum -a 256 "$MANIFEST" | cut -d' ' -f1
} | shasum -a 256 | cut -d' ' -f1)"
echo "PLAN_SHA256=$PLAN_SHA"

# A production apply that would WRITE needs the reviewed plan hash; a steady-state deploy (everything already registered) writes
# nothing, so it is not held up by it. "Would write" is judged by the engine's own read-only pass, never guessed here.
NEEDS_WRITE=false
if [ "$ENVN" = "prod" ] && [ "$DRY" != "true" ]; then
  PRE="$(SCHEMA_REGISTRY_URL="$REG" SCHEMAS_DIR="$SNAP" SCHEMA_ID_MAP="$WORK/plan.map" DEPLOY_DRY_RUN=true bash scripts/kafka/ensure-fixed-id-schemas.sh 2>&1)" \
    || { printf '%s\n' "$PRE"; echo "FAIL: the read-only pre-pass refused this plan"; exit 1; }
  printf '%s' "$PRE" | grep -q "would import" && NEEDS_WRITE=true
fi
if [ "$NEEDS_WRITE" = "true" ]; then
  [ -n "$DEPLOY_HEAD" ] || { echo "FAIL: not a git checkout - a production write must be bound to a deploy commit"; exit 1; }
  [ -z "$(git status --porcelain -- schema-manifest scripts/kafka/ensure-fixed-id-schemas.sh scripts/ci/schema-sync.sh)" ] \
    || { echo "FAIL: the manifest, snapshots or sync scripts have uncommitted changes - a production write needs a clean checkout"; exit 1; }
  [ "${PLAN_SHA256:-}" = "$PLAN_SHA" ] \
    || { echo "FAIL: production apply needs PLAN_SHA256=$PLAN_SHA from a reviewed dry run of this exact commit, manifest and registry (got '${PLAN_SHA256:-}')"; exit 1; }
fi

SCHEMA_REGISTRY_URL="$REG" SCHEMAS_DIR="$SNAP" SCHEMA_ID_MAP="$WORK/plan.map" DEPLOY_DRY_RUN="$DRY" \
  bash scripts/kafka/ensure-fixed-id-schemas.sh
echo "schema-sync: OK service=$SERVICE environment=$ENVN"
