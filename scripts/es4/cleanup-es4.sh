#!/usr/bin/env bash
# cleanup-es4.sh — ATOMIC reset of the es4 Kafka + Schema Registry (the "cleanup v2").
#
# Runs ON the es4 box (rsync'd there by the es4-deploy Jenkins job, CLEAN_RESET action).
#
# WHY THIS EXISTS
# --------------
# The scorer/gex hit "org.apache.avro.AvroRuntimeException: Malformed data. Length is
# negative" on options.databento.raw. Root cause: a message framed with Confluent schema
# id=N was written when id=N meant schema A, but Kafka's _schemas topic (where the Schema
# Registry stores its id->schema map, on the SAME persistent volume as the data topics) was
# later wiped/re-seeded so id=N now resolves to a DIFFERENT schema B (e.g. id=1 became
# PaceRankSnapshot). Reading the old message then decodes A's bytes with B's schema -> garbage.
#
# The ONLY way a stale id can appear is if _schemas is re-seeded OUT OF LOCKSTEP with the topic
# messages that reference the old ids. So the invariant this script enforces is:
#
#   Never re-seed the Schema Registry without also wiping every Avro data topic (and vice
#   versa) — wipe them TOGETHER, atomically, so no message can outlive its schema id.
#
# It does that by wiping the WHOLE Kafka data volume (all topics INCLUDING _schemas) in one
# shot while producers are quiesced, then recreating the core topics and restarting the app so
# every message thereafter references only current-registry ids.
#
# Idempotent + safe to re-run. Honors DEPLOY_DRY_RUN=true (print, do nothing destructive).
set -euo pipefail

ES4_HOME=/home/es4
INFRA_DIR="$ES4_HOME/infra"
KAFKA_DATA=/home/es4/volumes/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY="${DEPLOY_DRY_RUN:-false}"
KC="sudo -n /usr/local/bin/k3s kubectl -n options-edge"

log() { printf '\n=== %s ===\n' "$*"; }
run() { if [ "$DRY" = "true" ]; then echo "DRY: $*"; else eval "$*"; fi; }

[ -d "$INFRA_DIR" ] || { echo "no $INFRA_DIR — run bootstrap-es4.sh (infra-sync) first" >&2; exit 1; }

# ---------------------------------------------------------------- 1. quiesce the app
# Scale every app Deployment to 0 (capturing its current replicas) so nothing produces or
# re-registers a schema mid-wipe. A producer that wrote between the wipe and create-topics
# would auto-create a topic with the wrong config and re-open the very race we are closing.
log "quiescing app: scaling es4 Deployments to 0 (replica counts captured for restore)"
REPLICA_STATE=/tmp/es4-cleanup-replicas.tsv
if [ "$DRY" = "true" ]; then
  $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers 2>/dev/null || true
else
  $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers > "$REPLICA_STATE" 2>/dev/null || true
  while read -r name reps; do
    [ -n "$name" ] || continue
    $KC scale "deploy/$name" --replicas=0 >/dev/null 2>&1 || true
  done < "$REPLICA_STATE"
  # best-effort wait for pods to drain (bounded)
  $KC wait --for=delete pod --all --timeout=120s >/dev/null 2>&1 || sleep 15
fi

# ---------------------------------------------------- 2. stop infra + wipe Kafka atomically
log "docker compose down (stop kafka/SR/mm2 — no producers can reach a stopped broker)"
run "(cd '$INFRA_DIR' && docker compose down)"

# Wipe the CONTENTS of the bind-mounted Kafka data dir (topics + _schemas together), NOT the
# dir itself (compose re-mounts it). This is the atomic step: _schemas and every data topic
# disappear in the same instant, so no surviving message can reference a to-be-reassigned id.
log "wiping Kafka data volume contents: $KAFKA_DATA/* (all topics + _schemas)"
[ -d "$KAFKA_DATA" ] || { echo "expected Kafka data dir $KAFKA_DATA missing" >&2; exit 1; }
run "sudo find '$KAFKA_DATA' -mindepth 1 -maxdepth 1 -exec rm -rf {} +"

# ------------------------------------------------------------ 3. bring infra back up
log "docker compose up -d (fresh kafka + empty SR)"
run "(cd '$INFRA_DIR' && docker compose up -d)"
if [ "$DRY" != "true" ]; then
  log "waiting for kafka + schema-registry health (fail-closed)"
  ok=false
  for _ in $(seq 1 40); do
    hk=$(docker inspect -f '{{.State.Health.Status}}' es4-kafka 2>/dev/null || echo starting)
    hs=$(docker inspect -f '{{.State.Health.Status}}' es4-schema-registry 2>/dev/null || echo starting)
    if [ "$hk" = healthy ] && [ "$hs" = healthy ]; then ok=true; break; fi
    sleep 5
  done
  [ "$ok" = true ] || { echo "CLEANUP FAILED: kafka/SR not healthy after 200s" >&2; exit 1; }
fi

# ------------------------------------------------------------ 4. recreate core topics
log "recreating core es.* topics (create-es-topics.sh)"
run "bash '$SCRIPT_DIR/create-es-topics.sh'"

# ------------------------------------------------------- 5. restart the app (clean re-seed)
# Restore each Deployment to its captured replica count. The pods start against a fresh Kafka
# + empty SR, so every schema they register and every message they produce references only
# current-registry ids — no stale id can exist because there are no pre-wipe messages left.
log "restoring app Deployments to their captured replica counts"
if [ "$DRY" = "true" ]; then
  echo "DRY: would restore replicas from $REPLICA_STATE"
elif [ -s "$REPLICA_STATE" ]; then
  while read -r name reps; do
    [ -n "$name" ] || continue
    [ "$reps" = "<none>" ] && reps=1
    $KC scale "deploy/$name" --replicas="$reps" >/dev/null 2>&1 || true
  done < "$REPLICA_STATE"
fi

log "es4 clean reset complete — Kafka + Schema Registry wiped and re-seeded ATOMICALLY"
