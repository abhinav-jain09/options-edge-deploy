#!/usr/bin/env bash
# cleanup-es4.sh — ATOMIC es4 Kafka + Schema-Registry reset (the "cleanup v2").
#
# Runs ON the es4 box (rsync'd by es4-deploy, CLEAN_RESET action). The Jenkins stage MUST
# quiesce the cross-cluster prod `es-feed` producer (options-edge/es-feed on .252, publishing to
# 192.168.100.4:9092 + :8081) BEFORE invoking this and restore it AFTER — this script cannot reach
# the prod cluster. It refuses to run unless told the external feed is fenced (ES4_FEED_FENCED=1).
#
# WHY: the scorer/gex hit "AvroRuntimeException: Malformed data. Length is negative" —
# a message framed with Confluent schema id=N (written when id=N meant schema A) survived a
# re-seed of Kafka's _schemas topic (the SR id->schema map, on the SAME volume as the data
# topics) so id=N now resolves to schema B (id=1 -> PaceRankSnapshot). A stale id can ONLY
# appear when _schemas is re-seeded out of lockstep with the messages referencing the old ids.
# Invariant enforced here: wipe _schemas AND every data topic TOGETHER, with ALL producers
# fenced, so no message can outlive its schema id.
#
# Safety: preflight before ANY mutation; fail-closed quiescence (verified, not best-effort);
# durable resume state (survives interruption without losing the original replica counts);
# single-instance lock; canonical-path-guarded delete. Honors DEPLOY_DRY_RUN=true (no mutation).
set -euo pipefail

ES4_HOME=/home/es4
INFRA_DIR="$ES4_HOME/infra"
KAFKA_DATA=/home/es4/volumes/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY="${DEPLOY_DRY_RUN:-false}"
STATE="$ES4_HOME/.es4-cleanup.state"          # durable (NOT /tmp): original replicas for resume
LOCKDIR="$ES4_HOME/.es4-cleanup.lock"
KC="sudo -n /usr/local/bin/k3s kubectl -n options-edge"

log()  { printf '\n=== %s ===\n' "$*"; }
die()  { echo "CLEANUP ABORT: $*" >&2; exit 1; }
run()  { if [ "$DRY" = "true" ]; then echo "DRY: $*"; else eval "$*"; fi; }

# ------------------------------------------------------------ 0. PREFLIGHT (before any mutation)
log "preflight (no mutation happens unless every check passes)"
[ "${ES4_FEED_FENCED:-0}" = "1" ] || [ "$DRY" = "true" ] || \
  die "external prod es-feed not fenced (ES4_FEED_FENCED!=1) — the Jenkins clean-reset stage must scale options-edge/es-feed to 0 on .252 first, else it re-pollutes the fresh cluster with cached schema ids"
[ -d "$INFRA_DIR" ] || die "no $INFRA_DIR — run bootstrap-es4.sh (infra-sync) first"
[ -f "$INFRA_DIR/docker-compose.yml" ] || die "no compose file in $INFRA_DIR"
# canonical-path guard for the destructive rm: KAFKA_DATA must be the exact compose bind source,
# a real directory (not a symlink), and match the compose file — never delete anything else.
[ -d "$KAFKA_DATA" ] && [ ! -L "$KAFKA_DATA" ] || die "Kafka data dir $KAFKA_DATA missing or is a symlink"
CANON="$(cd "$KAFKA_DATA" && pwd -P)"
[ "$CANON" = "$KAFKA_DATA" ] || die "Kafka data path is not canonical ($CANON != $KAFKA_DATA)"
grep -qF "$KAFKA_DATA:/var/lib/kafka/data" "$INFRA_DIR/docker-compose.yml" \
  || die "$KAFKA_DATA is not the compose Kafka bind source — refusing to delete"
if [ "$DRY" != "true" ]; then
  $KC version >/dev/null 2>&1 || die "sudo -n k3s kubectl not usable (sudoers/kubeconfig) — fix before a destructive run"
fi
command -v docker >/dev/null || die "docker not found"

# single-instance lock (mkdir is atomic); trap removes it on exit
if [ "$DRY" != "true" ]; then
  mkdir "$LOCKDIR" 2>/dev/null || die "another cleanup is running (lock $LOCKDIR present) — refusing concurrent reset"
  trap 'rmdir "$LOCKDIR" 2>/dev/null || true' EXIT
fi

# ------------------------------------------------------------ 1. capture / resume replica state
# If a prior run was interrupted, $STATE already holds the ORIGINAL replica counts. NEVER
# overwrite it while it exists (a re-capture now would read the scaled-to-0 counts and restore 0).
# A resume re-runs the idempotent wipe using the preserved originals.
if [ -s "$STATE" ]; then
  log "resuming interrupted reset — reusing captured replica state $STATE (not re-capturing)"
else
  log "capturing es4 Deployment replica counts -> $STATE"
  if [ "$DRY" = "true" ]; then
    $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers 2>/dev/null || true
  else
    TMP="$STATE.tmp.$$"
    $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers > "$TMP" \
      || die "could not enumerate Deployments — aborting BEFORE any wipe"
    [ -s "$TMP" ] || die "no Deployments found (unexpected) — aborting before wipe"
    mv -f "$TMP" "$STATE"          # atomic publish of the durable state
  fi
fi

# ------------------------------------------------------------ 2. quiesce app (fail-closed + verified)
log "scaling es4 Deployments to 0 and PROVING every one reached 0 replicas"
if [ "$DRY" != "true" ]; then
  while read -r name reps; do
    [ -n "$name" ] || continue
    $KC scale "deploy/$name" --replicas=0 >/dev/null || die "scale-to-0 failed for $name — aborting before wipe"
  done < "$STATE"
  # verify: every Deployment reports 0 replicas, and no pods remain (bounded poll; fail-closed).
  ok=false
  for _ in $(seq 1 30); do
    bad=$($KC get deploy -o jsonpath='{range .items[*]}{.status.replicas}{"\n"}{end}' 2>/dev/null | grep -vE '^(0)?$' | wc -l | tr -d ' ')
    pods=$($KC get pods --no-headers 2>/dev/null | grep -vE 'Completed|Terminating' | wc -l | tr -d ' ')
    if [ "${bad:-1}" = "0" ] && [ "${pods:-1}" = "0" ]; then ok=true; break; fi
    sleep 5
  done
  [ "$ok" = true ] || die "app not fully quiesced (residual replicas/pods) — refusing to wipe with producers alive"
fi

# ------------------------------------------------------- 3. offline coordinated wipe (kafka down)
log "docker compose down (broker offline — no producer can reach it)"
run "(cd '$INFRA_DIR' && docker compose down)"
log "wiping Kafka data volume CONTENTS ($KAFKA_DATA/* — all topics + _schemas), Kafka offline"
run "sudo -n find '$KAFKA_DATA' -mindepth 1 -maxdepth 1 -exec rm -rf {} +"
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
  [ "$ok" = true ] || die "kafka/SR not healthy after 200s — app left down (rerun resumes from $STATE)"
fi

# ------------------------------------------------------------ 4. recreate core topics
log "recreating core es.* topics (create-es-topics.sh)"
run "bash '$SCRIPT_DIR/create-es-topics.sh'"

# ------------------------------------------------- 5. restore app (verified) then clear state
log "restoring es4 Deployments to captured replica counts (verified) from $STATE"
if [ "$DRY" = "true" ]; then
  echo "DRY: would restore replicas from $STATE and verify"
else
  fail=0
  while read -r name reps; do
    [ -n "$name" ] || continue
    [ "$reps" = "<none>" ] && reps=1
    $KC scale "deploy/$name" --replicas="$reps" >/dev/null || { echo "restore scale FAILED for $name" >&2; fail=1; }
  done < "$STATE"
  # verify each Deployment's spec.replicas matches the captured value
  while read -r name reps; do
    [ -n "$name" ] || continue
    [ "$reps" = "<none>" ] && reps=1
    got=$($KC get "deploy/$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
    [ "$got" = "$reps" ] || { echo "restore MISMATCH $name: want $reps got $got" >&2; fail=1; }
  done < "$STATE"
  [ "$fail" = 0 ] || die "restore incomplete — leaving $STATE for a resume (rerun to finish restore)"
  rm -f "$STATE"                 # success: clear durable state so a later run captures fresh
fi

log "es4 clean reset COMPLETE — Kafka + Schema Registry wiped and re-seeded atomically; app restored"
