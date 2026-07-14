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
LOCKDIR="$ES4_HOME/.es4-cleanup.lock"         # flock target (kernel-released on any exit)
KC="sudo -n /usr/local/bin/k3s kubectl -n options-edge"
# es-web is a UI CONSUMER (it reads es.* topics for the browser feed; it does NOT produce Avro / register
# schemas), so keeping it UP across the wipe cannot re-pollute the fresh cluster with cached schema ids.
# Keep it running so es.fullfunding.nl never drops during a clean-reset. Excluded from scale-to-0, the
# "all reached 0" quiesce proof, and restore. Space-separated deployment names. (2026-07-14)
SCALE_EXCLUDE="es-web"

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
command -v docker >/dev/null || die "docker not found"
# Validate the bind through the RENDERED compose config (a raw-file grep could match a comment
# or a stale service). The Kafka data path must be the resolved bind SOURCE.
(cd "$INFRA_DIR" && docker compose config 2>/dev/null) | grep -qE "(source: |[[:space:]])$KAFKA_DATA([[:space:]:]|\$)" \
  || die "$KAFKA_DATA is not the compose-resolved Kafka bind source — refusing to delete"
if [ "$DRY" != "true" ]; then
  $KC version >/dev/null 2>&1 || die "sudo -n k3s kubectl not usable (sudoers/kubeconfig) — fix before a destructive run"
  # Preflight the DESTRUCTIVE command's sudo too (not just kubectl): a non-destructive probe of the
  # same `sudo -n` rule, so we never scale the app to 0 and then discover the rm is unauthorized.
  sudo -n test -d "$KAFKA_DATA" || die "sudo -n cannot access $KAFKA_DATA — the wipe would fail AFTER the app is down; fix sudoers first"
fi

# single-instance lock via flock: kernel-released when this process exits FOR ANY REASON
# (including SIGKILL / reboot), so a hard-killed run never leaves a stale lock that blocks the
# documented resume — unlike a mkdir/rmdir lock that only clears via the EXIT trap.
if [ "$DRY" != "true" ]; then
  exec 9>"$LOCKDIR"
  flock -n 9 || die "another cleanup holds the lock ($LOCKDIR) — refusing concurrent reset"
fi

# ------------------------------------------------------------ 1. capture / resume replica state
# If a prior run was interrupted, $STATE already holds the ORIGINAL replica counts. NEVER
# overwrite it while it exists (a re-capture now would read the scaled-to-0 counts and restore 0).
# A resume re-runs the idempotent wipe using the preserved originals.
# ONE durable state file: line 1 = PHASE (WIPING|RESTORED), lines 2+ = "name replicas". Written
# ATOMICALLY (temp+mv) so the phase is always consistent with its content — no second marker file
# that could go stale and make a future reset silently skip the wipe. A resume keys off line 1.
REPS() { tail -n +2 "$STATE" | awk -v ex="$SCALE_EXCLUDE" 'BEGIN{n=split(ex,a," ");for(i=1;i<=n;i++)x[a[i]]=1} !($1 in x)'; }   # replica lines (skip phase header) MINUS SCALE_EXCLUDE
# A legacy state file from a pre-single-file build would have a deployment row (not a phase word) on
# line 1. Never misread that as a phase — fail closed and make the operator inspect/clear it.
[ -e "$ES4_HOME/.es4-cleanup.phase" ] && die "legacy phase marker $ES4_HOME/.es4-cleanup.phase present — inspect + remove it (and $STATE) before rerunning"
if [ -s "$STATE" ]; then
  ph=$(head -n1 "$STATE" 2>/dev/null || true)
  case "$ph" in WIPING|RESTORED) ;; *) die "unrecognized state file $STATE (line 1='$ph', not WIPING/RESTORED) — legacy or corrupt; inspect + remove it before rerunning" ;; esac
  if [ "$ph" = "RESTORED" ]; then
    # A prior run verified RESTORE and was killed before clearing state. Re-wiping now would destroy
    # data produced since — only the state cleanup remained. Do it and exit (never re-wipe restored data).
    log "prior run already restored the app (phase=RESTORED); clearing leftover state, NO wipe"
    [ "$DRY" = "true" ] || rm -f "$STATE"
    exit 0
  fi
  log "resuming interrupted reset (phase=${ph:-WIPING}) — reusing captured replicas from $STATE"
else
  log "capturing es4 Deployment replica counts (phase WIPING) -> $STATE"
  if [ "$DRY" = "true" ]; then
    $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers 2>/dev/null || true
  else
    TMP="$STATE.tmp.$$"
    { echo WIPING; $KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers; } > "$TMP" \
      || die "could not enumerate Deployments — aborting BEFORE any wipe"
    [ "$(wc -l < "$TMP")" -ge 2 ] || die "no Deployments found (unexpected) — aborting before wipe"
    mv -f "$TMP" "$STATE"          # atomic publish: phase WIPING committed WITH the captured replicas
  fi
fi

# ------------------------------------------------------------ 2. quiesce app (fail-closed + verified)
log "scaling es4 Deployments to 0 and PROVING every one reached 0 replicas"
if [ "$DRY" != "true" ]; then
  while read -r name reps; do
    [ -n "$name" ] || continue
    $KC scale "deploy/$name" --replicas=0 >/dev/null || die "scale-to-0 failed for $name — aborting before wipe"
  done < <(REPS)
  # verify: every Deployment reports 0 replicas, and no pods remain (bounded poll; fail-closed).
  ok=false
  for _ in $(seq 1 30); do
    # awk (not grep|wc): always emits a number, no pipefail exit-code coupling. bad = Deployments
    # whose status.replicas is present and != 0; pods = live pods (excl. terminating/completed).
    bad=$($KC get deploy -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.replicas}{"\n"}{end}' 2>/dev/null | awk -v ex="$SCALE_EXCLUDE" 'BEGIN{n=split(ex,a," ");for(i=1;i<=n;i++)x[a[i]]=1} !($1 in x) && $2!="" && $2!=0 {c++} END{print c+0}')
    # Require producer pods to be ACTUALLY DELETED — a Terminating pod still runs during its grace
    # period and could reconnect + write cached schema ids. Count anything not in a terminal phase
    # (Terminating pods are still present -> counted -> we keep waiting until they are gone).
    pods=$($KC get pods --no-headers 2>/dev/null | awk -v ex="$SCALE_EXCLUDE" 'BEGIN{n=split(ex,a," ")} {skip=0; for(i=1;i<=n;i++) if($1 ~ "^"a[i]"-") skip=1; if(!skip && $3!="Completed" && $3!="Succeeded") c++} END{print c+0}')
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
# Start ONLY the datastores (kafka + schema-registry + postgres + redis) — NOT mm2 (a producer
# that could auto-create topics before reconciliation). Topics are created next; mm2 + the app
# producers start only AFTER topics exist, so nothing races topic reconciliation on the fresh cluster.
log "docker compose up -d kafka schema-registry postgres redis (datastores first, mm2 held back)"
run "(cd '$INFRA_DIR' && docker compose up -d kafka schema-registry postgres redis)"
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

# ------------------------------------------------------------ 4. recreate core topics, THEN mm2
log "recreating core es.* topics (create-es-topics.sh) before any producer starts"
run "bash '$SCRIPT_DIR/create-es-topics.sh'"
log "docker compose up -d (start mm2 + any remaining infra now that topics exist)"
run "(cd '$INFRA_DIR' && docker compose up -d)"

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
  done < <(REPS)
  # verify each Deployment's spec.replicas matches the captured value
  while read -r name reps; do
    [ -n "$name" ] || continue
    [ "$reps" = "<none>" ] && reps=1
    got=$($KC get "deploy/$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
    [ "$got" = "$reps" ] || { echo "restore MISMATCH $name: want $reps got $got" >&2; fail=1; }
  done < <(REPS)
  [ "$fail" = 0 ] || die "restore incomplete — leaving $STATE for a resume (rerun to finish restore)"
  # Atomically flip the SINGLE state file's phase to RESTORED, THEN unlink it. If killed between the
  # flip and the unlink, a resume reads phase=RESTORED and only clears state — it never re-wipes. There
  # is no second file to go stale, so a later fresh reset can never inherit a spurious RESTORED.
  TMP="$STATE.tmp.$$"; { echo RESTORED; REPS; } > "$TMP" && mv -f "$TMP" "$STATE"
  rm -f "$STATE"                # success: clear durable state so a later run captures fresh
fi

log "es4 clean reset COMPLETE — Kafka + Schema Registry wiped and re-seeded atomically; app restored"
