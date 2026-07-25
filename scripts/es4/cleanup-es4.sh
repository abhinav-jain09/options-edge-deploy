#!/usr/bin/env bash
# cleanup-es4.sh — ATOMIC es4 Kafka + Schema-Registry reset (the "cleanup v2").
#
# Runs ON the es4 box (rsync'd by es4-deploy, CLEAN_RESET action). The Jenkins stage MUST
# prove the cross-cluster prod `es-feed` producer cannot publish BEFORE invoking this — this script
# cannot reach the prod cluster. It refuses to run unless told so (ES4_FEED_FENCED=1).
# ⚠️ Since DBP-R33 (2026-07-25) the prod es-feed is RETIRED rather than fenced-and-restored, and the
# Jenkins stage proves its ABSENCE (scripts/es4/assert-prod-es-feed-absent.sh) before passing
# ES4_FEED_FENCED=1 — that flag is an assertion, so something must actually check it.
# es-feed is NEVER restored by this script — it is always left at 0, whatever the snapshot captured.
# It owns a Databento live session on a shared key and DBP-R22 reserves starting it to the separate
# activate action. This also removes the interrupted-run hazard: a durable snapshot holding a stale
# `es-feed 1` can no longer start a session nobody asked for. Re-start it with activate-es-feed.
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
# durable deployment + topic snapshots (survive interruption); single-instance lock;
# canonical-path-guarded deletes. Honors DEPLOY_DRY_RUN=true (no mutation).
set -euo pipefail

ES4_HOME=/home/es4
INFRA_DIR="$ES4_HOME/infra"
KAFKA_DATA=/home/es4/volumes/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY="${DEPLOY_DRY_RUN:-false}"
STATE="$ES4_HOME/.es4-cleanup.state"          # durable (NOT /tmp): original replicas for resume
LOCKDIR="$ES4_HOME/.es4-cleanup.lock"         # flock target (kernel-released on any exit)
KC="sudo -n /usr/local/bin/k3s kubectl -n options-edge"
# A clean reset has no exclusions: snapshot and stop every Deployment, including es-web.

log()  { printf '\n=== %s ===\n' "$*"; }
die()  { echo "CLEANUP ABORT: $*" >&2; exit 1; }
run()  { if [ "$DRY" = "true" ]; then echo "DRY: $*"; else eval "$*"; fi; }

# ------------------------------------------------------------ 0. PREFLIGHT (before any mutation)
log "preflight (no mutation happens unless every check passes)"
[ "${ES4_FEED_FENCED:-0}" = "1" ] || [ "$DRY" = "true" ] || \
  die "external prod es-feed not proven absent (ES4_FEED_FENCED!=1) — the Jenkins clean-reset stage must run scripts/es4/assert-prod-es-feed-absent.sh first (DBP-R33 deleted the prod es-feed; it is no longer scaled and restored), else a surviving external producer re-pollutes the fresh cluster with cached schema ids"
[ -d "$INFRA_DIR" ] || die "no $INFRA_DIR — run bootstrap-es4.sh (infra-sync) first"
[ -f "$INFRA_DIR/docker-compose.yml" ] || die "no compose file in $INFRA_DIR"
# canonical-path guard for the destructive rm: KAFKA_DATA must be the exact compose bind source,
# a real directory (not a symlink), and match the compose file — never delete anything else.
sudo -n test -d "$KAFKA_DATA" && ! sudo -n test -L "$KAFKA_DATA" || die "Kafka data dir $KAFKA_DATA missing or is a symlink"
CANON="$(sudo -n readlink -f "$KAFKA_DATA")"
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
  sudo -n test -d /var/log/pods || die "sudo -n cannot access /var/log/pods — service-log cleanup would fail"
  sudo -n test -d /var/log/containers || die "sudo -n cannot access /var/log/containers — service-log cleanup would fail"
  sudo -n test -d /var/lib/rancher/k3s/storage || die "sudo -n cannot access k3s local-path storage — Streams-state cleanup would fail"
fi

# single-instance lock via flock: kernel-released when this process exits FOR ANY REASON
# (including SIGKILL / reboot), so a hard-killed run never leaves a stale lock that blocks the
# documented resume — unlike a mkdir/rmdir lock that only clears via the EXIT trap.
if [ "$DRY" != "true" ]; then
  exec 9>"$LOCKDIR"
  flock -n 9 || die "another cleanup holds the lock ($LOCKDIR) — refusing concurrent reset"
fi

# ------------------------------------------------------------ 1. capture / resume replica state
# If a prior run was interrupted, $STATE already holds the ORIGINAL replica counts. Never replace
# those counts with current values: a re-capture could read scaled-to-0 Deployments and restore 0.
# Before resuming, reconcile the capture by name with the current cluster: preserve original counts
# for existing names, append services created after the capture using their current desired count,
# and drop services that no longer exist. This keeps every current producer inside the quiesce proof
# even when a WIPING file survives across deployments. A successful resume marks RESTORED and clears
# the state normally; blind/manual deletion of a WIPING capture is neither needed nor safe.
# ONE durable state file: line 1 = PHASE (WIPING|RESTORED), lines 2+ = "name replicas". Written
# ATOMICALLY (temp+mv) so the phase is always consistent with its content — no second marker file
# that could go stale and make a future reset silently skip the wipe. A resume keys off line 1.
REPS() { tail -n +2 "$STATE" | awk 'NF >= 2'; } # replica lines (skip phase header)
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
  log "resuming interrupted reset (phase=${ph:-WIPING}) — reconciling captured replicas with current Deployments"
  tail -n +2 "$STATE" | awk 'NF != 2 || ($2 != "<none>" && $2 !~ /^[0-9]+$/) { exit 1 }' \
    || die "invalid captured Deployment replica data — refusing to alter $STATE"
  CURRENT_DATA=$($KC get deploy -o custom-columns=NAME:.metadata.name,REPLICAS:.spec.replicas --no-headers) \
    || die "could not enumerate current Deployments while reconciling $STATE"
  [ "$(printf '%s\n' "$CURRENT_DATA" | awk 'NF >= 2 { count++ } END { print count+0 }')" -ge 1 ] \
    || die "no current Deployments found while reconciling $STATE"
  printf '%s\n' "$CURRENT_DATA" | awk 'NF != 2 || ($2 != "<none>" && $2 !~ /^[0-9]+$/) { exit 1 }' \
    || die "invalid current Deployment replica data — refusing to alter $STATE"
  if [ "$DRY" = "true" ]; then
    echo "DRY: reconciled WIPING state preview (durable state remains unchanged)"
    awk -f "$SCRIPT_DIR/reconcile-cleanup-state.awk" "$STATE" <(printf '%s\n' "$CURRENT_DATA")
  else
    TMP="$STATE.tmp.$$"
    awk -f "$SCRIPT_DIR/reconcile-cleanup-state.awk" "$STATE" <(printf '%s\n' "$CURRENT_DATA") > "$TMP" \
      || die "failed to reconcile current Deployments into $STATE"
    [ "$(wc -l < "$TMP")" -ge 2 ] || die "reconciled state contains no Deployments — refusing to replace $STATE"
    mv -f "$TMP" "$STATE"
    log "reconciled WIPING state now covers every current Deployment; original captured counts preserved"
  fi
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

# Snapshot real ES application topics before stopping Kafka. Kafka/SR internals, Kafka Streams
# changelog/repartition state, and old recursive es.es... MM2 heartbeats are deliberately excluded.
# Streams recreates its own internal topics after its local state has been wiped.


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
    bad=$($KC get deploy -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.replicas}{"\n"}{end}' 2>/dev/null | awk '$2!="" && $2!=0 {c++} END{print c+0}')
    # Require producer pods to be ACTUALLY DELETED — a Terminating pod still runs during its grace
    # period and could reconnect + write cached schema ids. Count anything not in a terminal phase
    # (Terminating pods are still present -> counted -> we keep waiting until they are gone).
    pods=$($KC get pods --no-headers 2>/dev/null | awk '$3!="Completed" && $3!="Succeeded" {c++} END{print c+0}')
    if [ "${bad:-1}" = "0" ] && [ "${pods:-1}" = "0" ]; then ok=true; break; fi
    sleep 5
  done
  [ "$ok" = true ] || die "app not fully quiesced (residual replicas/pods) — refusing to wipe with producers alive"
fi

# ------------------------------------------------------- 3. offline coordinated wipe (kafka down)
log "docker compose down (Kafka and all local infra stopped; Docker container logs removed)"
run "(cd '$INFRA_DIR' && docker compose down)"
log "wiping Kafka data volume CONTENTS ($KAFKA_DATA/* — all topics + _schemas), Kafka offline"
run "sudo -n find '$KAFKA_DATA' -mindepth 1 -maxdepth 1 -exec rm -rf {} +"

# EmptyDir state vanished with the pods. Empty each persistent Kafka Streams PVC in place so its
# claim remains bound. Refuse any PV path outside k3s local-path storage.
log "wiping persistent Kafka Streams state stores used by snapshotted services"
if [ "$DRY" = "true" ]; then
  echo "DRY: would resolve snapshot Deployment PVCs and empty their guarded local-path directories"
else
  while IFS= read -r claim; do
    [ -n "$claim" ] || continue
    pv=$($KC get pvc "$claim" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)
    [ -n "$pv" ] || die "PVC $claim has no bound PV"
    path=$(sudo -n /usr/local/bin/k3s kubectl get pv "$pv" -o jsonpath='{.spec.local.path}' 2>/dev/null || true)
    if [ -z "$path" ]; then
      path=$(sudo -n /usr/local/bin/k3s kubectl get pv "$pv" -o jsonpath='{.spec.hostPath.path}' 2>/dev/null || true)
    fi
    # k3s local-path storage lives under a root:root 0700 parent, so a non-root
    # `[ -d ]`/`cd` can't traverse it and would FALSE-abort ("missing or symlink")
    # even though the dir is a real directory. Probe as root (rm below already does).
    sudo -n test -d "$path" && ! sudo -n test -L "$path" || die "PV $pv path missing or symlink ($path)"
    canon=$(sudo -n readlink -f "$path")
    case "$canon" in /var/lib/rancher/k3s/storage/*) ;; *) die "PV $pv path outside guarded k3s storage root: $canon" ;; esac
    sudo -n find "$canon" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    echo "wiped state PVC $claim ($canon)"
  done < <(
    while read -r name _; do
      $KC get "deploy/$name" -o jsonpath='{range .spec.template.spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null || true
    done < <(REPS) | awk 'NF' | LC_ALL=C sort -u
  )
fi

log "deleting options-edge Kubernetes service logs"
run "sudo -n find /var/log/containers -mindepth 1 -maxdepth 1 -type l -name '*_options-edge_*.log' -delete"
run "sudo -n find /var/log/pods -mindepth 1 -maxdepth 1 -type d -name 'options-edge_*' -exec rm -rf {} +"
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

# ------------------------------------------------------------ 4. recreate snapshotted topics, THEN mm2
# TOPIC KNOWLEDGE LIVES IN scripts/kafka/topics.env — NOT in this script.
# Previously the reset snapshotted the live broker and replayed it, which meant the environment
# rebuilt whatever happened to exist, including orphan topics from services that do not run on es4
# (ibkr.*, hpsf.*, mission-control, strike-invasion, strike-sr, open-direction, volume-sandwich).
# Now the single source of truth is topics.env, applied by the SAME scripts/kafka/apply-topics.sh
# Jenkins uses, scoped to the es4 set. This script is deliberately DUMB about topics.
log "reconciling required core es.* topic contracts (create-es-topics.sh)"
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
    # es-feed is special: it owns a Databento live session on a shared API key, so its replica count
    # must never be raised by a restore. The snapshot is durable and survives an interrupted run, so
    # an earlier capture of 1 would otherwise be restored long after the operator took the feed down
    # — silently starting a Databento session nobody asked for. Requirement DBP-R21/R22.
    if [ "$name" = "es-feed" ]; then
      # es-feed is NEVER restored by a reset. It owns a Databento live session on a shared API key,
      # and DBP-R22 says only the separate activate action may raise it. Restoring a captured 1 --
      # or a stale 1 from a durable snapshot that survived an interrupted run -- would make
      # clean-reset an activation path, which is exactly the class of side effect that caused the
      # 2026-07-24 incident. After a reset, start it deliberately with ACTION=activate-es-feed.
      [ "$reps" = "0" ] || log "es-feed: captured replicas=$reps IGNORED — a reset never activates the feed (DBP-R22); use activate-es-feed"
      reps=0
    fi
    $KC scale "deploy/$name" --replicas="$reps" >/dev/null || { echo "restore scale FAILED for $name" >&2; fail=1; }
  done < <(REPS)
  # verify each Deployment's spec.replicas matches the captured value
  while read -r name reps; do
    [ -n "$name" ] || continue
    [ "$reps" = "<none>" ] && reps=1
    # same override as the scale loop above, or verification would compare against the stale capture
    if [ "$name" = "es-feed" ]; then reps=0; fi   # must match the restore loop above
    got=$($KC get "deploy/$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
    [ "$got" = "$reps" ] || { echo "restore MISMATCH $name: want $reps got $got" >&2; fail=1; }
  done < <(REPS)
  ready=false
  for _ in $(seq 1 60); do
    missing=0
    while read -r name reps; do
      [ "$reps" = "<none>" ] && reps=1
      if [ "$name" = "es-feed" ]; then reps=0; fi   # must match the restore loop above
      [ "$reps" -gt 0 ] || continue
      avail=$($KC get "deploy/$name" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)
      [ "${avail:-0}" -ge "$reps" ] || missing=$((missing + 1))
    done < <(REPS)
    if [ "$missing" = 0 ]; then ready=true; break; fi
    sleep 5
  done
  [ "$ready" = true ] || fail=1
  [ "$fail" = 0 ] || die "restore incomplete — leaving $STATE for a resume (rerun to finish restore)"
  # Atomically flip the SINGLE state file's phase to RESTORED, THEN unlink it. If killed between the
  # flip and the unlink, a resume reads phase=RESTORED and only clears state — it never re-wipes. There
  # is no second file to go stale, so a later fresh reset can never inherit a spurious RESTORED.
  TMP="$STATE.tmp.$$"; { echo RESTORED; REPS; } > "$TMP" && mv -f "$TMP" "$STATE"
  rm -f "$STATE" # success: next run captures fresh service/topic inventories
fi

if [ "$DRY" = "true" ]; then
  log "es4 clean reset DRY RUN COMPLETE — no services, data, state or logs were changed"
else
  log "es4 clean reset COMPLETE — service/topic snapshot restored after Kafka/SR, Streams state and logs were wiped"
fi
