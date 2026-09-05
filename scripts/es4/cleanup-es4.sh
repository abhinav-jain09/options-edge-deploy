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
# durable deployment snapshot (survives interruption); topics come from the topics.env SSOT,
#         NOT from a live-broker snapshot; single-instance lock;
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
# ⚠️ THIS is the cluster that actually gets wiped. The Jenkins stage verifies its ES4_KUBECONFIG,
# but the destructive work runs HERE over SSH against the host's own k3s context — a different
# credential entirely. Verifying the Jenkins-side kubeconfig therefore proves nothing about the
# target. Bind them: the caller passes the pinned UID and we refuse unless the local cluster matches.
if [ "${ES4_EXPECTED_NS_UID:-}" != "" ]; then
  # Capture the status explicitly. `$( ... || echo "" )` masks it: a kubectl that exits non-zero but
  # still writes to stdout would yield a non-empty value and could compare equal, letting a failed
  # safety-critical query authorise a wipe. Same class of masking as the pod-count and grep-pipeline
  # defects found earlier in this review.
  set +e
  actual_uid=$($KC get namespace options-edge -o jsonpath='{.metadata.uid}' 2>&1)
  uid_rc=$?
  set -e
  [ "$uid_rc" -eq 0 ] || die "cannot read the local options-edge namespace UID (rc=$uid_rc: $actual_uid) — refusing to wipe an unidentified cluster"
  [ -n "$actual_uid" ] || die "empty local options-edge namespace UID — refusing to wipe an unidentified cluster"
  case "$actual_uid" in *[!0-9a-fA-F-]*) die "implausible local namespace UID '$actual_uid' — refusing to wipe";; esac
  [ "$actual_uid" = "$ES4_EXPECTED_NS_UID" ] || die "LOCAL CLUSTER IS NOT es4: options-edge namespace UID is $actual_uid, expected $ES4_EXPECTED_NS_UID — refusing to wipe"
  log "local cluster identity confirmed ($actual_uid)"
elif [ "$DRY" != "true" ]; then
  die "ES4_EXPECTED_NS_UID not supplied — a destructive run must bind to a verified cluster identity"
fi
[ "${ES4_FEED_FENCED:-0}" = "1" ] || [ "$DRY" = "true" ] || \
  die "external prod es-feed not proven absent (ES4_FEED_FENCED!=1) — the Jenkins clean-reset stage must run scripts/es4/assert-prod-es-feed-absent.sh first (DBP-R33 deleted the prod es-feed; it is no longer scaled and restored), else a surviving external producer re-pollutes the fresh cluster with cached schema ids"
[ -d "$INFRA_DIR" ] || die "no $INFRA_DIR — run bootstrap-es4.sh (infra-sync) first"
[ -f "$INFRA_DIR/docker-compose.yml" ] || die "no compose file in $INFRA_DIR"

# --- broker-contract drift guard (2026-07-31) --------------------------------------------
# The live compose is $INFRA_DIR/docker-compose.yml, but es4-deploy rsyncs the repo to
# $ES4_HOME/repo/infra/es4/. NOTHING runs the rsynced copy, so repo changes to the broker
# contract silently never reach the broker: PR #656 set AUTO_CREATE_TOPICS_ENABLE=false in
# the repo on 2026-07-12 and the LIVE broker was still "true" on 2026-07-31 — which let the
# broker keep re-creating exact-partition topics mid-clean (three failed clean-resets) and let
# Kafka Streams fall back to num.partitions=4 for 32-partition apps
# ("invalid partitions: expected: 32; actual: 4").
# Compare the keys that decide topic shape and FAIL LOUD rather than wipe against a stale
# contract. Warn-only (never blocks) for keys the repo copy does not carry.
REPO_COMPOSE="$ES4_HOME/repo/infra/es4/docker-compose.yml"
if [ -f "$REPO_COMPOSE" ]; then
  for key in KAFKA_AUTO_CREATE_TOPICS_ENABLE KAFKA_NUM_PARTITIONS; do
    live=$(grep -E "^[[:space:]]*$key:" "$INFRA_DIR/docker-compose.yml" | head -1 | sed -E 's/.*:[[:space:]]*//; s/[[:space:]]*#.*//; s/"//g' | tr -d ' ')
    repo=$(grep -E "^[[:space:]]*$key:" "$REPO_COMPOSE"                 | head -1 | sed -E 's/.*:[[:space:]]*//; s/[[:space:]]*#.*//; s/"//g' | tr -d ' ')
    [ -n "$repo" ] || continue
    if [ "$live" != "$repo" ]; then
      log "  BROKER CONTRACT DRIFT: $key live='$live' repo='$repo'"
      log "  live=$INFRA_DIR/docker-compose.yml  repo=$REPO_COMPOSE"
      die "es4 broker contract has drifted from the repo — reconcile before wiping (a clean against a stale contract recreates topics with the wrong shape)"
    fi
  done
  log "broker contract matches the repo (auto-create + num.partitions)"
else
  log "  WARNING: $REPO_COMPOSE absent — cannot verify the live broker contract against the repo"
fi
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
    #
    # ⚠️ But that prior run may have executed an OLDER build of this script, which restored es-feed to
    # its captured count — possibly 1. Exiting here without checking would let an interrupted reset
    # leave a Databento session running that DBP-R22 says only ACTION=activate-es-feed may start.
    # So enforce the invariant on this path too before clearing state.
    if [ "$DRY" = "true" ]; then
      echo "DRY: would force deploy/es-feed to 0 (DBP-R22) and clear $STATE"
    else
      # Existence query: capture the exit status directly. `$KC ... | grep -q .` masks kubectl's
      # status behind grep's, so an API failure would have read as "no Deployment" and skipped the
      # enforcement entirely.
      set +e
      es_out=$($KC get deploy es-feed --ignore-not-found -o name 2>&1)
      es_rc=$?
      set -e
      [ "$es_rc" -eq 0 ] || die "phase=RESTORED but the es-feed Deployment query failed (rc=$es_rc: $es_out) — refusing to clear state while the feed's state is unknown"
      if [ -n "$es_out" ]; then
        cur=$($KC get deploy es-feed -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
        [ "$cur" != "ERR" ] || die "phase=RESTORED but es-feed replicas are unreadable — refusing to clear state while the feed's state is unknown"
        if [ "$cur" != "0" ]; then
          log "phase=RESTORED left es-feed at $cur (an older build restored the captured count) — forcing 0; use activate-es-feed to start it"
          $KC scale deploy/es-feed --replicas=0 >/dev/null || die "could not force es-feed to 0 on the RESTORED resume path"
          # VERIFY the scale actually took: an unverified scale is not an enforcement.
          after=$($KC get deploy es-feed -o jsonpath='{.spec.replicas}' 2>/dev/null || echo ERR)
          [ "$after" = "0" ] || die "es-feed still at '$after' after scaling to 0 on the RESTORED path"
        fi
      fi
      # A missing Deployment does not mean nothing is running: pods can outlive it, and a terminating
      # pod still produces. Check regardless of whether the Deployment existed.
      set +e
      pods_out=$($KC get pods -l app.kubernetes.io/name=es-feed -o name 2>&1)
      pods_rc=$?
      set -e
      # `-o name` prints nothing at all for an empty result, so the informational "No resources
      # found" line that --no-headers emits on stderr can no longer be miscounted as a pod. And any
      # non-zero rc is a real failure: accepting one because its text happened to contain "no
      # resources found" masked a safety-critical query.
      [ "$pods_rc" -eq 0 ] || die "phase=RESTORED but the es-feed pod query failed (rc=$pods_rc: $pods_out) — failing closed"
      n=$(printf '%s\n' "$pods_out" | awk '/^pod\// {c++} END {print c+0}')
      [ "$n" = "0" ] || die "phase=RESTORED but $n es-feed pod(s) are still present — refusing to clear state while a Databento session may be live"
      rm -f "$STATE"
    fi
    log "prior run already restored the app (phase=RESTORED); leftover state cleared, NO wipe"
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

# NOTE: topics are NOT snapshotted from the live broker. They are reconciled from the topics.env
# SSOT in step 4, so undeclared topics that happen to exist are NOT preserved across a reset.
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

# ------------------------------------------------- 4. reconcile topics from topics.env, THEN mm2
# TOPIC KNOWLEDGE LIVES IN scripts/kafka/topics.env — NOT in this script.
# Previously the reset snapshotted the live broker and replayed it, which meant the environment
# rebuilt whatever happened to exist, including orphan topics from services that do not run on es4
# (ibkr.*, hpsf.*, mission-control, strike-invasion, strike-sr, open-direction, volume-sandwich).
# Now the single source of truth is topics.env, applied by the SAME scripts/kafka/apply-topics.sh
# Jenkins uses, scoped to the es4 set. This script is deliberately DUMB about topics.
log "reconciling required core es.* topic contracts (create-es-topics.sh)"
# A clean-reset IS the "approved destructive cleanup deployment" that
  # KAFKA_RECREATE_MISMATCHED_TOPICS gates: the wipe just deleted every topic, so anything
  # that reappeared before this line was AUTO-CREATED by a racing client (a consumer's
  # metadata request, MM2, or prod's reversal-confirmation-service) with broker-default
  # partitions instead of the contract's. Without the flag create-es-topics.sh REFUSES to
  # repair those and exits non-zero, so the reconcile stops at the FIRST mismatched topic
  # and every contract after it is never created — services then boot against a missing
  # topic, die in main() and sit 0/1 forever (observed: strike-intelligence dying on
  # "cannot enforce delete/<=1d retention on es.strike-intelligence-by-strike ->
  # UnknownTopicOrPartitionException", taking the whole strike-flow chain down).
  run "KAFKA_RECREATE_MISMATCHED_TOPICS=true bash '$SCRIPT_DIR/create-es-topics.sh'"
# Proves reconciliation actually produced the declared world: every declared topic exists and none
# is NARROWER than declared (a client that raced in ahead of apply-topics). It deliberately does NOT
# claim to validate the partition counts themselves — these topics were just created FROM
# topics.env, so comparing them back to it is close to a tautology, and it would have passed at
# 19:19 on 2026-08-02 with `tcbbo:4` in the file. The check that catches an UNDER-DECLARED contract
# is the steady-state audit after the apps are up (see below), where the owners' own contracts make
# the broker an independent source of truth.
log "verifying declared topics were created (missing/narrower are fatal; apps still down)"
run "bash '$SCRIPT_DIR/verify-topic-partition-contract.sh' created"
log "docker compose up -d (start mm2 + any remaining infra now that topics exist)"
run "(cd '$INFRA_DIR' && docker compose up -d)"

# ------------------------------------------------- 5. restore app (verified) then clear state
# Marks the start of the window the post-restore wedge scan reads logs over. SECONDS is bash's own
# monotonic counter since this shell started — no clock, timezone or container-TZ arithmetic.
RESTORE_T0=$SECONDS
log "restoring es4 Deployments to captured replica counts (verified) from $STATE"
if [ "$DRY" = "true" ]; then
  echo "DRY: would restore replicas from $STATE and verify (es-feed always forced to 0 — DBP-R22)"
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

  # SELF-HEAL boot-order stragglers (parity with morning-autostart and dev-cleanup).
  # Services restarted by the restore race Kafka/SR/topic creation; the losers die in
  # main() while their container stays Running, so kubelet never restarts them and they
  # sit 0/1 FOREVER — the fleet looks restored while a whole chain is dead. By now the
  # infra and every topic contract exist, so a bounce succeeds.
  log "self-healing boot-order stragglers"
  healed=0
  healed_names=""
  for d in $($KC get deploy -o name 2>/dev/null | sed 's#.*/##'); do
    des=$($KC get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null)
    rr=$($KC get deploy "$d" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
    if [ "${des:-0}" -gt 0 ] && [ "${rr:-0}" -lt "${des:-0}" ]; then
      log "  boot-order straggler: $d (${rr:-0}/${des}) — restarting"
      # Record WHICH ones were bounced: the post-restore wait must follow those exact rollouts,
      # not a fleet-wide availability count that old pods can satisfy on their own.
      $KC rollout restart deploy/"$d" >/dev/null 2>&1 && { healed=$((healed+1)); healed_names="$healed_names $d"; }
    fi
  done
  log "self-heal restarted ${healed} straggler(s)"

  # ⭐POST-RESTORE AUDITS. Everything above is satisfied by a Kafka Streams app whose StreamThreads
  # have all died: the health port keeps answering, availableReplicas stays at desired, restarts stay
  # 0, and the app processes nothing forever. The 2026-08-02 reset ended "23/23 ready" with
  # strike-flow-classifier in exactly that state. Two independent checks run here, and BOTH are
  # fatal — a reset that knowingly leaves a dead pipeline must not report green.
  #
  # The state file was already cleared above, so exiting non-zero here cannot make a resume
  # destructive. It does mean a blind rerun would re-WIPE, which is why the messages say plainly:
  # repair the named app, do NOT rerun clean-reset.
  #
  # Neither audit means anything until the apps the self-heal just restarted are actually back and
  # have applied their own topic contracts. Elapsed time alone does not establish that, so WAIT ON
  # THE ROLLOUTS first and only then add a short settle for the group join that follows readiness.
  post_fail=0
  settle="${ES4_POST_RESTORE_SETTLE_SECONDS:-90}"
  case "$settle" in
    ''|*[!0-9]*) die "ES4_POST_RESTORE_SETTLE_SECONDS must be a non-negative integer (got '$settle') — the reset itself COMPLETED and state is cleared; re-run only the post-restore audits" ;;
  esac
  [ "$settle" -le 3600 ] || die "ES4_POST_RESTORE_SETTLE_SECONDS=$settle exceeds the 3600s bound — the reset itself COMPLETED and state is cleared; fix the value and re-run only the post-restore audits, do NOT rerun clean-reset"

  if [ "$healed" -gt 0 ]; then
    # `rollout status` on the NAMED deployments, not a fleet-wide availableReplicas poll. During a
    # rolling restart the OLD ReplicaSet can keep availableReplicas at desired while the new one is
    # stalled, so that poll would return "ready" instantly and the audits would run against pods
    # that never applied their contracts. rollout status tracks the restart's own generation and
    # exits non-zero on timeout, which is the fail-closed behaviour this needs.
    rollout_wait="${ES4_ROLLOUT_WAIT_SECONDS:-300}"
    # Validated for the same reason as the settle: `0` (or a negative) tells kubectl to wait
    # FOREVER, which turns this gate into a hang, and a bare non-numeric would abort with no
    # diagnostic after the state file is already gone.
    case "$rollout_wait" in
      ''|*[!0-9]*) die "ES4_ROLLOUT_WAIT_SECONDS must be a positive integer (got '$rollout_wait') — the reset itself COMPLETED and state is cleared; fix the value and re-run only the post-restore audits, do NOT rerun clean-reset" ;;
    esac
    { [ "$rollout_wait" -ge 1 ] && [ "$rollout_wait" -le 1800 ]; } \
      || die "ES4_ROLLOUT_WAIT_SECONDS=$rollout_wait outside 1..1800 (0 would make kubectl wait forever) — the reset itself COMPLETED and state is cleared; do NOT rerun clean-reset"
    log "waiting up to ${rollout_wait}s for the rollout of:${healed_names}"
    for d in $healed_names; do
      $KC rollout status deploy/"$d" --timeout="${rollout_wait}s" >/dev/null 2>&1 \
        || die "rollout of $d did not complete — the post-restore audits below cannot mean anything until it does. The reset itself COMPLETED and state is cleared: investigate $d, then re-run the audits; do NOT rerun clean-reset"
    done
  fi
  log "settling ${settle}s so restarted apps can rejoin their consumer groups"
  sleep "$settle"

  # (1) Steady-state contract audit. topics.env is the EXACT desired shape for es4 (a reset recreates
  # the world from it, so anything the file does not say is not preserved). A live topic that differs
  # from its declaration is therefore a DISAGREEMENT that has to be adjudicated, not a shrug: either
  # the widening was deliberate — in which case the file must record it or the next reset silently
  # drops it back and rebuilds the 2026-08-02 wedge — or it was not, in which case the topic is
  # wrong. Both remedies are printed; the audit does not guess which one applies.
  log "auditing steady-state topic partition contract (any disagreement is fatal)"
  if ! run "bash '$SCRIPT_DIR/verify-topic-partition-contract.sh' steady"; then
    post_fail=1
  fi

  # (2) Green-pod / dead-topology scan. k8s cannot see this at all, but Streams says it in one
  # unmistakable line. Read per POD (a Deployment's `logs` reaches one pod only), keep the command's
  # exit status, and never let a failed log read pass as "no wedge".
  # Cover everything since the restore began, plus a minute of slack. RESTORE_T0 is bash's own
  # SECONDS counter, so this needs no clock arithmetic and cannot be skewed by the host's timezone.
  log "scanning for wedged Streams topologies (green pod, dead threads; window opens at restore)"
  wedged=""
  scan_errors=""
  # Pod DISCOVERY is evidence too. `for p in $(kubectl get pods ...)` swallows an RBAC/API/transport
  # failure into an empty word list, and `set -e` does not fire on a substitution that only supplies
  # loop words — the scan would then find nothing and report "no wedged topologies detected".
  # --request-timeout on BOTH kubectl calls: it defaults to no timeout, so an API-server, kubelet
  # or transport stall would hang this gate forever instead of failing it.
  set +e
  pod_list="$($KC get pods --field-selector=status.phase=Running -o name --request-timeout=30s 2>/dev/null)"
  pods_status=$?
  set -e
  [ "$pods_status" -eq 0 ] || die "cannot list pods to scan for wedged topologies (kubectl exited $pods_status) — the reset itself COMPLETED and state is cleared; do NOT rerun clean-reset, re-run the audit"
  pod_list="$(printf '%s\n' "$pod_list" | sed 's#.*/##')"
  [ -n "$pod_list" ] || die "pod list came back EMPTY while deployments are running — treating as an unreadable cluster, not as 'nothing wedged'"
  for p in $pod_list; do
    # Capture the log body and its EXIT STATUS separately. `kubectl logs | grep -q` is wrong twice
    # over: -q closes the pipe early so kubectl dies of SIGPIPE and, under `set -o pipefail`, a real
    # MATCH is reported as failure; and a kubectl error would be indistinguishable from "no match".
    # TIME-bounded, not line-bounded. The StreamsException is emitted once, during the first
    # assignment after startup; a chatty app can push it past any fixed --tail long before the scan
    # runs. --since covers the whole restore window, and --limit-bytes truncates from the START of
    # that window, which is exactly where the fatal line lives.
    # RECOMPUTED per pod: --since is relative to the instant THAT request runs, so one window
    # computed up front would creep forward with every sequential read and could drop the fatal
    # line for the pods scanned last.
    scan_window=$(( SECONDS - RESTORE_T0 + 60 ))
    set +e
    pod_logs="$($KC logs "$p" --all-containers --since="${scan_window}s" --limit-bytes=8000000 --request-timeout=60s 2>/dev/null)"
    logs_status=$?
    set -e
    if [ "$logs_status" -ne 0 ]; then
      scan_errors="$scan_errors $p"
    else
      # Pure-bash match: no pipeline, so nothing here can be confused by SIGPIPE or pipefail.
      case "$pod_logs" in
        *"invalid partitions: expected"*) wedged="$wedged $p" ;;
      esac
    fi
  done
  if [ -n "$wedged" ]; then
    echo "WEDGED STREAMS TOPOLOGIES (pods are Ready but process nothing):$wedged" >&2
    echo "REPAIR EACH ONE — do NOT rerun clean-reset (that would wipe again for no reason):" >&2
    echo "  scale that app to 0 -> delete ONLY the topics whose names start with its Streams" >&2
    echo "  application.id (its *-repartition and *-changelog topics; never a broker-wide wildcard)" >&2
    echo "  -> scale back to 1. Kafka cannot shrink a topic, so there is no gentler path." >&2
    echo "Then correct the under-declared source topic in topics.env, or the next reset rebuilds it." >&2
    post_fail=1
  fi
  # Reported INDEPENDENTLY of the wedge list: a run that finds one wedge and cannot read three other
  # pods has an incomplete repair picture, and hiding the unreadable ones behind the wedges would
  # make the operator think the named list is the whole job.
  if [ -n "$scan_errors" ]; then
    echo "WEDGE SCAN INCONCLUSIVE — could not read logs for:$scan_errors" >&2
    echo "An unreadable pod is an UNKNOWN, not a pass. Re-run the scan or check these by hand." >&2
    post_fail=1
  fi
  if [ -z "$wedged" ] && [ -z "$scan_errors" ]; then
    log "  no wedged topologies detected"
  fi

  [ "$post_fail" = 0 ] || die "post-restore audit FAILED — the reset finished and state is already cleared, so fix what is named above; do NOT rerun clean-reset"
fi

if [ "$DRY" = "true" ]; then
  log "es4 clean reset DRY RUN COMPLETE — no services, data, state or logs were changed"
else
  log "es4 clean reset COMPLETE — service snapshot restored and topics reconciled from topics.env after Kafka/SR, Streams state and logs were wiped"
fi
