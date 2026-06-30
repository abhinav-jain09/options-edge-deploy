#!/usr/bin/env bash
# Off-hours CLEAN-SLATE wipe for OptionsEdge (dev + prod).
#
# OptionsEdge only needs data DURING the market session. Outside it (after close,
# before open, weekends, holidays) the env must hold a NEAR-ZERO footprint to fit
# a hard memory/disk budget. This single gated script, when explicitly enabled,
# brings the env to that near-zero state. It is the AGGRESSIVE successor to the
# gentle kafka-changelog-cleanup (24h trim) and daily-kafka-reset (keep-real-data)
# jobs — here `delete` means DELETE: no compaction sparing, no 24h retention.
#
# Design doc (authoritative): options-edge-documents/infra/OFFHOURS-CLEAN-SLATE-DESIGN.md
# Sibling reused patterns: scripts/ops/premarket-reset.sh, scripts/ops/daily-kafka-reset.sh
#
# WHAT IT DOES (each step a strict no-op under DRY_RUN):
#   A. Kafka
#      - DELETE every *-changelog and *-repartition topic (incl. compact ones —
#        nothing is spared), so the apps rebuild empty state stores on restart.
#      - PURGE every remaining data topic to 0 records via delete-records to the
#        high-watermark (keeps topic shape: partitions + retention config).
#      - PRESERVE exactly the 3 cluster-system topics: __consumer_offsets,
#        __transaction_state, _schemas. (Deleting these breaks the cluster.)
#   B. Streams local state (on-disk RocksDB / the <svc>-streams-state PVCs)
#      - For every Streams app: scale 0 -> delete+recreate its PVC -> scale 1, via
#        the jenkins-deployer SA. A changelog-only wipe CRASHES apps against
#        freshly-emptied changelogs (BufferUnderflowException / codec mismatch),
#        so clearing the local state is MANDATORY, not optional.
#   C. Postgres (flow DB `options_flow` ONLY — Keycloak is NEVER touched)
#      - TRUNCATE every table in the public schema (incl. pin_session_close), then
#        CHECKPOINT (+ VACUUM) to recycle WAL and actually return disk.
#   D. Logs + docker disk (truncate-in-place, NEVER rm)
#      - Kafka broker server.log, Postgres server logs, docker container json-logs
#        (dev only): `: > file`. Plus docker system prune + registry GC on dev.
#
# HARD SAFETY RAILS (see the design doc §5 / the prompt):
#   1. DRY_RUN defaults to TRUE. With DRY_RUN=true ZERO destructive actions run —
#      it only LOGS what it WOULD do, with counts. Destruction needs DRY_RUN=false.
#   2. WIPE_ENABLED is a second latch — even with DRY_RUN=false, nothing mutates
#      unless WIPE_ENABLED=true (mirrors premarket-reset's RESET_ENABLED).
#   3. MARKET-HOURS GUARD: refuses to run during the session (09:30 ET .. close +
#      buffer), fail-closed, using the vendored MarketCalendar. On a non-trading
#      day a LIVE wipe still proceeds (the point is to stay empty over weekends/
#      holidays) but a session-hours run is ALWAYS refused.
#   4. FAIL-CLOSED: aborts untouched if Kafka OR k8s OR (for a live wipe) the DB is
#      unreachable, or identity guards fail. Single-instance lock.
#   5. Keep-list is sacred: the 3 system topics and Keycloak's DB/pod are asserted
#      and can never be deleted/truncated.
#   6. Idempotent + heavily logged; one-line Discord summary.
set -uo pipefail

# ===========================================================================
# Config (override via env)
# ===========================================================================
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="${K8S_NAMESPACE:-options-edge}"
SELECTOR="${APP_SELECTOR:-app.kubernetes.io/part-of=options-edge}"
# The jenkins-deployer SA is the ONLY principal the cluster's admission policy
# lets mutate PVCs / scale Deployments. --as runs every PVC/scale op as it.
KUBECTL_AS="${KUBECTL_AS:-system:serviceaccount:options-edge:jenkins-deployer}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-240}"
SCALEDOWN_WAIT="${SCALEDOWN_WAIT:-90}"
PVC_RECREATE_WAIT="${PVC_RECREATE_WAIT:-60}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$SCRIPT_DIR/../jenkins" && pwd 2>/dev/null || echo "$SCRIPT_DIR/../jenkins")}"

DRY_RUN="${DRY_RUN:-true}"
WIPE_ENABLED="${WIPE_ENABLED:-false}"

# Market-hours guard: refuse to run between open and close+buffer. The calendar
# already knows the per-day close (incl. early-close days); we add a buffer so the
# 16:15 ET options close (calendar NORMAL_CLOSE is the 16:00 equity close) and any
# late settle is fully past before we are willing to wipe.
CLOSE_BUFFER_MIN="${CLOSE_BUFFER_MIN:-30}"   # minutes after the calendar close before off-hours begins

# Environment identity (fail-closed for a LIVE wipe). Identity is the Kafka
# cluster-id + DB host + DB name + connected role — NOT a free-text env string.
EXPECTED_ENV="${EXPECTED_ENV:-prod}"                        # label only
EXPECTED_KAFKA_CLUSTER_ID="${EXPECTED_KAFKA_CLUSTER_ID:-}"  # REQUIRED for live wipe
EXPECTED_DB="${EXPECTED_DB:-options_flow}"
EXPECTED_DB_HOST="${EXPECTED_DB_HOST:-}"                    # REQUIRED for live DB wipe
PG_DSN="${PG_DSN:-}"                                        # flow-DB superuser/owner DSN (REQUIRED for live DB wipe)

# Keycloak — asserted NEVER-TOUCH. The wipe DSN must not point at it; its DB name
# and pod are hard-denied below.
KEYCLOAK_DB_NAMES="${KEYCLOAK_DB_NAMES:-keycloak}"          # space-separated DB names we refuse to wipe
KEYCLOAK_POD="${KEYCLOAK_POD:-oe-keycloak-postgres-0}"     # never scaled/cleared

# Log truncation targets (host paths). Empty -> that class is skipped (logged).
KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-/opt/kafka/logs}"          # server.log, controller.log, state-change.log
PG_LOG_DIR="${PG_LOG_DIR:-}"                               # flow PG server log dir (e.g. /var/lib/pgsql/data/log)
DOCKER_CONTAINER_LOG_GLOB="${DOCKER_CONTAINER_LOG_GLOB:-}" # dev only, e.g. /var/lib/docker/containers/*/*-json.log
RECLAIM_DOCKER="${RECLAIM_DOCKER:-false}"                  # dev only: docker system prune + registry GC
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-registry}"      # local-registry container name for GC

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

KT="$KAFKA_BIN/kafka-topics.sh"
KGO="$KAFKA_BIN/kafka-get-offsets.sh"
KDR="$KAFKA_BIN/kafka-delete-records.sh"
KCG="$KAFKA_BIN/kafka-consumer-groups.sh"
KCL="$KAFKA_BIN/kafka-cluster.sh"

# ===========================================================================
# The SACRED keep-list — never delete these Kafka system topics.
# ===========================================================================
# Exact-match list (NOT a prefix): only these three survive the topic sweep.
SYSTEM_TOPICS="__consumer_offsets __transaction_state _schemas"
is_system_topic() {
  local t="$1"
  case " $SYSTEM_TOPICS " in *" $t "*) return 0 ;; *) return 1 ;; esac
}

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] [offhours] $*"; }
die() { log "FATAL: $*"; discord "❌ Off-hours clean-slate ABORTED ($EXPECTED_ENV): $*"; exit 1; }
discord() {
  [ -n "$DISCORD_WEBHOOK_URL" ] || return 0
  command -v curl >/dev/null 2>&1 || return 0
  # URL via stdin config (-K -), not argv, so the secret stays out of `ps`.
  printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
    | curl -fsS --max-time 10 -K - -H 'Content-Type: application/json' \
        -d "{\"username\":\"OptionsEdge Ops\",\"content\":\"$1\"}" >/dev/null 2>&1 \
    || log "WARN: Discord post failed"
}
pg() { psql "$PG_DSN" -v ON_ERROR_STOP=1 -tAc "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }
# kubectl impersonating the jenkins-deployer SA (PVC/scale ops are policy-gated).
kc() { kubectl -n "$NS" --as="$KUBECTL_AS" "$@"; }
# read-only kubectl (no impersonation needed for gets; keeps reads working even if
# the impersonation grant is narrow).
kcr() { kubectl -n "$NS" "$@"; }

MUTATE="true"; [ "$DRY_RUN" = "true" ] && MUTATE="false"

# ===========================================================================
# Single-instance lock
# ===========================================================================
LOCK="/tmp/offhours-clean-slate.lock"
if have flock; then
  exec 9>"$LOCK" || exit 1
  flock -n 9 || { log "another off-hours wipe is already running; exiting"; exit 0; }
else
  log "WARN: flock not available — single-instance lock skipped"
fi

log "=== off-hours clean-slate start (env=$EXPECTED_ENV DRY_RUN=$DRY_RUN WIPE_ENABLED=$WIPE_ENABLED) ==="
START_TS=$(date +%s)

# ===========================================================================
# 0. MARKET-HOURS GUARD (fail-closed). ALWAYS run, even in DRY_RUN.
# ===========================================================================
# Refuse to run during the session. A non-trading day is fine (we want it empty);
# only being INSIDE 09:30..close+buffer on a trading day blocks the wipe.
have python3 || die "python3 required for the market-hours guard"
GUARD=$(CALENDAR_DIR="$CALENDAR_DIR" CLOSE_BUFFER_MIN="$CLOSE_BUFFER_MIN" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
try:
    from market_calendar import MarketCalendar, MARKET_OPEN
except Exception as e:
    print(f"ERR calendar import failed: {e}"); sys.exit(0)

cal = MarketCalendar()
buf = timedelta(minutes=int(os.environ.get("CLOSE_BUFFER_MIN", "30")))
tz = ZoneInfo("America/New_York")
now = datetime.now(tz)
day = now.date()
td = cal.current_trading_date(now).strftime("%Y%m%d")

if not cal.is_trading_day(day):
    # Non-trading day -> always off-hours -> safe to wipe.
    print(f"OFFHOURS non-trading-day td={td}")
    sys.exit(0)

open_dt = datetime.combine(day, MARKET_OPEN, tz)
close_dt = datetime.combine(day, cal.close_time(day), tz) + buf
if open_dt <= now < close_dt:
    print(f"SESSION inside-session now={now.strftime('%H:%M')} close+buf={close_dt.strftime('%H:%M')} td={td}")
else:
    when = "pre-open" if now < open_dt else "post-close"
    print(f"OFFHOURS {when} now={now.strftime('%H:%M')} td={td}")
PY
)
log "market-hours guard: $GUARD"
case "$GUARD" in
  OFFHOURS*) : ;;                                  # safe to proceed
  SESSION*)  die "refusing to run DURING market hours ($GUARD)" ;;
  *)         die "market-hours guard inconclusive ($GUARD) — fail-closed" ;;
esac

# ===========================================================================
# 1. Reachability + identity guards (fail-closed). Read-only — always run.
# ===========================================================================
[ -x "$KT" ] || have "$KT" || die "kafka CLI missing at $KAFKA_BIN"
"$KT" --bootstrap-server "$BOOTSTRAP" --list >/tmp/ohcs-all-topics.txt 2>/dev/null \
  || die "cannot reach Kafka at $BOOTSTRAP"
kcr get deploy >/dev/null 2>&1 || die "cannot reach k8s namespace $NS"

# Kafka cluster-id identity (REQUIRED before a live wipe — a wrong BOOTSTRAP
# cannot wipe another cluster).
if [ -n "$EXPECTED_KAFKA_CLUSTER_ID" ] && [ -x "$KCL" ]; then
  CID=$("$KCL" cluster-id --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -oE 'Cluster ID:.*' | awk '{print $3}')
  [ "$CID" = "$EXPECTED_KAFKA_CLUSTER_ID" ] || die "Kafka cluster-id mismatch (got '$CID', expected '$EXPECTED_KAFKA_CLUSTER_ID')"
  log "kafka cluster-id verified ($CID)"
elif [ "$MUTATE" = "true" ]; then
  die "EXPECTED_KAFKA_CLUSTER_ID unset — refusing to wipe without cluster identity"
fi

# DB identity (REQUIRED before a live DB wipe). Verify host + db name + that the
# DSN does NOT point at Keycloak.
DB_WIPE="false"
if [ -n "$PG_DSN" ]; then
  have psql || die "psql missing but PG_DSN set — cannot verify/wipe DB"
  # Hard-deny: the DSN must not name a Keycloak DB.
  for kdb in $KEYCLOAK_DB_NAMES; do
    case "$PG_DSN" in *"dbname=$kdb"*|*"/$kdb"*|*" $kdb "*)
      die "PG_DSN appears to target Keycloak DB '$kdb' — REFUSING (Keycloak is never touched)" ;;
    esac
  done
  DBINFO=$(pg "select current_database()||'|'||coalesce(host(inet_server_addr()),'')||'|'||current_user") \
    || die "Postgres unreachable / identity query failed"
  DBN=${DBINFO%%|*}; _rest=${DBINFO#*|}; SRV=${_rest%%|*}; USR=${_rest##*|}
  for kdb in $KEYCLOAK_DB_NAMES; do
    [ "$DBN" = "$kdb" ] && die "connected database is Keycloak DB '$kdb' — REFUSING"
  done
  [ "$DBN" = "$EXPECTED_DB" ] || die "DB name mismatch (got '$DBN', expected '$EXPECTED_DB')"
  if [ "$MUTATE" = "true" ]; then
    [ -n "$EXPECTED_DB_HOST" ] || die "EXPECTED_DB_HOST unset — refusing DB wipe without host identity"
    case "$PG_DSN" in *"$EXPECTED_DB_HOST"*) : ;; *) die "PG_DSN host does not contain EXPECTED_DB_HOST=$EXPECTED_DB_HOST" ;; esac
    [ -z "$SRV" ] || [ "$SRV" = "$EXPECTED_DB_HOST" ] || die "DB server addr mismatch (got '$SRV', expected '$EXPECTED_DB_HOST')"
  fi
  log "Postgres identity verified (db=$DBN host=${SRV:-socket} role=$USR)"
  DB_WIPE="true"
else
  if [ "$MUTATE" = "true" ]; then
    log "WARN: PG_DSN unset — DB wipe (step C) will be SKIPPED this run"
  else
    log "PG_DSN unset — DB wipe (step C) would be SKIPPED"
  fi
fi

# Second latch for any live mutation.
if [ "$MUTATE" = "true" ] && [ "$WIPE_ENABLED" != "true" ]; then
  die "WIPE_ENABLED!=true — guards passed but mutation not enabled (set WIPE_ENABLED=true AND DRY_RUN=false to wipe)"
fi
log "guards PASSED (mutate=$MUTATE db_wipe=$DB_WIPE)"

# ===========================================================================
# Build the topic plan (classification only — no mutation yet).
# ===========================================================================
STATE_TOPICS=""   # *-changelog / *-repartition -> DELETE
PURGE_TOPICS=""   # everything else non-system -> delete-records to 0
KEEP_SYS=0
while read -r t; do
  [ -n "$t" ] || continue
  if is_system_topic "$t"; then KEEP_SYS=$((KEEP_SYS+1)); continue; fi
  case "$t" in
    *-changelog|*-repartition) STATE_TOPICS="$STATE_TOPICS $t" ;;
    *)                         PURGE_TOPICS="$PURGE_TOPICS $t" ;;
  esac
done </tmp/ohcs-all-topics.txt
N_STATE=$(echo $STATE_TOPICS | wc -w | tr -d ' ')
N_PURGE=$(echo $PURGE_TOPICS | wc -w | tr -d ' ')
TOTAL_TOPICS=$(grep -c . /tmp/ohcs-all-topics.txt | tr -d ' ')
log "topics: total=$TOTAL_TOPICS  delete(state)=$N_STATE  purge(data)=$N_PURGE  keep(system)=$KEEP_SYS"

# Streams apps = those that own a *-streams-state PVC (the on-disk state to clear).
STATE_PVCS=$(kcr get pvc -l app.kubernetes.io/component=kafka-streams-state \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep . || true)
N_PVCS=$(printf '%s\n' "$STATE_PVCS" | grep -c . || true)
log "streams-state PVCs discovered: $N_PVCS"

# ===========================================================================
# DRY-RUN: log the full plan and exit WITHOUT touching anything.
# ===========================================================================
if [ "$MUTATE" = "false" ]; then
  log "DRY-RUN — would perform (NO changes made):"
  log "  A. Kafka: DELETE $N_STATE state topics; PURGE $N_PURGE data topics to 0; KEEP $KEEP_SYS system topics ($SYSTEM_TOPICS)"
  [ "$N_STATE" -gt 0 ] && { log "     state-topic sample:"; echo $STATE_TOPICS | tr ' ' '\n' | grep . | head -20 | sed 's/^/       DEL /'; }
  log "  B. Streams state: for each of $N_PVCS PVCs -> scale owner 0, delete+recreate PVC, scale 1 (--as=$KUBECTL_AS)"
  [ "$N_PVCS" -gt 0 ] && printf '%s\n' "$STATE_PVCS" | sed 's/^/       PVC /'
  if [ "$DB_WIPE" = "true" ]; then
    log "  C. Postgres: TRUNCATE all public tables in '$EXPECTED_DB' (incl. pin_session_close) + CHECKPOINT + VACUUM"
    TBLS=$(pg "select count(*) from pg_tables where schemaname='public'" 2>/dev/null | tr -d '[:space:]')
    log "     public tables that would be truncated: ${TBLS:-?}"
  else
    log "  C. Postgres: SKIPPED (PG_DSN unset)"
  fi
  log "  D. Logs: truncate kafka='$KAFKA_LOG_DIR' pg='$PG_LOG_DIR' docker-glob='$DOCKER_CONTAINER_LOG_GLOB'; reclaim_docker=$RECLAIM_DOCKER"
  log "  (Keycloak DB '$KEYCLOAK_DB_NAMES' + pod '$KEYCLOAK_POD' are NEVER touched.)"
  discord "🧪 Off-hours clean-slate DRY-RUN ($EXPECTED_ENV) — would delete ${N_STATE} state topics, purge ${N_PURGE} data topics, clear ${N_PVCS} PVCs$([ "$DB_WIPE" = true ] && echo ', truncate flow DB' || echo ' (DB skipped)'). No changes."
  log "=== off-hours clean-slate DRY-RUN complete ==="
  exit 0
fi

# ===========================================================================
# LIVE WIPE PATH (DRY_RUN=false, WIPE_ENABLED=true, all guards passed).
# ===========================================================================
log ">>> LIVE WIPE BEGINS <<<"

# Snapshot replica counts FIRST so any failure path can always scale back up.
SNAP=$(kcr get deploy -l "$SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.replicas}{"\n"}{end}')
[ -n "$SNAP" ] || die "no deployments matched $SELECTOR"

SCALED_DOWN=0
restore_scale() {
  [ "$SCALED_DOWN" = "1" ] || return 0
  log "restoring deployment replica counts (scale-back safety)"
  while read -r name reps; do
    [ -n "$name" ] && kc scale deploy "$name" --replicas="${reps:-1}" >/dev/null 2>&1
  done <<<"$SNAP"
  SCALED_DOWN=0
}
trap 'restore_scale' EXIT

# ---- B-pre: scale ALL pipeline deployments to 0 (stop producers/consumers) ----
# We stop everything before touching Kafka/PVCs so nothing re-creates state mid-wipe.
log "scaling $(printf '%s\n' "$SNAP" | grep -c .) deployments to 0"
SCALED_DOWN=1
while read -r name reps; do
  [ -n "$name" ] || continue
  kc scale deploy "$name" --replicas=0 >/dev/null 2>&1 || die "scale-to-0 failed for $name"
done <<<"$SNAP"
deadline=$(( $(date +%s) + SCALEDOWN_WAIT ))
while :; do
  READY=$(kcr get deploy -l "$SELECTOR" -o jsonpath='{range .items[*]}{.status.readyReplicas}{" "}{end}' 2>/dev/null \
          | tr ' ' '\n' | awk '{s+=$1} END{print s+0}')
  [ "${READY:-0}" = "0" ] && { log "all consumers stopped"; break; }
  [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: ${READY} replicas still ready after ${SCALEDOWN_WAIT}s — proceeding"; break; }
  sleep 5
done

# ---- A.1: DELETE state topics (*-changelog / *-repartition, incl. compact) ----
del_ok=0; del_fail=0
for T in $STATE_TOPICS; do
  is_system_topic "$T" && { log "GUARD: refusing to delete system topic $T"; continue; }   # belt+braces
  if "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$T" >/dev/null 2>&1 \
     || "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$T" >/dev/null 2>&1; then
    del_ok=$((del_ok+1))
  else
    del_fail=$((del_fail+1)); log "  WARN failed to delete state topic: $T"
  fi
done
log "state-topic delete: ok=$del_ok failed=$del_fail"

# ---- A.2: PURGE data topics to 0 records (delete-records to high-water) ----
SPEC=$(mktemp); echo '{"partitions":[' >"$SPEC"; first=1; purged=0
for T in $PURGE_TOPICS; do
  is_system_topic "$T" && { log "GUARD: refusing to purge system topic $T"; continue; }   # belt+braces
  OFFS=$("$KGO" --bootstrap-server "$BOOTSTRAP" --topic "$T" --time -1 2>/dev/null \
         | grep -E '^[^:]+:[0-9]+:[0-9]+$') || continue
  any=0
  while IFS=: read -r tp part off; do
    [ -z "$tp" ] && continue
    [ "$off" = "0" ] && continue
    [ $first -eq 0 ] && printf ',' >>"$SPEC"
    printf '{"topic":"%s","partition":%s,"offset":%s}' "$tp" "$part" "$off" >>"$SPEC"
    first=0; any=1
  done <<<"$OFFS"
  [ "$any" = "1" ] && purged=$((purged+1))
done
echo '],"version":1}' >>"$SPEC"
if [ $first -eq 0 ]; then
  "$KDR" --bootstrap-server "$BOOTSTRAP" --offset-json-file "$SPEC" >/dev/null 2>&1 \
    || log "WARN: kafka-delete-records returned errors (some topics may not be fully purged)"
  log "purged records across $purged data topics"
else
  log "no non-empty data partitions to purge"
fi
rm -f "$SPEC"

# Reset all consumer groups to latest (groups idle — apps scaled to 0).
for G in $("$KCG" --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null); do
  "$KCG" --bootstrap-server "$BOOTSTRAP" --group "$G" --reset-offsets --all-topics --to-latest --execute >/dev/null 2>&1 \
    || log "WARN: could not reset group $G"
done
log "consumer groups reset to latest"

# ---- B: clear Streams local state — delete+recreate each *-streams-state PVC ----
# Apps are already at 0 replicas, so the PVC is unmounted and can be replaced.
# We capture each PVC's full spec, delete it, and re-apply the captured spec so the
# storage request / accessModes / labels are preserved exactly.
pvc_ok=0; pvc_fail=0
if [ -n "$STATE_PVCS" ]; then
  for PVC in $STATE_PVCS; do
    SPECYAML=$(mktemp)
    if ! kcr get pvc "$PVC" -o json 2>/dev/null \
         | python3 -c 'import sys,json; o=json.load(sys.stdin); [o.pop(k,None) for k in ("status",)]; m=o.get("metadata",{}); [m.pop(k,None) for k in ("resourceVersion","uid","creationTimestamp","annotations","finalizers","managedFields")]; s=o.get("spec",{}); [s.pop(k,None) for k in ("volumeName","volumeMode")]; print(json.dumps(o))' >"$SPECYAML" 2>/dev/null; then
      log "  WARN could not capture PVC spec for $PVC — SKIPPING (will not delete what we cannot recreate)"
      pvc_fail=$((pvc_fail+1)); rm -f "$SPECYAML"; continue
    fi
    if kc delete pvc "$PVC" --wait=true --timeout="${PVC_RECREATE_WAIT}s" >/dev/null 2>&1; then
      if kc apply -f "$SPECYAML" >/dev/null 2>&1; then
        pvc_ok=$((pvc_ok+1))
      else
        log "  ERROR recreate FAILED for PVC $PVC — its owner cannot start until recreated!"
        pvc_fail=$((pvc_fail+1))
      fi
    else
      log "  WARN delete failed/timed out for PVC $PVC (still bound?) — skipping recreate"
      pvc_fail=$((pvc_fail+1))
    fi
    rm -f "$SPECYAML"
  done
fi
log "streams-state PVCs: recreated=$pvc_ok failed=$pvc_fail"

# ---- C: Postgres flow-DB wipe (TRUNCATE all public + CHECKPOINT + VACUUM) ----
trunc_n=0
if [ "$DB_WIPE" = "true" ]; then
  # Re-assert we are NOT on a Keycloak DB right before truncating (paranoia gate).
  DBN_NOW=$(pg "select current_database()") || die "DB re-check failed before truncate"
  for kdb in $KEYCLOAK_DB_NAMES; do
    [ "$DBN_NOW" = "$kdb" ] && die "DB became Keycloak '$kdb' — REFUSING truncate"
  done
  [ "$DBN_NOW" = "$EXPECTED_DB" ] || die "DB name changed to '$DBN_NOW' — REFUSING truncate"
  # Discover every public table and TRUNCATE them all in one statement (no CASCADE
  # needed within a single all-tables truncate). pin_session_close IS included.
  TBL_LIST=$(pg "select string_agg(format('%I.%I', schemaname, tablename), ', ') from pg_tables where schemaname='public'")
  if [ -n "$TBL_LIST" ]; then
    trunc_n=$(pg "select count(*) from pg_tables where schemaname='public'" | tr -d '[:space:]')
    pg "set lock_timeout='30s'; set statement_timeout='300s'; truncate table $TBL_LIST restart identity" \
      || die "TRUNCATE of flow DB failed"
    log "truncated $trunc_n public tables in '$EXPECTED_DB' (incl. pin_session_close)"
    # Reclaim WAL/disk: CHECKPOINT recycles WAL segments; VACUUM returns pages.
    pg "checkpoint" >/dev/null 2>&1 || log "WARN: CHECKPOINT failed"
    # VACUUM cannot run inside a txn block; psql -c each is fine. FULL is heavy but
    # off-hours is the window — keep plain VACUUM by default (configurable).
    pg "vacuum" >/dev/null 2>&1 || log "WARN: VACUUM failed"
    log "CHECKPOINT + VACUUM done (WAL/disk reclaimed)"
  else
    log "WARN: no public tables found to truncate (empty schema?)"
  fi
else
  log "DB wipe SKIPPED (PG_DSN unset)"
fi

# ---- A.3 + B-post: scale apps back up so they rebuild empty state cleanly ----
log "scaling deployments back up; waiting for rollout"
SCALEBACK_OK=1
while read -r name reps; do
  [ -n "$name" ] || continue
  kc scale deploy "$name" --replicas="${reps:-1}" >/dev/null 2>&1 \
    || { log "WARN: scale-back command failed: $name"; SCALEBACK_OK=0; }
done <<<"$SNAP"
for d in $(printf '%s\n' "$SNAP" | awk '{print $1}'); do
  kcr rollout status "deploy/$d" --timeout="${ROLLOUT_TIMEOUT}s" >/dev/null 2>&1 \
    || { log "WARN: rollout slow/incomplete: $d"; SCALEBACK_OK=0; }
done
SCALED_DOWN=0   # scale-back issued for all; EXIT trap must not re-issue.

# ---- D: logs + docker disk (truncate-in-place, NEVER rm) ----
log_files=0
truncate_glob() {
  local label="$1"; shift
  for f in "$@"; do
    [ -f "$f" ] || continue
    : > "$f" 2>/dev/null && log_files=$((log_files+1)) || log "  WARN could not truncate $label log: $f"
  done
}
if [ -n "$KAFKA_LOG_DIR" ] && [ -d "$KAFKA_LOG_DIR" ]; then
  truncate_glob "kafka" "$KAFKA_LOG_DIR"/server.log "$KAFKA_LOG_DIR"/controller.log "$KAFKA_LOG_DIR"/state-change.log
fi
if [ -n "$PG_LOG_DIR" ] && [ -d "$PG_LOG_DIR" ]; then
  for f in "$PG_LOG_DIR"/*.log; do truncate_glob "postgres" "$f"; done
fi
if [ -n "$DOCKER_CONTAINER_LOG_GLOB" ]; then
  # NOTE: dev/docker-desktop only. On prod (containerd) this glob is unset.
  for f in $DOCKER_CONTAINER_LOG_GLOB; do truncate_glob "docker-json" "$f"; done
fi
log "log files truncated in place: $log_files"

docker_reclaimed="n/a"
if [ "$RECLAIM_DOCKER" = "true" ] && have docker; then
  docker system prune -f >/dev/null 2>&1 && docker_reclaimed="pruned" || log "WARN: docker system prune failed"
  # Local-registry GC (the registry once grew to 571GB and filled the disk).
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$REGISTRY_CONTAINER"; then
    docker exec "$REGISTRY_CONTAINER" registry garbage-collect /etc/docker/registry/config.yml >/dev/null 2>&1 \
      && docker_reclaimed="$docker_reclaimed+registry-gc" \
      || log "WARN: registry GC failed (check config path)"
  fi
  log "docker reclaim: $docker_reclaimed"
fi

# ===========================================================================
# Summary
# ===========================================================================
ELAPSED=$(( $(date +%s) - START_TS ))
log "=== off-hours clean-slate complete in ${ELAPSED}s ==="
SUMMARY="env=$EXPECTED_ENV state_topics_deleted=$del_ok purge_topics=$purged pvcs_recreated=$pvc_ok db_tables_truncated=$trunc_n logs_truncated=$log_files scaleback_ok=$SCALEBACK_OK elapsed=${ELAPSED}s"
log "SUMMARY: $SUMMARY"
if [ "$del_fail" -eq 0 ] && [ "$pvc_fail" -eq 0 ] && [ "$SCALEBACK_OK" = "1" ]; then
  discord "🧹 Off-hours clean-slate complete ($EXPECTED_ENV) — deleted **${del_ok}** state topics, purged **${purged}** data topics, recreated **${pvc_ok}** PVCs, truncated **${trunc_n}** DB tables, **${log_files}** logs; apps back up. ✅"
  exit 0
fi
discord "⚠️ Off-hours clean-slate ($EXPECTED_ENV) finished WITH ISSUES — state_del_fail=$del_fail pvc_fail=$pvc_fail scaleback_ok=$SCALEBACK_OK. CHECK: kubectl -n $NS get deploy"
exit 1
