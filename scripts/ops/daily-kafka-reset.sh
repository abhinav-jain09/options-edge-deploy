#!/usr/bin/env bash
# Daily 0DTE clean-slate reset for OptionsEdge prod Kafka + Streams apps.
#
# Runs on the prod host (cron, user abhinav). Because OptionsEdge trades 0DTE
# and there is no market data after close, all Kafka Streams state from the
# session is disposable. Each night this:
#   1) scales the options-edge app deployments to 0 (release topic ownership),
#   2) deletes disposable topics: every *-changelog / *-repartition (Streams
#      state), plus any *replay* / *smoke* / dev.* leftovers,
#   3) scales the apps back to their original replica counts; they recreate
#      their internal topics fresh on startup.
#
# SAFETY: this NEVER deletes system topics (__consumer_offsets, _schemas,
# __transaction_state) or real data topics (options.*, underlying.*, etc.).
# It only deletes topics matching the explicit disposable patterns below.
#
# Dry-run by default. Set DRY_RUN=false to actually mutate.
set -uo pipefail

# ---- config (override via env) ----
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.252:9094,192.168.100.252:9096}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="${K8S_NAMESPACE:-options-edge}"
SELECTOR="${APP_SELECTOR:-app.kubernetes.io/part-of=options-edge}"
DRY_RUN="${DRY_RUN:-true}"
SCALE_WAIT="${SCALE_WAIT:-90}"
KT="$KAFKA_BIN/kafka-topics.sh"

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] $*"; }

# never-delete: system topics + anything that is NOT clearly disposable.
# disposable = ends in -changelog/-repartition, OR contains replay/smoke, OR starts with dev.
is_disposable() {
  local t="$1"
  case "$t" in
    __*|_schemas) return 1 ;;                       # system: keep
  esac
  case "$t" in
    *-changelog|*-repartition) return 0 ;;          # Streams state: delete (recreated fresh)
    *replay*|*smoke*) return 0 ;;                    # one-off run leftovers: delete
    dev.*) return 0 ;;                               # pre-prefix-fix orphans: delete
  esac
  return 1                                           # everything else (real data): keep
}

# ---- single-instance lock ----
LOCK="/tmp/daily-kafka-reset.lock"
exec 9>"$LOCK" || exit 1
if ! flock -n 9; then log "another reset is already running; exiting"; exit 0; fi

log "=== daily kafka reset start (DRY_RUN=$DRY_RUN) ==="

# ---- sanity: both kafka and k8s must be reachable, else abort untouched ----
if ! "$KT" --bootstrap-server "$BOOTSTRAP" --list >/tmp/dkr-topics.txt 2>/dev/null; then
  log "ABORT: cannot reach Kafka at $BOOTSTRAP"; exit 1
fi
if ! kubectl -n "$NS" get deploy >/dev/null 2>&1; then
  log "ABORT: cannot reach k8s namespace $NS"; exit 1
fi
TOTAL=$(wc -l </tmp/dkr-topics.txt | tr -d ' ')
DELELIGIBLE=$(while read -r t; do is_disposable "$t" && echo "$t"; done </tmp/dkr-topics.txt | wc -l | tr -d ' ')
log "topics total=$TOTAL  disposable=$DELELIGIBLE  keep=$((TOTAL-DELELIGIBLE))"

# ---- capture current replicas so we can always restore ----
REPLICA_BAK="/tmp/dkr-replicas.txt"
kubectl -n "$NS" get deploy -l "$SELECTOR" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.replicas}{"\n"}{end}' > "$REPLICA_BAK" 2>/dev/null
APPS=$(wc -l <"$REPLICA_BAK" | tr -d ' ')
log "captured replicas for $APPS deployments -> $REPLICA_BAK"

# ---- always attempt to restore replicas on exit (even on error) ----
restore_replicas() {
  log "restoring replica counts..."
  while read -r name rep; do
    [ -n "$name" ] || continue
    [ "${rep:-0}" -gt 0 ] 2>/dev/null || rep=0
    if [ "$DRY_RUN" = "true" ]; then
      log "  [dry-run] would scale deploy/$name -> $rep"; continue
    fi
    ok=false
    for attempt in 1 2 3 4 5 6; do
      if kubectl -n "$NS" scale deploy "$name" --replicas="$rep" >/dev/null 2>&1; then ok=true; break; fi
      sleep 3
    done
    if [ "$ok" = "true" ]; then log "  scaled deploy/$name -> $rep"; else log "  WARN failed to scale deploy/$name -> $rep after retries"; fi
  done < "$REPLICA_BAK"
}
trap restore_replicas EXIT

if [ "$DRY_RUN" = "true" ]; then
  log "DRY-RUN: would scale $APPS deployments to 0, then delete $DELELIGIBLE topics:"
  while read -r t; do is_disposable "$t" && echo "    DEL $t"; done </tmp/dkr-topics.txt | head -40
  log "(showing up to 40; total $DELELIGIBLE). No changes made."
  exit 0
fi

# ---- 1) scale apps down ----
log "scaling $APPS deployments to 0..."
kubectl -n "$NS" scale deploy -l "$SELECTOR" --replicas=0 >/dev/null 2>&1
# wait for pods to terminate
for ((i=0;i<SCALE_WAIT;i++)); do
  running=$(kubectl -n "$NS" get pods -l "$SELECTOR" --no-headers 2>/dev/null | grep -vcE 'Terminating|Completed' )
  [ "${running:-0}" -eq 0 ] && break
  sleep 2
done
log "apps scaled down (waited up to ${SCALE_WAIT}s)"

# ---- 2) delete disposable topics (with one retry each) ----
deleted=0; failed=0
while read -r t; do
  is_disposable "$t" || continue
  if "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1 \
     || "$KT" --bootstrap-server "$BOOTSTRAP" --delete --topic "$t" >/dev/null 2>&1; then
    deleted=$((deleted+1))
  else
    failed=$((failed+1)); log "  WARN failed to delete topic: $t"
  fi
done </tmp/dkr-topics.txt
log "topic deletion done: deleted=$deleted failed=$failed"

# ---- 3) restore replicas (via trap on EXIT) ----
log "=== daily kafka reset complete; apps scaling back up ==="
