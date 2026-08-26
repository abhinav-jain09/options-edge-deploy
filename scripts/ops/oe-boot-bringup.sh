#!/usr/bin/env bash
# Staged, Kafka-gated bring-up of the OptionsEdge stack after a prod reboot.
#
# WHY (measured 2026-08-19): on a cold boot Kafka needs ~180s of log recovery before it
# listens on :9092, but k3s starts every Deployment immediately. Four services died on
# arrival and stayed down for 3h36m — they are fail-loud BY DESIGN and never retry:
#   delta-flow-service          Connection refused + Avro schema fetch failed
#   drop-classifier-service     TimeoutException: Failed to send request after 30000 ms
#   spot-vol-regime-service     "cannot determine whether source topic ... exists; refusing to boot"
#   strike-intelligence-service "timeout creating source topic underlying.spx.price"
# all with "node 1 being disconnected" — i.e. Kafka simply was not up yet.
#
# Starting all ~57 at once also drove load to 73 on 24 cores. On 2026-08-17 that same
# saturation stopped sshd, the k3s API and Kafka from answering at all (TCP connected,
# processes could not respond) and the box had to be power-cycled. So this starts in
# waves and waits for load to fall between them.
#
# Fails LOUD: a silent no-op here looks like protection that is not there.
set -uo pipefail

KUBECTL="k3s kubectl -n options-edge"
SA="--as=system:serviceaccount:options-edge:jenkins-deployer"
LOG=/var/log/oe-boot-bringup.log
KAFKA_WAIT_SECONDS="${KAFKA_WAIT_SECONDS:-900}"
LOAD_CEILING="${LOAD_CEILING:-30}"       # 24 cores; wait below this before the next wave
LOAD_WAIT_SECONDS="${LOAD_WAIT_SECONDS:-600}"

# Wave 1: the data path everything else consumes. Nothing downstream can be healthy
# before these are, so they go first and alone.
WAVE1='options-edge-databento-feed feed-gateway-service options-edge-web'
# Held down by explicit USER decision — never started here.
# 2026-08-26: kept IN SYNC with scripts/ops/morning-autostart.sh KEEP_DOWN. This list ran on
# BOOT with only 5 of the 18 declared names, so every reboot brought back services the USER
# had ordered held down — including directional-pressure-databento-service and volume-pace*,
# which took production down twice. Change BOTH lists or neither.
KEEP_DOWN='directional-pressure-databento-service|hpsf-stage-a-service|hpsf-stage-b-service|volume-sandwich-service|volume-sandwich-databento-service|volume-pace-service|volume-pace-databento-service|strike-flow-classifier-ibkr|options-edge-integration-test|spx-mission-control-service|short-premium-agent-service|spread-skew-service|spread-skew-postgres-writer|databento-mission-sandwich-service|directional-pressure-service|option-truth-engine-service|ibkr-feed-service|oi-shadow-service|raw-to-display-service|prod-pgadmin'

log() { printf '[%s] %s\n' "$(date '+%F %T %Z')" "$*" | tee -a "$LOG"; }
die() { log "FAIL: $*"; exit 1; }

log "=== boot bring-up start (uptime: $(uptime | tr -s ' ')) ==="

# ---------- gate 1: Kafka must actually LISTEN, not merely be "active" ----------
# systemctl reports kafka active while it is still replaying logs; that is exactly the
# window the four services died in. Only a listening socket proves it can serve.
waited=0
until ss -lnt 2>/dev/null | grep -q ':9092'; do
  [ "$waited" -ge "$KAFKA_WAIT_SECONDS" ] && die "Kafka never listened on :9092 within ${KAFKA_WAIT_SECONDS}s (systemctl says: $(systemctl is-active kafka))"
  sleep 10; waited=$((waited+10))
done
log "Kafka listening on :9092 after ${waited}s"

# A listening socket is necessary, not sufficient — the broker must answer a real request.
B=/opt/kafka/current/bin
until "$B/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  [ "$waited" -ge "$KAFKA_WAIT_SECONDS" ] && die "Kafka listens but does not answer --list within ${KAFKA_WAIT_SECONDS}s"
  sleep 10; waited=$((waited+10))
done
log "Kafka answering metadata after ${waited}s total"

wait_for_load() {
  local w=0
  while :; do
    cur=$(awk '{print int($1)}' /proc/loadavg)
    [ "$cur" -lt "$LOAD_CEILING" ] && { log "load ${cur} < ${LOAD_CEILING}, continuing"; return 0; }
    [ "$w" -ge "$LOAD_WAIT_SECONDS" ] && { log "WARN: load still ${cur} after ${w}s — continuing anyway so the stack is not left half-up"; return 0; }
    sleep 20; w=$((w+20))
  done
}

scale_up() {
  local n=0
  for d in $1; do
    $KUBECTL get deploy "$d" >/dev/null 2>&1 || { log "WARN: no deployment $d — skipped"; continue; }
    $KUBECTL scale deploy "$d" --replicas=1 $SA >/dev/null 2>&1 && n=$((n+1)) || log "WARN: scale failed for $d"
  done
  log "wave: scaled $n deployment(s)"
}

# ---------- wave 1 ----------
scale_up "$WAVE1"
sleep 60
wait_for_load

# ---------- wave 2: everything else that is currently at 0 ----------
REST=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{split($2,a,"/"); if (a[2]==0) print $1}' | grep -vE "$KEEP_DOWN")
scale_up "$REST"

sleep 120
TOTAL=$($KUBECTL get deploy --no-headers 2>/dev/null | wc -l)
READY=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{split($2,a,"/"); if (a[1]==a[2] && a[2]>0) r++} END{print r+0}')
NOTREADY=$($KUBECTL get deploy --no-headers 2>/dev/null | awk '{split($2,a,"/"); if (a[2]>0 && a[1]!=a[2]) print $1}')
log "result: ${READY}/${TOTAL} ready; load $(awk '{print $1}' /proc/loadavg)"
[ -n "$NOTREADY" ] && log "still not ready (may still be settling): $(echo $NOTREADY | tr '\n' ' ')"
log "=== boot bring-up done ==="
