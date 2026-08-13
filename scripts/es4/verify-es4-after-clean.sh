#!/usr/bin/env bash
# Post-clean verification for es4. Run after clean-reset + activate-es-feed.
#
# WHY THIS EXISTS (2026-08-13): a clean-reset reported success and every deployment
# read 1/1 Running restarts=0, while es-feed-gateway's avro consumer died every 30s on
#   FeedGatewayService$TopicMetadataTimeoutException: … [es.options.ibkr.directional-pressure]
# Only the CONSUMER THREAD died, so the pod stayed healthy and the ready-count was blind
# to it. The clean's own verify stage could not catch it either: that stage checks that
# DECLARED topics were created, and this topic was never declared — it had only ever
# existed because the broker auto-created it, and es4 auto-create is now off.
#
# So a green ready-count is NOT evidence that es4 works. This script checks the four
# things that actually are:
#   A. every deployment ready
#   B. service LOGS clean (the consumer end, not the pod's liveness)
#   C. every live es.* topic is DECLARED (anything undeclared dies on the NEXT clean)
#   D. data actually flowing — and if the market is shut, it says VERIFICATION DEFERRED
#      and exits non-zero rather than passing silently. CME Globex halts 17:00-18:00 ET
#      daily and is shut Fri 17:00 -> Sun 18:00; a clean finished inside those windows
#      proves nothing about the pipeline.
#
# Exit codes: 0 = verified · 1 = a real failure · 2 = cannot verify yet (market shut)
set -uo pipefail

ES4_HOST="${ES4_HOST:-192.168.100.4}"
SSH_BIN=(ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR -o ConnectTimeout=30)
[ -n "${ES4_SSH_PW:-}" ] && SSH_BIN=(sshpass -p "$ES4_SSH_PW" "${SSH_BIN[@]}")
REMOTE=("${SSH_BIN[@]}" "abhinav@${ES4_HOST}")
KUBECTL="sudo -n /usr/local/bin/k3s kubectl -n options-edge"
KTOPICS="sudo -n docker exec es4-kafka kafka-topics --bootstrap-server localhost:9092"
KOFFSETS="sudo -n docker exec es4-kafka kafka-get-offsets --bootstrap-server localhost:9092"

# Services whose CONSUMER can die while the pod stays Running. Extend as new ones appear.
LOG_SERVICES="${ES4_LOG_SERVICES:-es-feed-gateway es-feed es-web}"
# Topics that must advance for the chain to be considered alive.
FLOW_TOPICS="${ES4_FLOW_TOPICS:-es.options.databento.display es.options.databento.gex.strike}"
FLOW_WINDOW_SECONDS="${ES4_FLOW_WINDOW_SECONDS:-75}"
# Log lines that mean a consumer is broken. Deliberately narrow: Kafka Streams logs
# "No partitions were buffered locally" at INFO on a healthy idle task — matching that
# would make this script cry wolf on every quiet market.
LOG_ERROR_RE='Exception|Timed out waiting for Kafka topic|Missing source topics|UNKNOWN_TOPIC_OR_PARTITION|account_insufficient_funds'
# Not contract-managed: MirrorMaker2 and Kafka Streams create these themselves.
INTERNAL_TOPIC_RE='^es\.checkpoints\.internal$|^es\.heartbeats$|changelog$|repartition$|^__|-KSTREAM-|-STATE-STORE-'

fail=0
note() { printf '  %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; fail=1; }

remote() { "${REMOTE[@]}" "$@" 2>/dev/null; }

# ---------- market window (ET), so a 0 reading is never silently excused ----------
market_state() {
  TZ=America/New_York date '+%u %H%M' | awk '{
    dow=$1+0; hm=$2+0
    # CME Globex: Sun 18:00 open -> Fri 17:00 close, daily halt 17:00-18:00.
    if (dow==6) { print "SHUT weekend"; exit }
    if (dow==7) { if (hm<1800) print "SHUT weekend"; else print "OPEN"; exit }
    if (dow==5 && hm>=1700) { print "SHUT weekend"; exit }
    if (hm>=1700 && hm<1800) { print "SHUT daily-halt"; exit }
    print "OPEN"
  }'
}

echo "== A. deployments =="
DEPLOY=$(remote "$KUBECTL get deploy --no-headers")
[ -z "$DEPLOY" ] && { bad "could not read deployments from ${ES4_HOST}"; exit 1; }
NOT_READY=$(echo "$DEPLOY" | awk '{split($2,a,"/"); if (a[1]!=a[2] && a[2]>0) print "   "$1"  "$2}')
echo "$DEPLOY" | awk '{s++; split($2,a,"/"); if (a[1]==a[2] && a[2]>0) r++} END {printf "  %d deployments · ready %d\n", s, r+0}'
if [ -n "$NOT_READY" ]; then bad "deployments not ready:"; echo "$NOT_READY"; else note "all ready"; fi

echo "== B. service logs (the consumer end — a pod can be 1/1 while its consumer is dead) =="
for svc in $LOG_SERVICES; do
  logs=$(remote "$KUBECTL logs deploy/$svc --tail=200")
  # An empty read is NOT a clean read. grep -c on nothing returns 0, so a typo'd service
  # name, a deleted deployment, or an ssh failure would all report "clean" — the exact
  # silence-means-success trap this gate exists to close. Demand evidence of a real read.
  if [ "$(printf '%s' "$logs" | wc -l)" -lt 1 ]; then
    bad "$svc: no log output — deployment missing, or the read failed. NOT verified."
    continue
  fi
  hits=$(printf '%s\n' "$logs" | grep -cE "$LOG_ERROR_RE")
  if [ "${hits:-0}" -gt 0 ]; then
    bad "$svc: $hits error line(s)"
    printf '%s\n' "$logs" | grep -E "$LOG_ERROR_RE" | tail -2 | cut -c1-160 | sed 's/^/       /'
  else
    note "$svc: clean ($(printf '%s' "$logs" | wc -l) lines read)"
  fi
done

echo "== C. live topics vs the declared contract (undeclared = dies on the NEXT clean) =="
LIVE=$(remote "$KTOPICS --list" | grep -E '^es\.' | grep -vE "$INTERNAL_TOPIC_RE" | sort -u)
TOPICS_ENV="${TOPICS_ENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/kafka/topics.env}"
if [ ! -f "$TOPICS_ENV" ]; then
  bad "topics.env not found at $TOPICS_ENV — cannot check declarations"
else
  # shellcheck disable=SC1090
  # Filter internals from BOTH sides. Filtering only the live side made every DECLARED
  # internal (es.heartbeats) read as "missing from the broker" — a false alarm, and a
  # false alarm in a gate like this is worse than no gate: it trains the operator to
  # ignore the output.
  DECLARED=$(set -a; . "$TOPICS_ENV" >/dev/null 2>&1; \
             printf '%s\n' $OPTIONS_EDGE_ES4_TOPICS | cut -d: -f1 \
             | grep -vE "$INTERNAL_TOPIC_RE" | sort -u)
  UNDECLARED=$(comm -23 <(printf '%s\n' "$LIVE") <(printf '%s\n' "$DECLARED"))
  if [ -n "$UNDECLARED" ]; then
    bad "live but NOT declared in OPTIONS_EDGE_ES4_TOPICS — these will be wiped and never recreated:"
    printf '%s\n' "$UNDECLARED" | sed 's/^/       /'
  else
    note "every live es.* topic is declared"
  fi
  # The inverse also matters: something the contract promises but the broker does not have.
  MISSING=$(comm -13 <(printf '%s\n' "$LIVE") <(printf '%s\n' "$DECLARED"))
  if [ -n "$MISSING" ]; then
    bad "declared but MISSING on the broker:"
    printf '%s\n' "$MISSING" | sed 's/^/       /'
  fi
fi

echo "== D. data flow =="
STATE=$(market_state)
BEFORE=$(remote "for t in $FLOW_TOPICS; do echo \"\$t \$($KOFFSETS --topic \$t 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}')\"; done")
sleep "$FLOW_WINDOW_SECONDS"
AFTER=$(remote "for t in $FLOW_TOPICS; do echo \"\$t \$($KOFFSETS --topic \$t 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}')\"; done")
stalled=""
for t in $FLOW_TOPICS; do
  b=$(echo "$BEFORE" | awk -v t="$t" '$1==t{print $2}')
  a=$(echo "$AFTER"  | awk -v t="$t" '$1==t{print $2}')
  d=$(( ${a:-0} - ${b:-0} ))
  printf '  %-44s +%s/%ss\n' "$t" "$d" "$FLOW_WINDOW_SECONDS"
  [ "$d" -le 0 ] && stalled="$stalled $t"
done

if [ -n "$stalled" ]; then
  if [ "${STATE% *}" = "SHUT" ]; then
    echo
    echo "  VERIFICATION DEFERRED — market is ${STATE}."
    echo "  Topics not advancing:${stalled}"
    echo "  A zero reading here proves NOTHING. Re-run this script once Globex reopens"
    echo "  (Sun 18:00 ET, or 18:00 ET after the daily halt) before calling es4 verified."
    [ "$fail" -ne 0 ] && exit 1
    exit 2
  fi
  bad "market is OPEN but these topics are not advancing:${stalled}"
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "  RESULT: FAILED — es4 is NOT verified."
  exit 1
fi
echo "  RESULT: VERIFIED — deployments ready, consumers clean, contract complete, data flowing (market ${STATE})."
