#!/usr/bin/env bash
# Pre-market readiness sweep (P1 — READ ONLY) for the OptionsEdge Databento pipeline.
#
# Runs on the prod host (via the Jenkins job `options-edge-premarket`, fired at
# 09:30 ET minus 4h/3h/2h/1h) next to the brokers + k3s API. It answers ONE
# question: "is the system up and ready to take today's feed at the open?" and
# posts a GO / NOT-READY card to Discord. It NEVER mutates anything — no topic
# delete, no truncate, no scale. (The destructive T-4h reset is a separate,
# gated script: daily-kafka-reset.sh extended for P3.)
#
# Design notes:
#   * The k8s readiness PROBE for every service IS its /health/ready endpoint,
#     so a Deployment reporting readyReplicas == desired already means
#     /health/ready returned 200. We therefore read readiness from `kubectl`
#     rather than cur/ing each pod — simpler and exactly equivalent.
#   * Trading-day / holiday guard reuses the vendored MarketCalendar
#     (scripts/jenkins/market_calendar.py) — no separate calendar.
#   * IBKR is OUT OF SCOPE: ibkr-* services are not checked and IB* error lines
#     are filtered out of the log digest.
#   * Token discipline: deterministic checks only; a Haiku triage call happens
#     ONLY on NOT-READY and only if ANTHROPIC_API_KEY is set (mode H).
#
# Exit code: 0 if GO, 1 if NOT-READY (so Jenkins shows red on a bad morning).
# Non-trading day → exit 0 immediately, no Discord post.
set -uo pipefail

# ---------------------------------------------------------------------------
# Config (override via env)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALENDAR_DIR="${CALENDAR_DIR:-$SCRIPT_DIR/../jenkins}"   # market_calendar.py lives in scripts/jenkins

NS="${K8S_NAMESPACE:-options-edge}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.4:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://192.168.100.252:8082}"
KEYCLOAK_HEALTH_URL="${KEYCLOAK_HEALTH_URL:-}"           # e.g. http://192.168.100.252:9000/health/ready ; empty -> skip
GATEWAY_CONFIG_URL="${GATEWAY_CONFIG_URL:-}"             # e.g. http://192.168.100.252:8093/api/config ; empty -> skip expiry detect

# Postgres read-only liveness (SELECT 1). Provide a libpq DSN or PG* envs.
PG_DSN="${PG_DSN:-}"                                     # e.g. "host=192.168.100.252 port=5432 dbname=options_flow user=readonly"

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"          # empty -> print card to stdout only
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"             # empty -> skip Haiku triage even on red
HAIKU_MODEL="${HAIKU_MODEL:-claude-haiku-4-5-20251001}"

LAG_THRESHOLD="${LAG_THRESHOLD:-100000}"                # advisory pre-open
RESTART_WINDOW_MIN="${RESTART_WINDOW_MIN:-60}"
LOG_SINCE="${LOG_SINCE:-60m}"
LOG_TAIL="${LOG_TAIL:-2000}"                            # per-service log lines scanned (bounded)
TOP_N_ERRORS="${TOP_N_ERRORS:-5}"
CLOCK_SKEW_MAX_S="${CLOCK_SKEW_MAX_S:-15}"

# Identity-guard expectations (read-only assert in P1; HARD gate in P3).
EXPECTED_ENV="${EXPECTED_ENV:-prod}"
EXPECTED_KAFKA_CLUSTER_ID="${EXPECTED_KAFKA_CLUSTER_ID:-}"   # empty -> advisory only
EXPECTED_DB="${EXPECTED_DB:-options_flow}"

# Services that may legitimately be absent / scaled to 0 in some envs (space-
# separated names) → reported as WARN instead of FAIL. Everything else in
# PIPELINE_SERVICES that is missing or 0/desired is a hard FAIL (fail-closed).
OPTIONAL_SERVICES="${OPTIONAL_SERVICES:-}"
is_optional() { case " $OPTIONAL_SERVICES " in *" $1 "*) return 0;; *) return 1;; esac; }

# Databento pipeline deployments (IBKR + one-off test rigs excluded). Override
# with PIPELINE_SERVICES="a b c". Tier prefix groups them in the card.
# Names are the authoritative k8s metadata.name values (verified against the
# deploy manifests + the dev cluster 2026-06-28). Databento variants only;
# IBKR (*-ibkr) and the legacy non-databento duplicates are excluded.
PIPELINE_SERVICES="${PIPELINE_SERVICES:-\
T1:options-edge-databento-feed T1:databento-volume-aggregator \
T2:strike-flow-classifier-databento T2:raw-to-display-databento-service T2:directional-pressure-databento-service \
T2:volume-pace-databento-service T2:volume-sandwich-databento-service \
T2:databento-gex-service T2:databento-gex-history-service T2:databento-maxpain-service \
T3:databento-mission-pace-service T3:databento-mission-pressure-service T3:databento-mission-sandwich-service \
T3:spx-mission-control-service T3:feed-gateway-service T3:options-edge-web \
T4:hpsf-postgres-writer-service T4:raw-postgres-writer T4:pressure-postgres-writer T4:pin-postgres-writer \
H:hpsf-stage-a-service H:hpsf-stage-b-service}"

# ---------------------------------------------------------------------------
# Result accumulation
# ---------------------------------------------------------------------------
declare -a FAILS=()    # hard failures -> NOT-READY
declare -a WARNS=()    # advisory only
declare -a OKS=()
fail() { FAILS+=("$1"); }
warn() { WARNS+=("$1"); }
ok()   { OKS+=("$1"); }
have() { command -v "$1" >/dev/null 2>&1; }

KAFKA_TOPICS="$KAFKA_BIN/kafka-topics.sh"
KAFKA_CG="$KAFKA_BIN/kafka-consumer-groups.sh"
KAFKA_GETOFF="$KAFKA_BIN/kafka-get-offsets.sh"

# ---------------------------------------------------------------------------
# Hard prerequisites — FAIL CLOSED. A missing core tool means we cannot assert
# readiness; reporting GO in that case would be a dangerous false positive.
# (Codex review: "silently skipped → false GO" was the top risk.)
# ---------------------------------------------------------------------------
for t in curl jq kubectl python3; do
  have "$t" || fail "required tool missing: $t (cannot assert readiness)"
done
have "$KAFKA_TOPICS" || fail "kafka CLI missing at $KAFKA_BIN (cannot assert Kafka readiness)"

# ---------------------------------------------------------------------------
# 0. Trading-day guard (exit 0 silently if not a trading day)
# ---------------------------------------------------------------------------
TODAY_EXPIRY=""
HOURS_TO_OPEN=""
if have python3; then
  GUARD=$(CALENDAR_DIR="$CALENDAR_DIR" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
from datetime import datetime
from zoneinfo import ZoneInfo
try:
    from market_calendar import MarketCalendar
except Exception as e:
    print(f"ERR calendar import failed: {e}"); sys.exit(0)
cal = MarketCalendar()
now = datetime.now(ZoneInfo("America/New_York"))
td = cal.current_trading_date(now)
if not cal.is_trading_day(now.date()):
    print("SKIP not-a-trading-day"); sys.exit(0)
secs = cal.seconds_until_next_open(now)
print(f"OK {td.strftime('%Y%m%d')} {secs/3600:.1f}")
PY
)
  # PREMARKET_FORCE=1 runs the sweep regardless of calendar (manual/on-demand
  # verification). It still computes today's expiry where possible.
  case "$GUARD" in
    SKIP*) if [ "${PREMARKET_FORCE:-0}" = "1" ]; then
             echo "[premarket] PREMARKET_FORCE=1 — running despite '$GUARD'"
           else
             echo "[premarket] $(date '+%FT%T%z') $GUARD — exiting 0, no alert"; exit 0
           fi ;;
    OK*)   TODAY_EXPIRY=$(echo "$GUARD" | awk '{print $2}'); HOURS_TO_OPEN=$(echo "$GUARD" | awk '{print $3}') ;;
    *)     fail "calendar guard inconclusive ($GUARD) — cannot determine trading day; fail-closed" ;;
  esac
else
  fail "python3 missing — cannot run trading-day guard (fail-closed)"
fi

# ---------------------------------------------------------------------------
# 1. Identity guard (read-only assert; surfaces P3-blocked prerequisites)
# ---------------------------------------------------------------------------
P3_BLOCKED=()
if [ -n "$EXPECTED_KAFKA_CLUSTER_ID" ] && have "$KAFKA_BIN/kafka-cluster.sh"; then
  CID=$("$KAFKA_BIN/kafka-cluster.sh" cluster-id --bootstrap-server "$KAFKA_BOOTSTRAP" 2>/dev/null | grep -oE 'Cluster ID:.*' | awk '{print $3}')
  if [ "$CID" != "$EXPECTED_KAFKA_CLUSTER_ID" ]; then
    P3_BLOCKED+=("kafka cluster-id mismatch (got '$CID', expected '$EXPECTED_KAFKA_CLUSTER_ID')")
  fi
elif [ -z "$EXPECTED_KAFKA_CLUSTER_ID" ]; then
  P3_BLOCKED+=("EXPECTED_KAFKA_CLUSTER_ID unset — pin before enabling P3 reset")
fi

# ---------------------------------------------------------------------------
# 2. Tier 0 — infrastructure
# ---------------------------------------------------------------------------
# Kafka reachable + broker count
if have "$KAFKA_TOPICS"; then
  if "$KAFKA_TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/tmp/pmr-topics.txt 2>/dev/null; then
    ok "Kafka reachable ($(wc -l </tmp/pmr-topics.txt | tr -d ' ') topics)"
  else
    fail "Kafka UNREACHABLE at $KAFKA_BOOTSTRAP"
  fi
fi   # (kafka CLI absence already FAILed in the prerequisites gate)

# Schema Registry
if have curl; then
  if curl -fsS --max-time 8 "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
    ok "Schema Registry reachable"
  else
    fail "Schema Registry UNREACHABLE ($SCHEMA_REGISTRY_URL)"
  fi
fi   # (curl absence already FAILed in the prerequisites gate)

# Postgres liveness (fail-closed: unconfigured or missing client is a FAIL,
# not a silent skip — a readiness gate must not GO without checking Postgres)
if [ -z "$PG_DSN" ]; then
  fail "PG_DSN not configured — cannot verify Postgres readiness"
elif ! have psql; then
  fail "psql missing — cannot verify Postgres readiness"
elif psql "$PG_DSN" -tAc 'select 1' >/dev/null 2>&1; then
  ok "Postgres reachable"
  # DB identity (P3 prerequisite)
  DBN=$(psql "$PG_DSN" -tAc 'select current_database()' 2>/dev/null | tr -d '[:space:]')
  [ "$DBN" = "$EXPECTED_DB" ] || P3_BLOCKED+=("Postgres db mismatch (got '$DBN', expected '$EXPECTED_DB')")
else
  fail "Postgres UNREACHABLE / SELECT 1 failed"
fi

# Keycloak
if [ -n "$KEYCLOAK_HEALTH_URL" ] && have curl; then
  if curl -fsS --max-time 8 "$KEYCLOAK_HEALTH_URL" >/dev/null 2>&1; then
    ok "Keycloak healthy"
  else
    fail "Keycloak NOT ready ($KEYCLOAK_HEALTH_URL) — Mission Control is fail-closed on auth"
  fi
else
  warn "KEYCLOAK_HEALTH_URL unset — Keycloak check skipped"
fi

# Clock skew (advisory) — compare host clock to an HTTP Date header
if have curl; then
  REMOTE_DATE=$(curl -sI --max-time 8 https://www.cloudflare.com 2>/dev/null | grep -i '^date:' | head -1 | cut -d' ' -f2-)
  if [ -n "$REMOTE_DATE" ]; then
    REMOTE_EPOCH=$(date -d "$REMOTE_DATE" +%s 2>/dev/null || echo "")
    if [ -n "$REMOTE_EPOCH" ]; then
      SKEW=$(( $(date +%s) - REMOTE_EPOCH )); SKEW=${SKEW#-}
      if [ "$SKEW" -le "$CLOCK_SKEW_MAX_S" ]; then ok "clock skew ${SKEW}s (<= ${CLOCK_SKEW_MAX_S}s)"
      else warn "clock skew ${SKEW}s exceeds ${CLOCK_SKEW_MAX_S}s — see 2026-06-26 stale-freeze incident"; fi
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 3. Tier 1-4 + HPSF — Deployment readiness (readyReplicas == desired)
# ---------------------------------------------------------------------------
if have kubectl && have jq; then
  DEPLOY_JSON=$(kubectl -n "$NS" get deploy -o json 2>/dev/null)
  if [ -z "$DEPLOY_JSON" ]; then
    fail "kubectl: cannot list deployments in namespace $NS"
  else
    for entry in $PIPELINE_SERVICES; do
      tier="${entry%%:*}"; svc="${entry##*:}"
      row=$(echo "$DEPLOY_JSON" | jq -r --arg n "$svc" '.items[] | select(.metadata.name==$n) | "\(.spec.replicas // 0) \(.status.readyReplicas // 0)"')
      if [ -z "$row" ]; then
        if is_optional "$svc"; then warn "[$tier] $svc MISSING (optional)"; else fail "[$tier] $svc MISSING (no deployment)"; fi
        continue
      fi
      desired=$(echo "$row" | awk '{print $1}'); ready=$(echo "$row" | awk '{print $2}')
      if [ "$desired" = "0" ]; then
        if is_optional "$svc"; then warn "[$tier] $svc scaled to 0 (optional)"; else fail "[$tier] $svc scaled to 0"; fi
      elif [ "$ready" = "$desired" ]; then ok "[$tier] $svc $ready/$desired ready"
      else fail "[$tier] $svc only $ready/$desired ready"; fi
    done
    # Restart storms within the last RESTART_WINDOW_MIN minutes only (compare the
    # container's last terminated time to now — a restart from yesterday is not a
    # "recent storm" and must not warn every morning).
    POD_JSON=$(kubectl -n "$NS" get pods -o json 2>/dev/null)
    HOT=$(echo "$POD_JSON" | jq -r --argjson w "$RESTART_WINDOW_MIN" '
      (now) as $n
      | .items[] | . as $p | (.status.containerStatuses // [])[]
      | select(.restartCount > 0)
      | (.lastState.terminated.finishedAt // "") as $fin
      | select($fin != "" and (($n - ($fin|fromdateiso8601)) <= ($w * 60)))
      | "\($p.metadata.name) restarts=\(.restartCount)"' 2>/dev/null | grep -viE 'ibkr' | head -10)
    [ -n "$HOT" ] && warn "pod restarts in last ${RESTART_WINDOW_MIN}m: $(echo "$HOT" | tr '\n' '; ')"
  fi
else
  fail "kubectl/jq missing — cannot assess service readiness"
fi

# ---------------------------------------------------------------------------
# 4. Consumer lag (advisory pre-open) + DLQ
# ---------------------------------------------------------------------------
if have "$KAFKA_CG"; then
  LAGGED=$("$KAFKA_CG" --bootstrap-server "$KAFKA_BOOTSTRAP" --all-groups --describe 2>/dev/null \
    | awk -v th="$LAG_THRESHOLD" 'NR>1 && $6 ~ /^[0-9]+$/ && $6+0 > th {print $1":"$6}' \
    | grep -viE 'ibkr|replay|smoke' | sort -u | head -10)
  [ -n "$LAGGED" ] && warn "consumer lag > $LAG_THRESHOLD: $(echo "$LAGGED" | tr '\n' ' ')"
fi
if have "$KAFKA_GETOFF"; then
  for dlq in options.hpsf.dlq options.databento.hpsf.dlq; do
    END=$("$KAFKA_GETOFF" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$dlq" 2>/dev/null \
      | awk -F: '{s+=$3} END{print s+0}')
    [ -n "$END" ] && [ "$END" -gt 0 ] 2>/dev/null && warn "DLQ $dlq has $END records"
  done
fi

# ---------------------------------------------------------------------------
# 5. Expiry rollover detection (WARN only until gateway-consumes-control lands)
# ---------------------------------------------------------------------------
if [ -n "$GATEWAY_CONFIG_URL" ] && [ -n "$TODAY_EXPIRY" ] && have curl; then
  CFG=$(curl -fsS --max-time 8 "$GATEWAY_CONFIG_URL" 2>/dev/null)
  SEL=$(echo "$CFG" | grep -oE '"expiry"[^,}]*' | head -1 | grep -oE '[0-9]{8}' | head -1)
  if [ -n "$SEL" ]; then
    if [ "$SEL" = "$TODAY_EXPIRY" ]; then ok "UI expiry == today ($TODAY_EXPIRY)"
    else warn "UI expiry STALE: gateway=$SEL, today=$TODAY_EXPIRY — needs rollover (control msg / re-resolve)"; fi
  fi
fi

# ---------------------------------------------------------------------------
# 6. Error-log digest (grep -> normalize -> dedupe -> count; IBKR filtered)
# ---------------------------------------------------------------------------
ERROR_DIGEST=""
if have kubectl; then
  tmp=$(mktemp)
  for entry in $PIPELINE_SERVICES; do
    svc="${entry##*:}"
    kubectl -n "$NS" logs "deploy/$svc" --since="$LOG_SINCE" --tail="$LOG_TAIL" --all-containers 2>/dev/null \
      | grep -aE 'ERROR|Exception|FATAL' \
      | grep -avE 'IB[A-Za-z]*Error|clientId|:4001|ibkr' \
      | sed -E 's/[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9:.,]+Z?//g; s/[0-9a-f]{8}-[0-9a-f-]{27,}//g; s/0x[0-9a-f]+//g; s/[0-9]+/#/g' \
      | sed -E "s/^/${svc}| /" >>"$tmp"
  done
  ERROR_DIGEST=$(sort "$tmp" | uniq -c | sort -rn | head -"$TOP_N_ERRORS" \
    | sed -E 's/^ *([0-9]+) (.*)$/  x\1  \2/' | cut -c1-160)
  rm -f "$tmp"
  [ -n "$ERROR_DIGEST" ] && warn "top error signatures present (see digest)"
fi

# ---------------------------------------------------------------------------
# 7. Verdict + Discord card (+ Haiku triage on red)
# ---------------------------------------------------------------------------
STATUS="GO"; COLOR=3066993   # green
[ ${#FAILS[@]} -gt 0 ] && { STATUS="NOT-READY"; COLOR=15158332; }   # red
# Show the real hours-to-open when the calendar guard succeeded; otherwise say so explicitly.
# A bare "T-?h" was opaque — an empty HOURS_TO_OPEN always means the calendar guard returned
# non-OK (stale/missing market_calendar.py, bad CALENDAR_DIR, or a calendar crash), which is
# ALSO why STATUS is NOT-READY (the fail-closed catch-all above). Make the cause visible.
if [ -n "$HOURS_TO_OPEN" ]; then
  TITLE="$STATUS — pre-market (T-${HOURS_TO_OPEN}h to open)"
else
  TITLE="$STATUS — pre-market (hours-to-open unknown — calendar guard failed)"
fi

# Plain-text body (also the Haiku input on red). Always show the passed COUNT so
# a NOT-READY card still proves the full sweep ran; list failures/warns in detail.
BODY="✅ ${#OKS[@]} checks passed, ❌ ${#FAILS[@]} failed, ⚠️ ${#WARNS[@]} warnings.\n"
[ ${#FAILS[@]} -gt 0 ] && BODY+="❌ FAILURES:\n$(printf '  - %s\n' "${FAILS[@]}")\n"
[ ${#WARNS[@]} -gt 0 ] && BODY+="⚠️ WARN:\n$(printf '  - %s\n' "${WARNS[@]}")\n"
[ ${#P3_BLOCKED[@]} -gt 0 ] && BODY+="🔒 P3-blocked (reset prerequisites):\n$(printf '  - %s\n' "${P3_BLOCKED[@]}")\n"
# On GO, list what passed (terse confirmation the pipeline is up).
[ "$STATUS" = "GO" ] && [ ${#OKS[@]} -gt 0 ] && BODY+="passed:\n$(printf '  - %s\n' "${OKS[@]}")\n"
[ -n "$ERROR_DIGEST" ] && BODY+="🪵 errors:\n$ERROR_DIGEST\n"

TRIAGE=""
if [ "$STATUS" = "NOT-READY" ] && [ -n "$ANTHROPIC_API_KEY" ] && have curl && have jq; then
  PROMPT="Pre-market readiness FAILED. In 2-3 terse sentences, give the most likely root cause and the first action. Failures + context follow:\n${BODY}"
  REQ=$(jq -n --arg m "$HAIKU_MODEL" --arg p "$PROMPT" \
    '{model:$m, max_tokens:300, messages:[{role:"user", content:$p}]}')
  RESP=$(curl -sS --max-time 25 https://api.anthropic.com/v1/messages \
    -H "x-api-key: $ANTHROPIC_API_KEY" -H "anthropic-version: 2023-06-01" \
    -H "content-type: application/json" -d "$REQ" 2>/dev/null)
  TRIAGE=$(echo "$RESP" | jq -r '.content[0].text // empty' 2>/dev/null)
  [ -n "$TRIAGE" ] && BODY+="🧭 triage: $TRIAGE\n"
fi

echo "=========================================================="
echo "$TITLE"
echo "----------------------------------------------------------"
printf '%b\n' "$BODY"
echo "=========================================================="

if [ -n "$DISCORD_WEBHOOK_URL" ] && have curl && have jq; then
  DESC=$(printf '%b' "$BODY" | head -c 3900)
  PAYLOAD=$(jq -n --arg t "$TITLE" --arg d "$DESC" --argjson c "$COLOR" \
    '{embeds:[{title:$t, description:$d, color:$c}]}')
  curl -fsS --max-time 12 -H "Content-Type: application/json" \
    -d "$PAYLOAD" "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 \
    && echo "[premarket] posted to Discord" \
    || echo "[premarket] WARN: Discord post failed" >&2
fi

[ "$STATUS" = "GO" ] && exit 0 || exit 1
