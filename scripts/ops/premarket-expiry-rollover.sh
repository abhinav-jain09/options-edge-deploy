#!/usr/bin/env bash
# Pre-market expiry rollover (P2) for the OptionsEdge Databento pipeline.
#
# Publishes a ContractSelection control message with TODAY's trading date so the
# live Databento feed reconnects to today's 0DTE chain — the same mechanism the
# UI date-picker uses (options.databento.control -> request_contract_selection()).
# This fixes the "expiry did not flip" problem WITHOUT a redeploy: the feed is a
# long-lived pod with no calendar of its own; left alone it sits on whatever
# expiry it started with.
#
# Runs on the prod host (via Jenkins, or as the final step of premarket-reset.sh
# after a topic wipe — which is why the publish must come LAST). Idempotent: a
# second publish for the same expiry is a no-op for the feed.
#
# DRY_RUN=true (default) only logs the message it would publish.
set -uo pipefail

# ---- config (override via env) ----
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.4:9092}"
CONTROL_TOPIC="${CONTROL_TOPIC:-options.databento.control}"   # dev: dev.options.databento.control
SYMBOL="${SYMBOL:-SPX}"
SOURCE="${MARKET_DATA_SOURCE:-DATABENTO}"
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../jenkins" && pwd)}"
DRY_RUN="${DRY_RUN:-true}"
EXPIRY_OVERRIDE="${EXPIRY:-}"                                 # YYYYMMDD; empty -> compute from calendar
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

PRODUCER="$KAFKA_BIN/kafka-console-producer.sh"
ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] [expiry] $*"; }
discord() {
  [ -n "$DISCORD_WEBHOOK_URL" ] || return 0; command -v curl >/dev/null 2>&1 || return 0
  printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
    | curl -fsS --max-time 10 -K - -H 'Content-Type: application/json' \
        -d "{\"username\":\"OptionsEdge Ops\",\"content\":\"$1\"}" >/dev/null 2>&1 \
    || log "WARN: Discord post failed"
}

# ---- resolve today's trading-day expiry (YYYYMMDD) ----
if [ -n "$EXPIRY_OVERRIDE" ]; then
  if ! printf '%s' "$EXPIRY_OVERRIDE" | grep -Eq '^[0-9]{8}$'; then
    log "FATAL: EXPIRY override '$EXPIRY_OVERRIDE' is not YYYYMMDD"; exit 1
  fi
  EXPIRY="$EXPIRY_OVERRIDE"
else
  command -v python3 >/dev/null 2>&1 || { log "FATAL: python3 required to resolve expiry"; exit 1; }
  EXPIRY=$(CALENDAR_DIR="$CALENDAR_DIR" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
from datetime import datetime
from zoneinfo import ZoneInfo
from market_calendar import MarketCalendar
cal = MarketCalendar()
now = datetime.now(ZoneInfo("America/New_York"))
# current_trading_date rolls to the next session after close; pre-open it is today.
print(cal.current_trading_date(now).strftime("%Y%m%d"))
PY
)
  if ! printf '%s' "$EXPIRY" | grep -Eq '^[0-9]{8}$'; then
    log "FATAL: could not resolve today's expiry (got '$EXPIRY')"; exit 1
  fi
fi

REQUESTED_AT=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
MSG=$(printf '{"type":"select-contract","source":"%s","symbol":"%s","expiry":"%s","requestedAt":"%s"}' \
  "$SOURCE" "$SYMBOL" "$EXPIRY" "$REQUESTED_AT")

log "control topic = $CONTROL_TOPIC"
log "message       = $MSG"

if [ "$DRY_RUN" = "true" ]; then
  log "DRY-RUN: would publish the above ContractSelection. No message sent."
  discord "🧪 Expiry rollover DRY-RUN — would select ${SYMBOL} ${EXPIRY} (${SOURCE}); nothing published."
  exit 0
fi

[ -x "$PRODUCER" ] || { log "FATAL: producer not found at $PRODUCER"; exit 1; }
if printf '%s\n' "$MSG" | "$PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic "$CONTROL_TOPIC" >/dev/null 2>&1; then
  log "published ContractSelection: ${SYMBOL} ${EXPIRY}"
  discord "🔁 Expiry rolled to today — selected ${SYMBOL} **${EXPIRY}** (${SOURCE}) via ${CONTROL_TOPIC}."
else
  log "FATAL: failed to publish to $CONTROL_TOPIC"
  discord "❌ Expiry rollover FAILED to publish ${SYMBOL} ${EXPIRY} to ${CONTROL_TOPIC}."
  exit 1
fi
