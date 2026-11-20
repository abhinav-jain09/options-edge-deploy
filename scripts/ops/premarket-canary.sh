#!/usr/bin/env bash
# Pre-market END-TO-END CANARY for the OptionsEdge Databento pipeline.
#
# Proves data actually FLOWS (not just that pods are up): injects ONE synthetic
# OptionStrikeSnapshot into options.databento.raw at a FAR-OTM strike (default
# 100.0 — far from the ~6000 spot, so it is obviously synthetic, never collides
# with a real tradeable strike, and does not move near-the-money GEX/maxpain/pin),
# then confirms the downstream topics that consume raw advance their offsets.
#
# Verification = offset-advance (flow proof): snapshot end-offsets of the
# raw-consuming topics, inject, wait, re-check. GO if >= MIN_ADVANCED advanced.
#
# Read-only-ish: the only write is the single synthetic record (and the topics it
# fans out to). The far strike makes it identifiable; the T-4h reset / real feed
# overwrite it. DRY_RUN=true logs the sample without producing.
set -uo pipefail

# ---- config (override via env) ----
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092,192.168.100.4:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://192.168.100.252:8082}"
RAW_TOPIC="${RAW_TOPIC:-options.databento.raw}"
DOWNSTREAM="${DOWNSTREAM:-options.databento.display options.databento.directional-pressure options.databento.gex.strike options.databento.pace options.databento.maxpain}"
CANARY_SYMBOL="${CANARY_SYMBOL:-SPX}"
CANARY_STRIKE="${CANARY_STRIKE:-100.0}"     # far OTM, identifiable, never a real strike
CANARY_EXPIRY="${CANARY_EXPIRY:-}"          # default: today's trading date
WAIT_S="${WAIT_S:-20}"
MIN_ADVANCED="${MIN_ADVANCED:-1}"           # how many downstream topics must advance for GO
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../jenkins" && pwd)}"
DRY_RUN="${DRY_RUN:-true}"
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

PRODUCER="$KAFKA_BIN/kafka-avro-console-producer.sh"
KGO="$KAFKA_BIN/kafka-get-offsets.sh"
ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] [canary] $*"; }
discord() {
  [ -n "$DISCORD_WEBHOOK_URL" ] || return 0; command -v curl >/dev/null 2>&1 || return 0
  printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
    | curl -fsS --max-time 10 -K - -H 'Content-Type: application/json' \
        -d "{\"username\":\"OptionsEdge Ops\",\"content\":\"$1\"}" >/dev/null 2>&1 || log "WARN: Discord post failed"
}
# Sum of end-offsets across all partitions of a topic. Prints "ERR" on a read
# failure / absent topic, so callers don't mistake a failed read for offset 0
# (which would fabricate a delta). An empty topic legitimately returns 0.
end_sum() {
  local out
  out=$("$KGO" --bootstrap-server "$BOOTSTRAP" --topic "$1" --time -1 2>/dev/null) || { echo ERR; return; }
  [ -n "$out" ] || { echo ERR; return; }
  printf '%s\n' "$out" | awk -F: '{s+=$3} END{print s+0}'
}

# ---- today's expiry (for a realistic, processable sample) ----
if [ -z "$CANARY_EXPIRY" ]; then
  if command -v python3 >/dev/null 2>&1; then
    CANARY_EXPIRY=$(CALENDAR_DIR="$CALENDAR_DIR" python3 - <<'PY'
import os,sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
from datetime import datetime
from zoneinfo import ZoneInfo
from market_calendar import MarketCalendar
print(MarketCalendar().current_trading_date(datetime.now(ZoneInfo("America/New_York"))).strftime("%Y%m%d"))
PY
)
  fi
  [ -n "$CANARY_EXPIRY" ] || CANARY_EXPIRY="$(date -u +%Y%m%d)"
fi

NOW_ISO="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
# Fetch the registered raw schema (also reused by the producer below).
command -v python3 >/dev/null 2>&1 || { log "FATAL: python3 required"; exit 1; }
command -v curl    >/dev/null 2>&1 || { log "FATAL: curl required"; exit 1; }
SCHEMA=$(curl -fsS --max-time 8 "$SCHEMA_REGISTRY_URL/subjects/${RAW_TOPIC}-value/versions/latest" 2>/dev/null \
         | python3 -c "import json,sys; print(json.load(sys.stdin)['schema'])") \
  || { log "FATAL: could not fetch raw value schema from $SCHEMA_REGISTRY_URL"; exit 1; }

# Build a COMPLETE Avro-JSON record by walking the schema: every field must be
# present (the decoder does NOT fill defaults). Required fields come from our
# overrides; everything else takes its schema default. This is schema-version
# proof — new optional fields are auto-filled.
SAMPLE=$(SCHEMA="$SCHEMA" SYMBOL="$CANARY_SYMBOL" EXPIRY="$CANARY_EXPIRY" STRIKE="$CANARY_STRIKE" NOW="$NOW_ISO" python3 <<'PY'
import json, os
sch = json.loads(os.environ["SCHEMA"]); now = os.environ["NOW"]

# Register every named record so string type-references (e.g. "put":"OptionContractMetrics") resolve.
REG = {}
def register(t):
    if isinstance(t, dict) and t.get("type") == "record":
        REG[t["name"]] = t
        if t.get("namespace"): REG[t["namespace"]+"."+t["name"]] = t
        for f in t["fields"]: register(f["type"])
    elif isinstance(t, list):
        for x in t: register(x)
register(sch)

# Top-level scalar overrides; per-record value maps (so last/mark don't bleed across records).
TOP = {"schemaVersion":3,"messageType":"OPTION_STRIKE_SNAPSHOT","capturedAt":now,"eventTime":now,
       "receiveTime":now,"eventTimeSource":"CANARY","source":"DATABENTO","symbol":os.environ["SYMBOL"],
       "expiry":os.environ["EXPIRY"],"strike":float(os.environ["STRIKE"]),"currency":"USD","exchange":"OPRA","multiplier":100}
REC = {"UnderlyingSnapshot":{"last":6000.0,"mark":6000.0},
       "OptionContractMetrics":{"bid":0.05,"ask":0.10,"last":0.07,"mark":0.075,"volume":1,"openInterest":1,"delta":0.0,"gamma":0.0001}}
def prim(t):
    if isinstance(t, list): return None if "null" in t else prim(t[0])
    return {"int":0,"long":0,"double":0.0,"float":0.0,"string":"","boolean":False}.get(t, "")
def build(rec):
    vals = REC.get(rec["name"], {}); o = {}
    for f in rec["fields"]:
        n, t = f["name"], f["type"]
        if isinstance(t, dict) and t.get("type")=="record": o[n] = build(t)
        elif isinstance(t, str) and t in REG:               o[n] = build(REG[t])   # named ref (e.g. put)
        elif n in vals:                                     o[n] = vals[n]
        elif rec["name"]=="OptionStrikeSnapshot" and n in TOP: o[n] = TOP[n]
        elif "default" in f:                                o[n] = f["default"]
        else:                                               o[n] = prim(t)
    return o
print(json.dumps(build(sch)))
PY
)
[ -n "$SAMPLE" ] || { log "FATAL: failed to build sample record"; exit 1; }

log "raw topic = $RAW_TOPIC  symbol=$CANARY_SYMBOL expiry=$CANARY_EXPIRY strike=$CANARY_STRIKE (synthetic far-OTM)"
log "downstream watched: $DOWNSTREAM"

if [ "$DRY_RUN" = "true" ]; then
  log "DRY-RUN: would inject the sample below and verify downstream offsets advance. Nothing produced, no alert."
  log "sample = $SAMPLE"
  exit 0
fi

# ---- snapshot downstream offsets BEFORE ----
declare -A BEFORE
for t in $DOWNSTREAM; do BEFORE[$t]=$(end_sum "$t"); done

# ---- inject one synthetic record (Avro, registered schema fetched above) ----
[ -x "$PRODUCER" ] || { log "FATAL: avro producer not found at $PRODUCER"; exit 1; }
if printf '%s\n' "$SAMPLE" | "$PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic "$RAW_TOPIC" \
     --property schema.registry.url="$SCHEMA_REGISTRY_URL" --property value.schema="$SCHEMA" >/dev/null 2>&1; then
  log "injected synthetic snapshot into $RAW_TOPIC"
else
  log "FATAL: failed to inject synthetic snapshot"
  discord "❌ Canary FAILED to inject into $RAW_TOPIC (producer error)."
  exit 1
fi

# ---- wait, then re-check downstream offsets ----
# NOTE: this attributes any downstream advance to the injected record, which is
# sound ONLY when the pipeline is otherwise idle — i.e. PRE-OPEN, exactly when
# this runs. If run during an active feed, unrelated production could advance a
# topic; for that case raise MIN_ADVANCED or use content correlation instead.
log "waiting ${WAIT_S}s for propagation"; sleep "$WAIT_S"
ADVANCED=0; ERRORS=0; REPORT=""
for t in $DOWNSTREAM; do
  after=$(end_sum "$t"); before=${BEFORE[$t]:-ERR}
  if [ "$before" = "ERR" ] || [ "$after" = "ERR" ]; then ERRORS=$((ERRORS+1)); REPORT="$REPORT  ❓ $t (offset read failed)\n"; continue; fi
  d=$((after-before))
  if [ "$d" -gt 0 ]; then ADVANCED=$((ADVANCED+1)); REPORT="$REPORT  ✅ $t (+$d)\n"; else REPORT="$REPORT  ⚠️ $t (no advance)\n"; fi
done

echo "=== canary result: $ADVANCED advanced, $ERRORS read-errors / $(echo $DOWNSTREAM | wc -w) watched ==="
printf '%b' "$REPORT"
# Inconclusive (can't read offsets) is NOT a pass.
if [ "$ADVANCED" -lt "$MIN_ADVANCED" ] && [ "$ERRORS" -gt 0 ]; then
  discord "❓ Canary INCONCLUSIVE — $ERRORS downstream offset reads failed (Kafka/admin issue). Not a GO."
  exit 1
fi
if [ "$ADVANCED" -ge "$MIN_ADVANCED" ]; then
  discord "🐤 Canary GO — synthetic $CANARY_SYMBOL flowed through; $ADVANCED downstream topics advanced."
  exit 0
else
  discord "❌ Canary NOT-READY — synthetic record did NOT propagate ($ADVANCED advanced, need $MIN_ADVANCED). Pipeline may be stalled."
  exit 1
fi
