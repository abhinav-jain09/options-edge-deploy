#!/usr/bin/env bash
# Pre-market DESTRUCTIVE clean-slate reset (P3) for the OptionsEdge Databento
# pipeline. Runs T-4h before the open (via Jenkins SSH on the prod host), once
# per trading day, BEFORE the read-only readiness sweeps verify the result.
#
# It performs, in order (each step a no-op under DRY_RUN):
#   1. identity guards (env + Kafka cluster-id + DB name + trading-date + RESET_ENABLED)
#   2. pre-intent audit row
#   3. scale the pipeline Deployments to 0 (snapshot replica counts first)
#   4. purge EVERY non-system Kafka topic (delete-records to high-water)
#   5. reset all consumer groups to latest (groups are idle now)
#   6. TRUNCATE the intraday Postgres allow-list (NOT pin_session_close)
#   7. scale Deployments back to their original counts; wait for rollout
#   8. publish today's expiry ContractSelection (LAST — control topic was wiped)
#   9. final audit + Discord summary
#
# SAFETY:
#   * DRY_RUN=true by default — logs everything it WOULD do, mutates nothing.
#   * RESET_ENABLED must be "true" AND all identity guards must pass before any
#     mutation. A wrong KAFKA_BOOTSTRAP / PG_DSN cannot wipe another cluster.
#   * pin_session_close is never truncated (and the dedicated role has no grant
#     to do so — see premarket-reset-role.sql).
#   * scale-to-0 happens ~4h before the open, a wide recovery window; a failed
#     scale-back hard-alerts so the T-3h readiness sweep catches it.
set -uo pipefail

# ---- config (override via env) ----
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/kafka_2.13-4.3.0/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.252:9092}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="${K8S_NAMESPACE:-options-edge}"
SELECTOR="${APP_SELECTOR:-app.kubernetes.io/part-of=options-edge}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-240}"
SCALEDOWN_WAIT="${SCALEDOWN_WAIT:-60}"
CALENDAR_DIR="${CALENDAR_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../jenkins" && pwd)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DRY_RUN="${DRY_RUN:-true}"
RESET_ENABLED="${RESET_ENABLED:-false}"
PREMARKET_FORCE="${PREMARKET_FORCE:-0}"

# Identity guard expectations (fail-closed). The REAL environment identity is the
# Kafka cluster-id + the DB server host + the DB name + the connected role — not a
# free-text "env" string (which would be trivially wrong). All are REQUIRED to
# match before any live mutation.
EXPECTED_ENV="${EXPECTED_ENV:-prod}"                         # label only
EXPECTED_KAFKA_CLUSTER_ID="${EXPECTED_KAFKA_CLUSTER_ID:-}"   # REQUIRED for live mutate
EXPECTED_DB="${EXPECTED_DB:-options_flow}"
EXPECTED_DB_HOST="${EXPECTED_DB_HOST:-}"                     # REQUIRED for live (e.g. 192.168.100.252)
EXPECTED_DB_USER="${EXPECTED_DB_USER:-premarket_reset}"     # the least-priv role (cannot touch pin_session_close)
PG_DSN="${PG_DSN:-}"                                          # premarket_reset role DSN (REQUIRED for live)

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

# Intraday truncate allow-list — MUST match premarket-reset-role.sql. NO CASCADE.
# pin_session_close intentionally EXCLUDED (cross-day realized-close labels).
TRUNCATE_TABLES="${TRUNCATE_TABLES:-\
hpsf_signal hpsf_latest_signal hpsf_market_flow hpsf_strike_score hpsf_strike_flow \
hpsf_exit_signal hpsf_dlq hpsf_writer_dlq \
pin_strike_minute pin_uw_crosscheck_minute pin_self_gex_minute pin_market_minute pin_strike_fine \
databento_option_raw_snapshot databento_option_raw_quarantine}"

# HARD DENY: pin_session_close must never be truncated, even via a bad override.
# (Belt to the role's braces — the premarket_reset role also lacks the grant.)
case " $TRUNCATE_TABLES " in
  *" pin_session_close "*)
    echo "FATAL: pin_session_close present in TRUNCATE_TABLES — refusing (preserve realized-close labels)" >&2
    exit 1 ;;
esac

KT="$KAFKA_BIN/kafka-topics.sh"
KGO="$KAFKA_BIN/kafka-get-offsets.sh"
KDR="$KAFKA_BIN/kafka-delete-records.sh"
KCG="$KAFKA_BIN/kafka-consumer-groups.sh"
KCL="$KAFKA_BIN/kafka-cluster.sh"

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { echo "[$(ts)] [reset] $*"; }
die() { log "FATAL: $*"; discord "❌ Pre-market reset ABORTED: $*"; exit 1; }
discord() {
  [ -n "$DISCORD_WEBHOOK_URL" ] || return 0; command -v curl >/dev/null 2>&1 || return 0
  printf 'url = "%s"\n' "$DISCORD_WEBHOOK_URL" \
    | curl -fsS --max-time 10 -K - -H 'Content-Type: application/json' \
        -d "{\"username\":\"OptionsEdge Ops\",\"content\":\"$1\"}" >/dev/null 2>&1 || log "WARN: Discord post failed"
}
# psql helper. -tAc = tuples-only, unaligned, single command. ON_ERROR_STOP so a
# failed statement is a non-zero exit (callers treat that as fatal).
pg() { psql "$PG_DSN" -v ON_ERROR_STOP=1 -tAc "$1"; }

MUTATE="true"; [ "$DRY_RUN" = "true" ] && MUTATE="false"

# ---------------------------------------------------------------------------
# Single-instance lock
# ---------------------------------------------------------------------------
LOCK="/tmp/premarket-reset.lock"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK" || exit 1
  flock -n 9 || { log "another reset is already running; exiting"; exit 0; }
else
  log "WARN: flock not available — single-instance lock skipped (expected only off-host)"
fi

log "=== pre-market reset start (DRY_RUN=$DRY_RUN RESET_ENABLED=$RESET_ENABLED) ==="

# ---------------------------------------------------------------------------
# 0. Trading-day guard + today's expiry
# ---------------------------------------------------------------------------
command -v python3 >/dev/null 2>&1 || die "python3 required"
GUARD=$(CALENDAR_DIR="$CALENDAR_DIR" python3 - <<'PY'
import os, sys
sys.path.insert(0, os.environ["CALENDAR_DIR"])
from datetime import datetime
from zoneinfo import ZoneInfo
from market_calendar import MarketCalendar
cal = MarketCalendar()
now = datetime.now(ZoneInfo("America/New_York"))
print(("TRADING " if cal.is_trading_day(now.date()) else "SKIP ") + cal.current_trading_date(now).strftime("%Y%m%d"))
PY
)
TODAY_EXPIRY="${GUARD##* }"
if [[ "$GUARD" == SKIP* ]]; then
  # A live reset on a non-trading day is NEVER allowed — PREMARKET_FORCE only
  # lets a DRY-RUN proceed for manual verification.
  if [ "$MUTATE" = "true" ]; then die "refusing LIVE reset on a non-trading day ($GUARD)"; fi
  if [ "$PREMARKET_FORCE" != "1" ]; then log "not a trading day ($GUARD) — exiting 0, no reset"; exit 0; fi
  log "PREMARKET_FORCE=1 — continuing DRY-RUN only, despite non-trading day"
fi
log "trading date / expiry = $TODAY_EXPIRY"

# ---------------------------------------------------------------------------
# 1. Identity guards (fail-closed). Read-only — always run, even in DRY_RUN.
# ---------------------------------------------------------------------------
# Kafka reachable + cluster-id match
command -v "$KT" >/dev/null 2>&1 || [ -x "$KT" ] || die "kafka CLI missing at $KAFKA_BIN"
"$KT" --bootstrap-server "$BOOTSTRAP" --list >/tmp/pmr-all-topics.txt 2>/dev/null \
  || die "cannot reach Kafka at $BOOTSTRAP"
if [ -n "$EXPECTED_KAFKA_CLUSTER_ID" ] && [ -x "$KCL" ]; then
  CID=$("$KCL" cluster-id --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -oE 'Cluster ID:.*' | awk '{print $3}')
  [ "$CID" = "$EXPECTED_KAFKA_CLUSTER_ID" ] || die "Kafka cluster-id mismatch (got '$CID', expected '$EXPECTED_KAFKA_CLUSTER_ID')"
  log "kafka cluster-id verified ($CID)"
elif [ "$MUTATE" = "true" ]; then
  die "EXPECTED_KAFKA_CLUSTER_ID unset — refusing to wipe without cluster identity"
fi

# k8s reachable
kubectl -n "$NS" get deploy >/dev/null 2>&1 || die "cannot reach k8s namespace $NS"

# DB identity (required for live mutate): verify host + db name + connected role,
# not just the database name (another instance could also be named options_flow).
if [ "$MUTATE" = "true" ]; then
  [ -n "$PG_DSN" ] || die "PG_DSN unset — refusing to truncate without DB identity"
  [ -n "$EXPECTED_DB_HOST" ] || die "EXPECTED_DB_HOST unset — refusing without DB host identity"
  command -v psql >/dev/null 2>&1 || die "psql missing"
  case "$PG_DSN" in *"$EXPECTED_DB_HOST"*) : ;; *) die "PG_DSN host does not contain EXPECTED_DB_HOST=$EXPECTED_DB_HOST" ;; esac
  # current_database | server addr (empty for unix socket) | connected role
  DBINFO=$(pg "select current_database()||'|'||coalesce(host(inet_server_addr()),'')||'|'||current_user") \
    || die "Postgres unreachable / identity query failed (is premarket.reset_audit present?)"
  DBN=${DBINFO%%|*}; _rest=${DBINFO#*|}; SRV=${_rest%%|*}; USR=${_rest##*|}
  [ "$DBN" = "$EXPECTED_DB" ] || die "DB name mismatch (got '$DBN', expected '$EXPECTED_DB')"
  [ -z "$SRV" ] || [ "$SRV" = "$EXPECTED_DB_HOST" ] || die "DB server addr mismatch (got '$SRV', expected '$EXPECTED_DB_HOST')"
  [ "$USR" = "$EXPECTED_DB_USER" ] || die "connected as '$USR', expected least-priv role '$EXPECTED_DB_USER'"
  log "Postgres identity verified (db=$DBN host=${SRV:-socket} role=$USR)"
  [ "$RESET_ENABLED" = "true" ] || die "RESET_ENABLED!=true — guards passed but mutation not enabled"

  # Idempotency: refuse if a reset already ran today. A failed audit query is
  # FATAL (we will not mutate without a working audit trail). No FORCE bypass on
  # the live path — to re-run, delete today's audit row deliberately.
  EXISTING=$(pg "select count(*) from premarket.reset_audit where trade_date = to_date('$TODAY_EXPIRY','YYYYMMDD') and status in ('INTENT','DONE')" | tr -d '[:space:]') \
    || die "audit/idempotency query failed"
  case "$EXISTING" in ''|*[!0-9]*) die "audit/idempotency query returned non-numeric ('$EXISTING')" ;; esac
  if [ "$EXISTING" != "0" ]; then
    log "reset already recorded for $TODAY_EXPIRY (count=$EXISTING) — skipping (idempotent)"; exit 0
  fi
fi
log "identity guards PASSED"

# Candidate topics = everything except Kafka system topics and the explicit
# DURABLE keep-list. es.reversal.final-summary + es.reversal.outcome are the
# CALIBRATION topics: compacted, bounded corpora whose entire purpose is
# cross-day accumulation (the reversal engine's ground-truth dataset, design
# §14 promotion gate) — same preservation principle as the clean-slate
# dealer-ledger exemption. The ARMED-HUNT program adds its contract-mandated
# purge-exempt topics: es.reversal.hunt.state (compacted derived hunt state —
# an armed hunt must survive the nightly reset), es.reversal.hunt.alerts
# (7 d one-shot transitions, delete-cleaned but cross-day), and the wall
# break-rate dataset (compacted, versioned). Rolling streams
# (es.reversal.verdicts/strength, hunt status/commands) stay purgeable by
# design.
#
# spx.basis.state (2026-07-28) is the ES->SPX basis engine's CROSS-DAY durable
# state by design (retention.ms=-1: STATE_CURRENT restart authority, per-
# generation STATE history, daily close anchors/acks, A1 certificates —
# ES-SPX-TRANSLATION-ENGINE.md §3.3/§4). Purging it cold-starts the basis every
# morning: the feed's boot scan finds no STATE_CURRENT, the engine serves
# UNAVAILABLE/COLD_START instead of PROJECTED off yesterday's measurement, and
# the ES-chain "SPX ≈" mapping is dark from open until the day's A1 cert +
# first fresh measurement (~30-40 min into the session). The topic is tiny
# (one partition, a few MB/day of state records) — nothing here needs a
# pre-market wipe. Found 2026-07-28: this purge was why the mapping vanished
# overnight after the 07-27 restoration.
# underlying.vix.price added 2026-07-28: declared retention=-1 in topics.env
# (prod-only overrides, VIX feed separation) — declared-durable topics must never
# be purged; drift is now unmergeable (scripts/ci/validate-durable-topic-preservation.sh).
# options.spx.gamma-migration.scoring added 2026-07-31: declared retention=-1 in topics.env. It
# is the gamma-migration falsification record (GMS-R9), accumulated ACROSS sessions — purging it
# nightly would leave it permanently one session deep, which is the same as not having it.
# The gamma-migration SCORER changelog is preserved for the same reason as the scoring topic
# itself: it holds the open +5/+15/+30 horizons, which are recomputable from nothing. The pattern
# is deliberately narrow — a broad `gamma-migration.*-changelog` also matched the SESSION
# changelog, which would have carried yesterday's dwell and ladder into today through a reset
# whose whole purpose is to start clean.
PRESERVE_TOPICS_REGEX='^es\.reversal\.(final-summary|outcome)$|^es\.reversal\.hunt\.(state|alerts)$|^options\.spx\.wall-break-rates\.dataset$|^(es|spx)\.drop\.(final-summary|outcome)$|^spx\.basis\.state$|^underlying\.vix\.price$|^options\.spx\.gamma-migration\.scoring$|^options\.databento\.oi\.anchor-manifest$|gamma-migration-scorer-changelog$'
N_PRESERVED=$(grep -vE '^(__|_schemas$)' /tmp/pmr-all-topics.txt | grep -cE "$PRESERVE_TOPICS_REGEX" || true)
log "durable keep-list: preserving $N_PRESERVED topic(s) matching $PRESERVE_TOPICS_REGEX"
CANDIDATES=$(grep -vE '^(__|_schemas$)' /tmp/pmr-all-topics.txt | grep -vE "$PRESERVE_TOPICS_REGEX" || true)
NTOPICS=$(printf '%s\n' "$CANDIDATES" | grep -c . || true)
log "topics to purge: $NTOPICS (system topics excluded)"

if [ "$MUTATE" = "false" ]; then
  log "DRY-RUN — would now: scale '$SELECTOR' to 0; delete-records on $NTOPICS topics;"
  log "DRY-RUN   reset all consumer groups to latest; TRUNCATE $(echo $TRUNCATE_TABLES | wc -w) tables;"
  log "DRY-RUN   scale back up; publish expiry $TODAY_EXPIRY. No changes made."
  discord "🧪 Pre-market reset DRY-RUN ($TODAY_EXPIRY) — would purge ${NTOPICS} topics + truncate $(echo $TRUNCATE_TABLES | wc -w) tables. No changes."
  exit 0
fi

# ===========================================================================
# LIVE MUTATION PATH (guards passed, RESET_ENABLED=true, DRY_RUN=false)
# ===========================================================================
AUDIT_ID=$(pg "insert into premarket.reset_audit(trade_date,status) values (to_date('$TODAY_EXPIRY','YYYYMMDD'),'INTENT') returning id" | tr -d '[:space:]') \
  || die "failed to write audit intent row — refusing to mutate without an audit trail"
case "$AUDIT_ID" in ''|*[!0-9]*) die "audit intent returned non-numeric id ('$AUDIT_ID')" ;; esac
log "audit intent row id=$AUDIT_ID"

# Snapshot replica counts FIRST (before any scaling), so restore always has them.
SNAP=$(kubectl -n "$NS" get deploy -l "$SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.replicas}{"\n"}{end}')
[ -n "$SNAP" ] || die "no deployments matched $SELECTOR"

SCALED_DOWN=0
restore_scale() {
  [ "$SCALED_DOWN" = "1" ] || return 0
  log "restoring deployment replica counts (scale-back)"
  while read -r name reps; do [ -n "$name" ] && kubectl -n "$NS" scale deploy "$name" --replicas="${reps:-1}" >/dev/null 2>&1; done <<<"$SNAP"
  SCALED_DOWN=0
}
# On ANY exit after scale-down, ensure deployments are scaled back up so a failed
# wipe never leaves prod down. (No-op once a clean scale-back has run.)
trap 'restore_scale' EXIT
audit_fail() {
  pg "update premarket.reset_audit set status='FAILED', finished_at=now(), detail='$1' where id=$AUDIT_ID" >/dev/null 2>&1 || true
  die "$1"   # die exits -> EXIT trap runs restore_scale
}

# 3. Scale the pipeline to 0 (hard-stop producers/consumers), fail-closed
log "scaling $(printf '%s\n' "$SNAP" | grep -c .) deployments to 0"
SCALED_DOWN=1
while read -r name reps; do
  [ -n "$name" ] || continue
  kubectl -n "$NS" scale deploy "$name" --replicas=0 >/dev/null 2>&1 || audit_fail "scale-to-0 failed for $name"
done <<<"$SNAP"
# Wait for consumers to actually stop (readyReplicas -> 0), not just a blind sleep.
deadline=$(( $(date +%s) + SCALEDOWN_WAIT ))
while :; do
  READY=$(kubectl -n "$NS" get deploy -l "$SELECTOR" -o jsonpath='{range .items[*]}{.status.readyReplicas}{" "}{end}' 2>/dev/null | tr ' ' '\n' | awk '{s+=$1} END{print s+0}')
  [ "${READY:-0}" = "0" ] && { log "all consumers stopped"; break; }
  [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: ${READY} replicas still ready after ${SCALEDOWN_WAIT}s — proceeding"; break; }
  sleep 5
done

# 4. Purge every candidate topic via delete-records to high-water
SPEC=$(mktemp); echo '{"partitions":[' >"$SPEC"; first=1; purged=0
for T in $CANDIDATES; do
  OFFS=$("$KGO" --bootstrap-server "$BOOTSTRAP" --topic "$T" --time -1 2>/dev/null \
         | grep -E '^[^:]+:[0-9]+:[0-9]+$') || continue
  while IFS=: read -r tp part off; do
    [ -z "$tp" ] && continue
    [ "$off" = "0" ] && continue
    [ $first -eq 0 ] && printf ',' >>"$SPEC"
    printf '{"topic":"%s","partition":%s,"offset":%s}' "$tp" "$part" "$off" >>"$SPEC"
    first=0
  done <<<"$OFFS"
  purged=$((purged+1))
done
echo '],"version":1}' >>"$SPEC"
if [ $first -eq 0 ]; then
  "$KDR" --bootstrap-server "$BOOTSTRAP" --offset-json-file "$SPEC" >/dev/null 2>&1 \
    || audit_fail "kafka-delete-records failed"
  log "purged records across $purged topics"
else
  log "no non-empty partitions to purge"
fi
rm -f "$SPEC"

# 5. Reset all consumer groups to latest (groups idle — apps scaled to 0)
for G in $("$KCG" --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null); do
  "$KCG" --bootstrap-server "$BOOTSTRAP" --group "$G" --reset-offsets --all-topics --to-latest --execute >/dev/null 2>&1 \
    || log "WARN: could not reset group $G"
done
log "consumer groups reset to latest"

# 6. TRUNCATE the intraday allow-list (single statement, NO CASCADE)
QUALIFIED=$(for t in $TRUNCATE_TABLES; do printf 'public.%s,' "$t"; done | sed 's/,$//')
pg "set lock_timeout='15s'; set statement_timeout='120s'; truncate table $QUALIFIED restart identity" \
  || audit_fail "TRUNCATE failed"
NTABLES=$(echo $TRUNCATE_TABLES | wc -w | tr -d ' ')
log "truncated $NTABLES tables (pin_session_close preserved)"

# 7. Scale back up to snapshot counts, wait for rollout
log "scaling deployments back up; waiting for rollout"
SCALEBACK_OK=1
while read -r name reps; do
  [ -n "$name" ] || continue
  kubectl -n "$NS" scale deploy "$name" --replicas="${reps:-1}" >/dev/null 2>&1 \
    || { log "WARN: scale-back command failed: $name"; SCALEBACK_OK=0; }
done <<<"$SNAP"
for d in $(printf '%s\n' "$SNAP" | awk '{print $1}'); do
  kubectl -n "$NS" rollout status "deploy/$d" --timeout="${ROLLOUT_TIMEOUT}s" >/dev/null 2>&1 \
    || { log "WARN: rollout slow/incomplete: $d"; SCALEBACK_OK=0; }
done
# Scale-back was issued for every deployment; the EXIT trap must not re-issue it.
SCALED_DOWN=0

# 8. Publish today's expiry LAST (control topic was wiped)
EXPIRY_OK=1
DRY_RUN=false EXPIRY="$TODAY_EXPIRY" CALENDAR_DIR="$CALENDAR_DIR" \
  KAFKA_BIN="$KAFKA_BIN" KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP" DISCORD_WEBHOOK_URL="$DISCORD_WEBHOOK_URL" \
  "$SCRIPT_DIR/premarket-expiry-rollover.sh" \
  || { log "WARN: expiry publish FAILED — control topic was wiped, expiry NOT rolled"; EXPIRY_OK=0; }

# 9. Final audit + Discord. The destructive work succeeded (status DONE), but the
# message is honest about scale-back / expiry-publish problems that need a human.
DETAIL="scaleback_ok=$SCALEBACK_OK expiry_ok=$EXPIRY_OK"
pg "update premarket.reset_audit set status='DONE', finished_at=now(), topics_purged=$purged, tables_truncated=$NTABLES, detail='$DETAIL' where id=$AUDIT_ID" >/dev/null 2>&1 || true
log "=== pre-market reset complete (purged=$purged tables=$NTABLES $DETAIL) ==="
if [ "$SCALEBACK_OK" = "1" ] && [ "$EXPIRY_OK" = "1" ]; then
  discord "🧹 Pre-market reset complete ($TODAY_EXPIRY) — purged **${purged}** topics, truncated **${NTABLES}** tables, apps back up, expiry rolled. ✅"
  exit 0
fi
WARNMSG="⚠️ Pre-market reset ($TODAY_EXPIRY) — purged ${purged} topics, truncated ${NTABLES} tables, but:"
[ "$SCALEBACK_OK" = "1" ] || WARNMSG="$WARNMSG some deployments did NOT roll out;"
[ "$EXPIRY_OK" = "1" ]    || WARNMSG="$WARNMSG expiry NOT rolled (re-run premarket-expiry-rollover.sh);"
WARNMSG="$WARNMSG CHECK before open."
discord "$WARNMSG"
exit 1   # non-zero so Jenkins shows red and the operator looks
