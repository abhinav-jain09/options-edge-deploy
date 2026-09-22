#!/usr/bin/env bash
# ibkr-raw-retention.sh — bound databento_option_raw_snapshot. Hourly via cron.
#
# The table churns ~4M rows/day and the nightly Postgres truncate is off (WIPE_DB=false), so this
# is the only thing that bounds it. 2026-07-13.
#
# THREE THINGS WERE WRONG, each found only after it had already cost something:
#
#   1. It carried the database password IN THE FILE. The credential is now read at run time from the
#      k8s secret, exactly as oe-archive-postgres.sh and signal-ledger-backup.sh do, and never written
#      to disk. The literal that was in the file must be treated as disclosed and rotated. (2026-09-08)
#
#   2. It never checked whether the DELETE worked. psql's status was discarded, the "retention run
#      done" line was printed unconditionally, and that echo then decided the exit status — so a
#      failing DELETE reported success every hour while the table grew without bound. (2026-09-08)
#
#   3. Fix 1 broke the script the very next morning and nobody noticed for thirteen days. kubectl
#      lives in /usr/local/bin, which is NOT on cron's default PATH, so the secret read returned
#      empty and every run exited FATAL — 293 consecutive failures from 2026-09-09 05:17 until
#      2026-09-22, by which time the table was 110 GB / 41M rows under a 16-hour policy. The fix that
#      removed the on-disk credential is right; what was missing is that a cron job gets almost no
#      environment, so this script now sets its own PATH instead of inheriting one. The sibling
#      oe-archive-daily.sh had already learned this. (2026-09-22)
#
# Retention is two-tier, because the two classes of row are needed for different lengths of time:
#   rows WITH open interest    -> OI_RETENTION   (gex/directional-pressure carry; bound is 4 calendar days)
#   rows WITHOUT open interest -> RETENTION      (volume-pace backfills call/put_volume from these)
# Non-OI rows are ~93% of volume on a session where the pre-open OI capture failed and ~0-2% on a
# healthy one, so the tiers are about correctness, not just size.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
LOG="${LOG:-$HOME/oe-ops/ibkr-raw-retention.log}"
PGHOST="${PGHOST:-192.168.100.252}"
PGUSER="${PGUSER:-options_flow}"
PGDB="${PGDB:-options_flow}"
RETENTION="${RETENTION:-16 hours}"
OI_RETENTION="${OI_RETENTION:-5 days}"
BATCH="${BATCH:-50000}"
MAX_BATCHES="${MAX_BATCHES:-2000}"
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Thirteen days of hourly FATALs went unseen because this script only ever wrote to its own log, and
# nobody reads a log that is healthy 99% of the time. Sourced AFTER log() above: _oe_alert_log
# delegates to the caller's log() when one is defined, so alerts land in this log too.
OE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -r "$OE_DIR/oe-alert.sh" ]; then . "$OE_DIR/oe-alert.sh"
else alert() { log "ALERT (oe-alert.sh not readable at $OE_DIR): $*"; }
fi

PGPASSWORD="${PGPASSWORD:-$(kubectl -n options-edge get secret options-edge-runtime-secrets \
  -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d)}"
if [ -z "$PGPASSWORD" ]; then
  log "FATAL: could not read POSTGRES_PASSWORD from the k8s secret — retention did NOT run"
  alert "🚨 ibkr-raw retention did NOT run on $(hostname): no POSTGRES_PASSWORD from the k8s secret. databento_option_raw_snapshot is UNBOUNDED until this is fixed (this is how it reached 110 GB in 2026-09)."
  exit 1
fi
export PGPASSWORD

# Deletes in ctid batches so no single statement holds locks or grows WAL without bound — a run that
# has to catch up after an outage must not be the thing that takes the table down.
#
# The batch count comes from RETURNING, not from psql's command tag: -q suppresses the tag, and a
# count that reads as empty would be treated as 0 and end the loop after one batch, which looks
# exactly like "nothing to delete".
run_tier() {
  local label="$1" predicate="$2" total=0 batches=0 n rc
  while :; do
    n="$(psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -At \
          -c "SET statement_timeout = '300s'" \
          -c "WITH doomed AS (
                SELECT ctid FROM databento_option_raw_snapshot
                 WHERE $predicate
                 LIMIT $BATCH),
                  del AS (
                DELETE FROM databento_option_raw_snapshot t
                 USING doomed d WHERE t.ctid = d.ctid
                 RETURNING 1)
              SELECT count(*) FROM del" 2>>"$LOG" | tail -1)"
    rc=$?
    if [ "$rc" -ne 0 ]; then
      log "FATAL: $label DELETE failed (rc=$rc) after $total rows — the table is NOT bounded this hour"
      alert "🚨 ibkr-raw retention FAILED on $(hostname): tier [$label] rc=$rc after $total rows. databento_option_raw_snapshot is not bounded this hour."
      return "$rc"
    fi
    case "$n" in (''|*[!0-9]*)
      log "FATAL: $label got an unparseable row count '$n' after $total rows — treating as failure"
      alert "🚨 ibkr-raw retention on $(hostname): tier [$label] returned an unparseable row count after $total rows."
      return 1;;
    esac
    total=$(( total + n ))
    [ "$n" -eq 0 ] && break
    batches=$(( batches + 1 ))
    if [ "$batches" -ge "$MAX_BATCHES" ]; then
      log "WARN: $label hit MAX_BATCHES=$MAX_BATCHES after $total rows — more remains, next run continues"
      break
    fi
    sleep 0.2
  done
  log "$label: $total rows deleted"
  return 0
}

run_tier "no-oi older than $RETENTION" \
  "captured_at < now() - interval '$RETENTION' AND coalesce(call_open_interest,0) = 0 AND coalesce(put_open_interest,0) = 0" || exit 1
run_tier "oi older than $OI_RETENTION" \
  "captured_at < now() - interval '$OI_RETENTION' AND (coalesce(call_open_interest,0) > 0 OR coalesce(put_open_interest,0) > 0)" || exit 1
log "retention run done"
