#!/usr/bin/env bash
# ibkr-raw-retention.sh — bound databento_option_raw_snapshot. Hourly via cron.
#
# The table churns ~4M rows/day and the nightly Postgres truncate is off (WIPE_DB=false), so this
# is the only thing that bounds it. 2026-07-13.
#
# FOUR THINGS WERE WRONG, each found only after it had already cost something:
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
#   4. The first repair of 3 reintroduced the shape of 2 in a new place: on hitting its batch cap it
#      logged a WARN, returned 0, and the caller then logged "retention run done". A run that knows
#      it left the table over policy would have reported success, hourly, forever. Hitting a limit is
#      now a FATAL with an alert and a nonzero exit. (2026-09-22, Codex r1 MAJOR)
#
# RETENTION IS TWO-TIER AND THE TIERS ARE TOTAL — every row is in exactly one of them, so no row can
# become immortal by matching neither:
#
#   HAS OI  (coalesce(call_oi,0) > 0 OR coalesce(put_oi,0) > 0)
#           -> kept OI_RETENTION_DAYS *calendar* days, by session_date, in EXCHANGE_TZ.
#              gex and directional-pressure carry OI, and the carry bound is 4 calendar days. A
#              rolling "5 days" interval is NOT that bound: it keeps an extra rolling 24h and drifts
#              against the session boundary. session_date is also the leading indexed column, so this
#              tier deletes via an index instead of a seq scan.
#   EVERYTHING ELSE (zero, NULL, and negative OI alike)
#           -> kept RETENTION by captured_at. volume-pace backfills call_volume/put_volume from these.
#              Negative and NULL OI land here deliberately: they are not carry inputs, and a predicate
#              that named only "= 0" left them matching neither tier.
#
# Rows with a NULL timestamp are deleted by their tier's first sweep rather than left behind: a row
# that cannot be aged is not a row worth keeping.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
LOG="${LOG:-$HOME/oe-ops/ibkr-raw-retention.log}"
PGHOST="${PGHOST:-192.168.100.252}"
PGUSER="${PGUSER:-options_flow}"
PGDB="${PGDB:-options_flow}"
RETENTION="${RETENTION:-16 hours}"
OI_RETENTION_DAYS="${OI_RETENTION_DAYS:-4}"
EXCHANGE_TZ="${EXCHANGE_TZ:-America/New_York}"
BATCH="${BATCH:-50000}"
MAX_BATCHES="${MAX_BATCHES:-2000}"
RUN_BUDGET_SECONDS="${RUN_BUDGET_SECONDS:-2700}"   # 45 min; cron fires hourly
CONNECT_TIMEOUT="${CONNECT_TIMEOUT:-10}"
LOCK_FILE="${LOCK_FILE:-$HOME/oe-ops/.ibkr-raw-retention.lock}"
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Thirteen days of hourly FATALs went unseen because this script only ever wrote to its own log, and
# nobody reads a log that is healthy 99% of the time. Sourced AFTER log() above: _oe_alert_log
# delegates to the caller's log() when one is defined, so alerts land in this log too.
OE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -r "$OE_DIR/oe-alert.sh" ]; then . "$OE_DIR/oe-alert.sh"
else alert() { log "ALERT (oe-alert.sh not readable at $OE_DIR): $*"; }
fi

die() { log "FATAL: $1"; alert "🚨 ibkr-raw retention on $(hostname): $1"; exit "${2:-1}"; }

# A run that cannot finish inside the hour must not be joined by the next one: two copies scanning
# and deleting the same rows contend, multiply WAL, and time each other out.
exec 9>"$LOCK_FILE" || die "cannot open lock file $LOCK_FILE"
if ! flock -n 9; then
  log "another retention run holds $LOCK_FILE — exiting without running"
  exit 0
fi

# Interpolated straight into SQL and loop bounds; a stray cron environment must not turn retention
# into a syntax error or a silently ineffective run.
for v in BATCH MAX_BATCHES RUN_BUDGET_SECONDS OI_RETENTION_DAYS CONNECT_TIMEOUT; do
  case "${!v}" in (''|*[!0-9]*) die "$v must be a non-negative integer, got '${!v}'";; esac
  [ "${!v}" -gt 0 ] || die "$v must be > 0, got '${!v}'"
done

PGPASSWORD="${PGPASSWORD:-$(kubectl -n options-edge get secret options-edge-runtime-secrets \
  -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d)}"
[ -n "$PGPASSWORD" ] || die "could not read POSTGRES_PASSWORD from the k8s secret — retention did NOT run. databento_option_raw_snapshot is UNBOUNDED until this is fixed (this is how it reached 110 GB in 2026-09)."
export PGPASSWORD PGCONNECT_TIMEOUT="$CONNECT_TIMEOUT"

DEADLINE=$(( $(date +%s) + RUN_BUDGET_SECONDS ))

# Deletes in ctid batches so no single statement holds locks or grows WAL without bound — a run that
# has to catch up after an outage must not be the thing that takes the table down.
#
# The batch count comes from RETURNING, not from psql's command tag: -q suppresses the tag, and a
# count that reads as empty would be treated as 0 and end the loop after one batch, which looks
# exactly like "nothing to delete". An unparseable count is a failure, never a zero.
run_tier() {
  local label="$1" predicate="$2" total=0 batches=0 n rc
  while :; do
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
      die "tier [$label] hit the ${RUN_BUDGET_SECONDS}s run budget after $total rows — the table is STILL over policy and the next run must continue. If this repeats, retention is not keeping up with ingest." 2
    fi
    n="$(psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -At -v ON_ERROR_STOP=1 \
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
    [ "$rc" -eq 0 ] || die "tier [$label] DELETE failed (rc=$rc) after $total rows — the table is NOT bounded this hour" "$rc"
    case "$n" in (''|*[!0-9]*) die "tier [$label] returned an unparseable row count '$n' after $total rows — treating as failure";; esac
    total=$(( total + n ))
    [ "$n" -eq 0 ] && break
    batches=$(( batches + 1 ))
    if [ "$batches" -ge "$MAX_BATCHES" ]; then
      die "tier [$label] hit MAX_BATCHES=$MAX_BATCHES after $total rows — the table is STILL over policy. This is not a clean stop: it means retention is behind and needs either a larger budget or an ingest-side fix." 2
    fi
    sleep 0.2
  done
  log "$label: $total rows deleted"
}

HAS_OI="(coalesce(call_open_interest,0) > 0 OR coalesce(put_open_interest,0) > 0)"
OI_CUTOFF="((now() AT TIME ZONE '$EXCHANGE_TZ')::date - $OI_RETENTION_DAYS)"

run_tier "no-oi older than $RETENTION" \
  "NOT $HAS_OI AND (captured_at IS NULL OR captured_at < now() - interval '$RETENTION')"
run_tier "oi before $OI_RETENTION_DAYS calendar days ($EXCHANGE_TZ)" \
  "$HAS_OI AND (session_date IS NULL OR session_date < $OI_CUTOFF)"

log "retention run done"
