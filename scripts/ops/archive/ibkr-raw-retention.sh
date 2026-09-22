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
#              gex and directional-pressure carry OI and the carry bound is 4 calendar days. A rolling
#              interval is not that bound: it keeps an extra rolling 24h and drifts against the session
#              boundary. NOTE THE ARITHMETIC: "session_date < today - 4" retains FIVE session dates —
#              today and the four before it. That is deliberate and is a superset of the 4-day bound,
#              not an off-by-one; the bound says how far back carry may reach, so retaining today plus
#              four is the smallest window that always satisfies it.
#   EVERYTHING ELSE (zero, NULL, and negative OI alike)
#           -> kept RETENTION by captured_at. volume-pace backfills call_volume/put_volume from these.
#              Negative and NULL OI land here deliberately: they are not carry inputs, and a predicate
#              that named only "= 0" left them matching neither tier.
#
# THE OI TIER RUNS FIRST, and that order is load-bearing rather than cosmetic. Both tiers seq-scan —
# measured with EXPLAIN on the 41M-row prod table, neither uses an index, and a comment in an earlier
# revision of this file claimed otherwise. A seq scan is fine while matches are dense: the LIMIT fills
# immediately. It is ruinous once they are sparse, because each batch scans further to find its next
# 50k. The OI tier is the dense one (25.5M of 41M rows on 2026-09-22), so running it first shrinks the
# heap the sparse tier has to walk. Running the sparse tier first is what hit a 300s statement_timeout
# at 7.2M rows during the catch-up.
#
# Rows with a NULL session_date are deleted by the OI tier rather than left behind: a row that cannot
# be aged is not a row worth keeping.
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
STALE_LOCK_SECONDS="${STALE_LOCK_SECONDS:-7200}"
PROGRESS_EVERY="${PROGRESS_EVERY:-20}"
START_TS="$(date +%s)"
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Thirteen days of hourly FATALs went unseen because this script only ever wrote to its own log, and
# nobody reads a log that is healthy 99% of the time. Sourced AFTER log() above: _oe_alert_log
# delegates to the caller's log() when one is defined, so alerts land in this log too.
OE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -r "$OE_DIR/oe-alert.sh" ]; then . "$OE_DIR/oe-alert.sh"
else alert() { log "ALERT (oe-alert.sh not readable at $OE_DIR): $*"; }
fi

die() { log "FATAL: $1"; alert "🚨 ibkr-raw retention on $(hostname): $1"; exit "${2:-1}"; }

# Interpolated straight into SQL and loop bounds; a stray cron environment must not turn retention
# into a syntax error or a silently ineffective run.
for v in BATCH MAX_BATCHES RUN_BUDGET_SECONDS OI_RETENTION_DAYS CONNECT_TIMEOUT PROGRESS_EVERY STALE_LOCK_SECONDS; do
  case "${!v}" in (''|*[!0-9]*) die "$v must be a non-negative integer, got '${!v}'";; esac
  [ "${!v}" -gt 0 ] || die "$v must be > 0, got '${!v}'"
done

# A run that cannot finish inside the hour must not be joined by the next one: two copies scanning
# and deleting the same rows contend, multiply WAL, and time each other out.
exec 9>"$LOCK_FILE" || die "cannot open lock file $LOCK_FILE"
if ! flock -n 9; then
  # One overlap is ordinary. A lock held for hours means the holder is wedged, and because a skip
  # exits 0 nobody would ever hear about it.
  held_for=$(( $(date +%s) - $(stat -c %Y "$LOCK_FILE" 2>/dev/null || echo 0) ))
  if [ "$held_for" -gt "$STALE_LOCK_SECONDS" ]; then
    log "FATAL: $LOCK_FILE has been held for ${held_for}s — the holder is wedged and retention has stopped"
    alert "🚨 ibkr-raw retention on $(hostname) has been blocked for ${held_for}s by a run holding $LOCK_FILE. Nothing has been deleted since then; the table is growing unbounded."
    exit 3
  fi
  log "another retention run holds $LOCK_FILE (${held_for}s) — exiting without running"
  exit 0
fi
touch "$LOCK_FILE" 2>/dev/null || true

# --request-timeout because this runs AFTER the flock is taken: a kubectl that hangs forever holds
# the lock forever, and every later cron firing then exits 0 on the held lock — retention silently
# stopped, cron green. That is the same success-masking shape this script keeps being bitten by.
PGPASSWORD="${PGPASSWORD:-$(kubectl --request-timeout=15s -n options-edge get secret options-edge-runtime-secrets \
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
    # statement_timeout is clamped to what is left of the budget: a statement starting one second
    # before the deadline could otherwise run a further 300s and overlap the next cron firing, which
    # would then skip on the flock. A floor of 10s keeps the last batch from being unable to do
    # anything at all.
    remaining=$(( DEADLINE - $(date +%s) ))
    [ "$remaining" -gt 300 ] && remaining=300
    [ "$remaining" -lt 10 ] && remaining=10
    n="$(psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -At -v ON_ERROR_STOP=1 \
          -c "SET statement_timeout = '${remaining}s'" \
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
    # A multi-hour recovery that logs only on completion is indistinguishable from a wedged one.
    if [ $(( batches % PROGRESS_EVERY )) -eq 0 ]; then
      log "$label: $total rows in $batches batches, $(( $(date +%s) - START_TS ))s elapsed"
    fi
    if [ "$batches" -ge "$MAX_BATCHES" ]; then
      die "tier [$label] hit MAX_BATCHES=$MAX_BATCHES after $total rows — the table is STILL over policy. This is not a clean stop: it means retention is behind and needs either a larger budget or an ingest-side fix." 2
    fi
    sleep 0.2
  done
  log "$label: $total rows deleted"
}

HAS_OI="(coalesce(call_open_interest,0) > 0 OR coalesce(put_open_interest,0) > 0)"
OI_CUTOFF="((now() AT TIME ZONE '$EXCHANGE_TZ')::date - $OI_RETENTION_DAYS)"

# OI first — see the tier-order note at the top. captured_at is NOT NULL in the schema (verified on
# prod 2026-09-22), so the short tier does not test for it: an IS NULL arm against a NOT NULL column
# is dead weight that reads as though the case were possible.
run_tier "oi before $OI_RETENTION_DAYS calendar days ($EXCHANGE_TZ)" \
  "$HAS_OI AND (session_date IS NULL OR session_date < $OI_CUTOFF)"
run_tier "no-oi older than $RETENTION" \
  "NOT $HAS_OI AND captured_at < now() - interval '$RETENTION'"

# Deleting tens of millions of rows leaves the planner with stale statistics and the heap full of
# dead tuples. ANALYZE is cheap and is the part that matters for correctness of later plans; space is
# made reusable by autovacuum but is NOT returned to the OS — that needs a planned online repack, and
# it is a runbook decision, not something an hourly cron should ever attempt on its own.
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -At -v ON_ERROR_STOP=1 \
     -c "SET statement_timeout = '120s'" -c "ANALYZE databento_option_raw_snapshot" >/dev/null 2>>"$LOG" \
  || log "WARN: ANALYZE after retention failed — planner statistics are stale, deletes still applied"

log "retention run done"
