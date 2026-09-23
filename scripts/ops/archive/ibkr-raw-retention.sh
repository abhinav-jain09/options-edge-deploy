#!/usr/bin/env bash
# ibkr-raw-retention.sh — bound databento_option_raw_snapshot. Hourly, from cron (prod) or launchd (dev).
#
# The table churns ~4M rows/day and the nightly Postgres truncate is off (WIPE_DB=false), so this is
# the only thing that bounds it. 2026-07-13.
#
# FIVE THINGS WERE WRONG, each found only after it had already cost something. They are all the same
# mistake — something reported success while the table was over policy — so they are all listed:
#
#   1. The database password was IN THE FILE. It is now read at run time from the k8s secret, exactly
#      as oe-archive-postgres.sh and signal-ledger-backup.sh do. The literal that was in the file
#      must be treated as disclosed and rotated. (2026-09-08)
#
#   2. psql's status was discarded and "retention run done" printed unconditionally, so a failing
#      DELETE reported success every hour while the table grew without bound. (2026-09-08)
#
#   3. Fix 1 broke the script the next morning and nobody noticed for thirteen days: kubectl lives in
#      /usr/local/bin, which is NOT on cron's PATH, so the secret read returned empty and every run
#      exited FATAL. 293 consecutive failures, 2026-09-09 05:17 to 2026-09-22, by which point the
#      table was 110 GB / 41M rows under a 16-hour policy. The script now sets its own PATH, and
#      every FATAL alerts instead of only writing to a log nobody reads. (2026-09-22)
#
#   4. The first repair of 3 reintroduced 2 one layer down: on hitting its batch cap it logged a WARN,
#      returned 0, and the caller logged "retention run done". Hitting any limit is now FATAL with an
#      alert and a nonzero exit, and so is skipping on a held lock. (2026-09-22, Codex r2)
#
#   5. That repair still could not converge. Batches selected rows with an unordered LIMIT, and
#      because deleting does not compact the heap, every batch re-walked the dead pages left by the
#      last one — the catch-up hit a 300s statement_timeout at 7.2M rows. Progress is now a monotonic
#      cursor over the PRIMARY KEY, which is index-backed and therefore bounded. (2026-09-22, Codex r3)
#
# PROGRESSION. Each pass walks the id space once, in ID_CHUNK-sized ranges, from the minimum id to
# the maximum id captured when the pass started. "id >= lo AND id < hi" is an Index Scan on
# ibkr_option_raw_snapshot_pkey (verified with EXPLAIN on prod, PG 13.23), so a range costs its own
# rows and not the whole heap. The cursor only moves forward, so a pass always terminates: rows
# inserted after it started get ids above the captured maximum and are inside retention anyway.
# This is why there is no MAX_BATCHES — the bound is the id space, not a guessed iteration count.
#
# RETENTION IS TWO-TIER AND THE TIERS ARE TOTAL — every row is in exactly one, so no row can become
# immortal by matching neither:
#
#   HAS OI  (coalesce(call_oi,0) > 0 OR coalesce(put_oi,0) > 0)
#           -> kept OI_RETENTION_DAYS *calendar* days by session_date, in EXCHANGE_TZ. gex and
#              directional-pressure carry OI and the carry bound is 4 calendar days. NOTE THE
#              ARITHMETIC: "session_date < today - 4" retains FIVE session dates, today and the four
#              before it. That is deliberate, not an off-by-one — the bound says how far back carry
#              may REACH, so today plus four is the smallest window that always satisfies it.
#   EVERYTHING ELSE (zero, NULL, and negative OI alike)
#           -> kept RETENTION by captured_at. volume-pace backfills call_volume/put_volume from these.
#              Negative and NULL OI land here deliberately: they are not carry inputs, and a predicate
#              naming only "= 0" left them matching neither tier.
#
# Rows with a NULL session_date are deleted by the OI tier rather than left behind: a row that cannot
# be aged is not a row worth keeping. captured_at is NOT NULL in the schema (verified on prod), so
# the short tier does not test for it.
set -uo pipefail
# Bare command names, resolved against the hardened PATH below exactly as before — this changes
# nothing about which binary prod runs. It exists so a caller that needs a SPECIFIC binary (a test
# stub) can name one by absolute path instead of fighting the hardening with a PATH prefix, which
# the hardening below is written to always win: an override here is consulted at every call site,
# the hardcoded PATH is not touched.
PSQL_BIN="${PSQL_BIN:-psql}"
KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
# /usr/local/bin covers Linux/prod (kubectl lives there) and Intel Homebrew; /opt/homebrew/bin
# covers Apple Silicon Homebrew (dev, where psql lives there and NOT under /usr/local/bin — this
# script also runs on dev via launchd, and a PATH copied from the prod convention alone silently
# cannot find psql there). Harmless where a path does not exist.
export PATH=/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

# ---------------------------------------------------------------------------------------------
# POLICY. Frozen as literals on purpose: these are not operational knobs. A cron or launchd
# environment carrying RETENTION='100 years' would produce a clean zero-row run and report success
# while the policy was being violated — the exact failure this file exists to prevent. Changing
# retention is a code change, reviewed, not an environment variable. (Codex r3 MAJOR)
# ---------------------------------------------------------------------------------------------
readonly RETENTION='16 hours'
readonly OI_RETENTION_DAYS=4
readonly EXCHANGE_TZ='America/New_York'
readonly RAW_TABLE='databento_option_raw_snapshot'

# Operational knobs. These change how the work is paced, never what is kept.
PGHOST="${PGHOST:-192.168.100.252}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-options_flow}"
PGDB="${PGDB:-options_flow}"
SECRET_SOURCE="${SECRET_SOURCE:-k8s}"          # k8s | none (dev uses ambient auth)
ID_CHUNK="${ID_CHUNK:-500000}"
RUN_BUDGET_SECONDS="${RUN_BUDGET_SECONDS:-2700}"
CONNECT_TIMEOUT="${CONNECT_TIMEOUT:-10}"
PROGRESS_EVERY="${PROGRESS_EVERY:-25}"
STALE_LOCK_SECONDS="${STALE_LOCK_SECONDS:-7200}"
LOG="${LOG:-$HOME/oe-ops/ibkr-raw-retention.log}"
LOCK_FILE="${LOCK_FILE:-$HOME/oe-ops/.ibkr-raw-retention.lock.d}"   # a DIRECTORY — see acquire_lock
START_TS="$(date +%s)"

log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Thirteen days of hourly FATALs went unseen because this script only ever wrote to its own log, and
# nobody reads a log that is healthy 99% of the time. Sourced AFTER log(): _oe_alert_log delegates to
# the caller's log() when one is defined, so alerts land in this log too.
OE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -r "$OE_DIR/oe-alert.sh" ]; then . "$OE_DIR/oe-alert.sh"
else alert() { log "ALERT (oe-alert.sh not readable at $OE_DIR): $*"; }
fi
die() { log "FATAL: $1"; alert "🚨 ibkr-raw retention on $(hostname): $1"; exit "${2:-1}"; }

for v in ID_CHUNK RUN_BUDGET_SECONDS CONNECT_TIMEOUT PROGRESS_EVERY STALE_LOCK_SECONDS; do
  case "${!v}" in (''|*[!0-9]*) die "$v must be a positive integer, got '${!v}'";; esac
  [ "${!v}" -gt 0 ] || die "$v must be > 0, got '${!v}'"
done

# A run that cannot finish inside the hour must not be joined by the next one: two copies scan and
# delete the same rows, contend, multiply WAL, and time each other out.
#
# flock is not used: it does not exist on macOS (this script also runs on dev, via launchd, and
# BSD userland has no flock). mkdir is atomic on both platforms, which is the only property the
# lock actually needs. What flock gives for free that a plain directory does not is release-on-
# crash — a killed or crashed holder still holds its fd table closed, but nothing removes a stale
# directory automatically — so this lock records its holder's PID and treats a dead PID as
# conclusive staleness, on top of the age-based check. Heartbeat is a SEPARATE timestamp from the
# acquire time, touched every batch in run_tier, so a genuinely long catch-up is never confused
# with a wedged one (Codex r3 MINOR: age-since-acquire alone misdiagnoses exactly that).
#
# Reclaiming a stale lock is NOT a plain "rm -rf then mkdir": that pair is not atomic, and two
# contenders that both observe the same dead lock could both remove it and both believe they now
# hold it (Codex r4 BLOCKER). Reclaim instead RENAMEs the directory to a name unique to this PID
# before touching it — rename() is atomic, so it can never remove a path that has changed identity
# since it was inspected — then deletes that private, uniquely-named copy, which nothing else can
# reference. A rename that fails means someone else already changed what is at that path; this
# process then just tries an ordinary acquire against whatever is there now, rather than assuming
# anything about why the rename failed. A narrow race still exists if two contenders attempt this
# within the same instant (rename() only protects the object's identity, not against a third mkdir
# landing between one contender's rename and its own re-acquire) — accepted, because the SQL this
# lock protects is itself concurrency-safe (two DELETEs against overlapping rows contend for a row
# lock, they do not corrupt anything), so the failure mode of losing this narrow race is wasted
# work, not wrong data. GRACE_SECONDS separately protects a freshly-mkdir'd lock whose pid/heartbeat
# have not been written yet: without it, a contender that lost the mkdir race by milliseconds would
# read empty metadata and try to steal the winner's brand-new lock.
readonly GRACE_SECONDS=10
LOCK_HEARTBEAT="$LOCK_FILE/heartbeat"
acquire_lock() {
  mkdir "$LOCK_FILE" 2>/dev/null || return 1
  echo "$$" > "$LOCK_FILE/pid"
  date +%s > "$LOCK_HEARTBEAT"
  return 0
}
release_lock() { [ "${LOCK_OWNED:-0}" = 1 ] && rm -rf "$LOCK_FILE" 2>/dev/null; }
trap release_lock EXIT
steal_lock() {   # $1 = reason, for the log; steals whatever is CURRENTLY at $LOCK_FILE, atomically
  local quarantine="$LOCK_FILE.stale.$$"
  if mv "$LOCK_FILE" "$quarantine" 2>/dev/null; then
    rm -rf "$quarantine" 2>/dev/null
  fi
  # Whether the rename succeeded (we quarantined what we inspected) or failed (something else
  # already changed it), the only safe next step is an ordinary acquire attempt against whatever
  # is at the canonical path right now.
  acquire_lock
}

if acquire_lock; then
  LOCK_OWNED=1
else
  dir_age=$(( START_TS - $(stat -c %Y "$LOCK_FILE" 2>/dev/null || stat -f %m "$LOCK_FILE" 2>/dev/null || echo "$START_TS") ))
  holder_pid="$(cat "$LOCK_FILE/pid" 2>/dev/null || echo '')"
  beat="$(cat "$LOCK_HEARTBEAT" 2>/dev/null || echo '')"
  case "$beat" in (''|*[!0-9]*) beat='';; esac

  if [ -z "$holder_pid" ] || [ -z "$beat" ]; then
    if [ "$dir_age" -lt "$GRACE_SECONDS" ]; then
      log "another retention run is still initializing $LOCK_FILE (${dir_age}s old, no metadata yet) — skipping this firing"
      exit 75
    fi
    log "WARN: $LOCK_FILE has no pid/heartbeat after ${dir_age}s — treating as a crashed acquire, reclaiming"
    if steal_lock "incomplete metadata"; then
      LOCK_OWNED=1
    else
      log "lost the race to reclaim $LOCK_FILE — another run got there first, skipping"
      exit 75
    fi
  else
    held_for=$(( START_TS - beat ))
    pid_dead=1
    kill -0 "$holder_pid" 2>/dev/null && pid_dead=0

    if [ "$pid_dead" -eq 1 ]; then
      # The holder is not running. flock would have released this on its own; a directory lock does
      # not, so an unclean exit (crash, kill -9, OOM) would otherwise wedge every future run forever.
      log "WARN: reclaiming $LOCK_FILE — holder pid '$holder_pid' is not running (last heartbeat ${held_for}s ago)"
      if steal_lock "dead holder"; then
        LOCK_OWNED=1
      else
        log "lost the race to reclaim $LOCK_FILE — another run got there first, skipping"
        exit 75
      fi
    elif [ "$held_for" -gt "$STALE_LOCK_SECONDS" ]; then
      die "$LOCK_FILE has been held by pid $holder_pid with no heartbeat for ${held_for}s — the holder is wedged and nothing has been deleted since. The table is growing unbounded." 3
    else
      # Deliberately NONZERO. A skip is not success: the previous run is still working, so this
      # hour's policy has not been enforced by anything, and a scheduler that only sees exit 0
      # would call that healthy for as long as it kept happening. (Codex r3 BLOCKER)
      log "another retention run (pid $holder_pid) holds $LOCK_FILE (heartbeat ${held_for}s ago) — skipping this firing"
      exit 75
    fi
  fi
fi

case "$SECRET_SOURCE" in
  k8s)
    # --request-timeout because this runs AFTER the lock is acquired: a kubectl that hangs holds the
    # lock forever, and every later firing then skips on it.
    PGPASSWORD="${PGPASSWORD:-$("$KUBECTL_BIN" --request-timeout=15s -n options-edge get secret options-edge-runtime-secrets \
      -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d)}"
    [ -n "$PGPASSWORD" ] || die "could not read POSTGRES_PASSWORD from the k8s secret — retention did NOT run. $RAW_TABLE is UNBOUNDED until this is fixed (this is how it reached 110 GB in 2026-09)."
    export PGPASSWORD
    ;;
  none) : ;;   # dev: ambient auth, no secret to read
  *) die "SECRET_SOURCE must be 'k8s' or 'none', got '$SECRET_SOURCE'" ;;
esac
export PGCONNECT_TIMEOUT="$CONNECT_TIMEOUT"
DEADLINE=$(( START_TS + RUN_BUDGET_SECONDS ))

# Remaining budget, or 0 when spent. Nothing runs past the deadline — no floor, because a floor is
# just a smaller way of running over. (Codex r3 MAJOR)
remaining() { local r=$(( DEADLINE - $(date +%s) )); [ "$r" -lt 0 ] && r=0; printf '%s' "$r"; }

psql_at() {   # $1 = statement_timeout seconds, rest = -c args
  local t="$1"; shift
  # statement_timeout goes through PGOPTIONS (a startup-packet GUC), NOT a separate "-c 'SET ...'"
  # psql meta-command. Two -c flags each print their own output; when the caller's query times out
  # and returns no row, the SET command's own status line becomes the last line of output, and a
  # caller reading "the last line" reads the literal text "SET" as if it were the query's result.
  # That is not hypothetical — it is exactly what happened running this against prod: min(id) timed
  # out, and the id-range read tried to parse "SET" as a number. PGOPTIONS means there is exactly
  # one -c, so there is exactly one possible output, and a timeout now produces NO output rather
  # than a deceptive one. (found running this script, not by inspection)
  PGOPTIONS="-c statement_timeout=${t}s" \
  "$PSQL_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -At -v ON_ERROR_STOP=1 "$@"
}

run_tier() {
  local label="$1" predicate="$2" total=0 batches=0 lo hi n rc t width empty_streak
  local min_id max_id bound_budget
  width="$ID_CHUNK"
  empty_streak=0
  # MIN(id)/MAX(id) over a bigserial PK is an index scan that walks from the end until it finds the
  # first LIVE row — so after a large deletion at the low end of the id space (exactly retention's
  # own job) it is NOT the O(1) lookup it looks like: it has to skip every dead entry first. Measured
  # on prod after a 25M-row deletion: several minutes, not milliseconds. These reads get real budget,
  # not an arbitrary short constant, and a timeout here is a genuine FATAL — it means the table needs
  # a VACUUM before retention can make progress, which is worth alerting on, not masking.
  bound_budget="$(remaining)"; [ "$bound_budget" -gt 600 ] && bound_budget=600
  [ "$bound_budget" -gt 0 ] || die "tier [$label] had no run budget left to even read the id range"
  min_id="$(psql_at "$bound_budget" -c "SELECT coalesce(min(id), 0) FROM $RAW_TABLE" 2>>"$LOG")"
  max_id="$(psql_at "$bound_budget" -c "SELECT coalesce(max(id), -1) FROM $RAW_TABLE" 2>>"$LOG")"
  case "$min_id" in (''|*[!0-9]*) die "tier [$label] could not read min(id) within ${bound_budget}s (got '$min_id') — the table likely needs a VACUUM before retention can make progress";; esac
  case "$max_id" in ('-1') ;; (''|*[!0-9]*) die "tier [$label] could not read max(id) within ${bound_budget}s (got '$max_id')";; esac

  lo="$min_id"
  while [ "$lo" -le "$max_id" ]; do
    t="$(remaining)"
    [ "$t" -gt 0 ] || die "tier [$label] hit the ${RUN_BUDGET_SECONDS}s run budget at id $lo after $total rows — the table is STILL over policy and the next run must continue from the start of the id space. If this repeats, retention is not keeping up with ingest." 2
    [ "$t" -gt 300 ] && t=300
    hi=$(( lo + width ))
    n="$(psql_at "$t" -c "WITH del AS (
                            DELETE FROM $RAW_TABLE
                             WHERE id >= $lo AND id < $hi AND ($predicate)
                             RETURNING 1)
                          SELECT count(*) FROM del" 2>>"$LOG")"
    rc=$?
    [ "$rc" -eq 0 ] || die "tier [$label] DELETE failed (rc=$rc) at id $lo after $total rows — the table is NOT bounded this hour" "$rc"
    # The count comes from RETURNING, never psql's command tag: -q suppresses the tag and an empty
    # count read as 0 ends a loop while looking exactly like "nothing to delete".
    case "$n" in (''|*[!0-9]*) die "tier [$label] returned an unparseable row count '$n' at id $lo after $total rows";; esac
    total=$(( total + n ))
    lo="$hi"
    batches=$(( batches + 1 ))
    # A long stretch where nothing matches this tier's predicate — most commonly rows that belong
    # only to the OTHER tier sitting at low ids — still costs a full index range scan per chunk.
    # Widen geometrically (capped) so that stretch is skipped in a handful of queries instead of
    # thousands (Codex r4: an OI tier stuck behind a huge no-OI prefix could burn its whole budget
    # walking rows it will never delete). This is safe with NO memory across runs: a row that does
    # not match today — including one that GAINS open interest later via gex's own upsert onto an
    # existing row (on conflict ... do update, confirmed in DatabentoOiBaselineProvider) — is
    # re-examined in full next invocation, because that invocation starts over from the real
    # min(id), not from anything this run leaves behind.
    if [ "$n" -eq 0 ]; then
      empty_streak=$(( empty_streak + 1 ))
      if [ "$empty_streak" -ge 3 ]; then
        width=$(( width * 4 ))
        [ "$width" -gt $(( ID_CHUNK * 200 )) ] && width=$(( ID_CHUNK * 200 ))
      fi
    else
      width="$ID_CHUNK"
      empty_streak=0
    fi
    date +%s > "$LOCK_HEARTBEAT" 2>/dev/null || true   # the stale-lock check reads this
    # A multi-hour recovery that logs only on completion is indistinguishable from a wedged one.
    [ $(( batches % PROGRESS_EVERY )) -eq 0 ] && \
      log "$label: $total rows, id $lo/$max_id, width=$width, $(( $(date +%s) - START_TS ))s elapsed"
  done
  log "$label: $total rows deleted in $batches id ranges"
}

HAS_OI="coalesce(call_open_interest,0) > 0 OR coalesce(put_open_interest,0) > 0"

# Cutoffs are snapshotted ONCE, not re-evaluated by every batch's own now(). A pass over the whole
# id space takes real wall-clock time; without this, a run crossing NY midnight would judge early
# id ranges against yesterday's calendar cutoff and later ranges against today's, an internally
# inconsistent single pass (Codex r4 MINOR). Fetched from Postgres, not computed in bash, so the
# server's own clock and tz database — not this host's — are authoritative.
bound_budget="$(remaining)"; [ "$bound_budget" -gt 60 ] && bound_budget=60
[ "$bound_budget" -gt 0 ] || die "no run budget left to snapshot retention cutoffs"
OI_CUTOFF_DATE="$(psql_at "$bound_budget" -c "SELECT ((now() AT TIME ZONE '$EXCHANGE_TZ')::date - $OI_RETENTION_DAYS)" 2>>"$LOG")"
RETENTION_CUTOFF="$(psql_at "$bound_budget" -c "SELECT (now() - interval '$RETENTION')" 2>>"$LOG")"
[ -n "$OI_CUTOFF_DATE" ] || die "could not compute the OI calendar cutoff — retention did NOT run"
[ -n "$RETENTION_CUTOFF" ] || die "could not compute the no-OI retention cutoff — retention did NOT run"

run_tier "oi before $OI_RETENTION_DAYS calendar days ($EXCHANGE_TZ)" \
  "($HAS_OI) AND (session_date IS NULL OR session_date < '$OI_CUTOFF_DATE'::date)"
run_tier "no-oi older than $RETENTION" \
  "NOT ($HAS_OI) AND captured_at < '$RETENTION_CUTOFF'::timestamptz"

# Deleting tens of millions of rows leaves the planner with statistics describing a table that no
# longer exists. ANALYZE is cheap and is what matters for the correctness of later plans. Space is
# made reusable by autovacuum but is NOT returned to the OS — that needs a planned online repack and
# is a runbook decision, never something an hourly cron should attempt on its own.
t="$(remaining)"
if [ "$t" -gt 0 ]; then
  [ "$t" -gt 120 ] && t=120
  psql_at "$t" -c "ANALYZE $RAW_TABLE" >/dev/null 2>>"$LOG" \
    || log "WARN: ANALYZE after retention failed — planner statistics are stale, deletes still applied"
else
  log "WARN: no budget left for ANALYZE — planner statistics are stale, deletes still applied"
fi

log "retention run done"
