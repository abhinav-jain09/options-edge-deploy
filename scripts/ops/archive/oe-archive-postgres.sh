#!/usr/bin/env bash
# oe-archive-postgres.sh — archive the Postgres tables that expire, before they expire.
#
# WHY THIS EXISTS, AND WHY IT IS URGENT
# ------------------------------------
# databento_option_raw_snapshot is the ONLY place open interest lands in a readable form:
#   * options.databento.events.raw carries NO open-interest field at all (verified 2026-07-31:
#     ask/bid/size/strike/side/tsEvent/... and nothing else)
#   * options.databento.gex.strike does carry it, but it is Avro — a text archive of that topic
#     is undecodable bytes
# and ibkr-raw-retention.sh deletes anything older than 16 HOURS, hourly. The daily OI baseline
# is fetched around 06:30 ET, so every session's OI is destroyed the same night. Without this
# script, no OI has ever been archived — and with no OI you cannot reconstruct GEX, which makes
# the option-chain archive far less useful for training.
#
# WHAT IT WRITES  (same convention as oe-archive-kafka.sh)
#   $ARCHIVE_DIR/postgres/$ENV/<table>/dt=<sessionDate>/<table>.<from>-<to>.dt<YYYYMMDD>.<archivedAt>.csv.gz
#   $ARCHIVE_DIR/postgres/$ENV/_manifest/<table>.state    <- last archived id + row count per run
#   $ARCHIVE_DIR/postgres/$ENV/_manifest/runs.log
#
# CSV with a header row: dependency-free and read directly by pandas/polars/duckdb
# (duckdb: read_csv_auto('.../*.csv.gz')).
#
# Incremental on the primary key (id), which only ever increases — so a run picks up exactly
# what arrived since the last one and can never silently skip a block. The checkpoint advances
# ONLY after the file is written and its row count verified.
#
# USAGE
#   ARCHIVE_DIR=/mnt/nas/optionsedge ENV=prod ./oe-archive-postgres.sh
#   ARCHIVE_DIR=/home/kafka/archive  ENV=prod ALLOW_NON_NAS=true ./oe-archive-postgres.sh
set -uo pipefail

ARCHIVE_DIR="${ARCHIVE_DIR:?set ARCHIVE_DIR}"
ENV_NAME="${ENV:?set ENV to prod|dev}"
PGHOST="${PGHOST:-127.0.0.1}"
PGUSER="${PGUSER:-options_flow}"
PGDB="${PGDB:-options_flow}"
ALLOW_NON_NAS="${ALLOW_NON_NAS:-false}"
# Tables that EXPIRE and therefore must be captured. Others (signal_ledger etc.) have no purge
# and are covered by the existing pg_dump backup — adding them here would just duplicate them.
TABLES="${TABLES:-databento_option_raw_snapshot}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

[ -d "$ARCHIVE_DIR" ] || die "ARCHIVE_DIR '$ARCHIVE_DIR' does not exist (NAS not mounted?) — refusing"
probe="$ARCHIVE_DIR/.oe_write_test.$$"
touch "$probe" 2>/dev/null && rm -f "$probe" || die "ARCHIVE_DIR '$ARCHIVE_DIR' not writable — refusing"
case "$ARCHIVE_DIR" in
  /mnt/nas/*|/Volumes/nas/*) : ;;
  *) [ "$ALLOW_NON_NAS" = "true" ] || die "'$ARCHIVE_DIR' is not a NAS path. Set ALLOW_NON_NAS=true to stage locally on purpose." ;;
esac

# Password from the k8s secret at run time — never written to disk (same as signal-ledger-backup.sh).
PGPASSWORD="${PGPASSWORD:-$(kubectl -n options-edge get secret options-edge-runtime-secrets \
  -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d)}"
[ -n "$PGPASSWORD" ] || die "could not read POSTGRES_PASSWORD from the k8s secret"
export PGPASSWORD
psql_q() { psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -tAc "$1" 2>/dev/null; }

# Session date in NEW YORK, not UTC — this runs at 17:10 ET, already tomorrow in UTC.
DAY="${SESSION_DATE:-$(TZ=America/New_York date +%Y-%m-%d)}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ROOT="$ARCHIVE_DIR/postgres/$ENV_NAME"
MAN="$ROOT/_manifest"
mkdir -p "$MAN" || die "cannot create $MAN"

avail_mb=$(df -Pm "$ARCHIVE_DIR" | tail -1 | awk '{print $4}')
[ "${avail_mb:-0}" -gt 5000 ] || die "only ${avail_mb}MB free on $ARCHIVE_DIR — refusing"
log "archiving postgres env=$ENV_NAME db=$PGDB -> $ROOT (free ${avail_mb}MB)"

total=0; files=0; failed=0
for table in $TABLES; do
  state="$MAN/$table.state"; touch "$state"
  from=$(awk '{split($1,a,"="); if (a[1]=="last_id") print a[2]}' "$state" | tail -1)
  from="${from:-0}"
  to=$(psql_q "select coalesce(max(id),0) from $table")
  [ -n "$to" ] || { log "  WARN $table: cannot read max(id) — skipping"; failed=$((failed+1)); continue; }

  # If retention already deleted past our checkpoint, say so loudly — that is real data loss and
  # it means this job is not running often enough for the table's retention window.
  minid=$(psql_q "select coalesce(min(id),0) from $table")
  if [ "${minid:-0}" -gt "$((from + 1))" ] && [ "$from" -gt 0 ]; then
    # This is permanent data loss: retention deleted rows this job had not archived. It used to be
    # LOGGED and then walked past — the surviving suffix was archived, the checkpoint advanced over the
    # hole, and the run exited 0. So the only record of the loss was one line in a log nobody reads,
    # and the very next run could no longer even tell there had been a gap (r11 #4).
    #
    # Failing here does not recover the rows; nothing can. It stops the job from erasing the evidence,
    # and makes the exit status say what happened. Set ACCEPT_GAP=true, deliberately, to archive the
    # suffix and move on once someone has decided the loss is accepted.
    log "  GAP $table: checkpoint id=$from but the table now starts at id=$minid — $((minid-from-1)) rows expired unarchived"
    if [ "${ACCEPT_GAP:-false}" != "true" ]; then
      log "  FAILURE $table: refusing to checkpoint past unarchived rows — this job is not running often enough for the table's retention window. Set ACCEPT_GAP=true to accept the loss and continue."
      failed=$(( failed + 1 ))
      printf 'gap_detected=%s checkpoint=%s min_id=%s lost=%s observed=%s\n' \
        "$DAY" "$from" "$minid" "$((minid-from-1))" "$STAMP" >> "$MAN/$table.gaps"
      continue
    fi
    printf 'gap_accepted=%s checkpoint=%s min_id=%s lost=%s observed=%s\n' \
      "$DAY" "$from" "$minid" "$((minid-from-1))" "$STAMP" >> "$MAN/$table.gaps"
  fi

  rows=$(psql_q "select count(*) from $table where id > $from")
  if [ "${rows:-0}" -eq 0 ]; then log "  $table: nothing new"; continue; fi

  outdir="$ROOT/$table/dt=$DAY"; mkdir -p "$outdir"
  out="$outdir/$table.$from-$to.dt${DAY//-/}.$STAMP.csv.gz"
  tmp="$out.partial"

  psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" \
    -c "\copy (select * from $table where id > $from and id <= $to order by id) to stdout csv header" \
    2>/dev/null | gzip -6 > "$tmp"
  rc=${PIPESTATUS[0]}

  got=$(zcat "$tmp" 2>/dev/null | wc -l | tr -d ' ')
  got=$(( got > 0 ? got - 1 : 0 ))          # drop the CSV header row
  if [ "$rc" -eq 0 ] && [ "$got" -eq "$rows" ]; then
    mv "$tmp" "$out"
    printf 'last_id=%s rows=%s dt=%s archived=%s\n' "$to" "$got" "$DAY" "$STAMP" >> "$state"
    log "  $table: +$got rows (id $from -> $to)"
    total=$(( total + got )); files=$(( files + 1 ))
  else
    rm -f "$tmp"
    log "  WARN $table: expected $rows rows, got $got (psql rc=$rc) — checkpoint NOT advanced, will retry"
    failed=$(( failed + 1 ))
  fi
done

printf '%s env=%s rows=%s files=%s failed=%s\n' \
  "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$ENV_NAME" "$total" "$files" "$failed" >> "$MAN/runs.log"
log "DONE rows=$total files=$files failed=$failed"
[ "$failed" -eq 0 ] || exit 1
