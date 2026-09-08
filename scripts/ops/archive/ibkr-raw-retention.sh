#!/usr/bin/env bash
# ibkr-raw-retention.sh — bound databento_option_raw_snapshot to 16 hours. Hourly via cron.
#
# The table churns ~255k rows/hour and the nightly Postgres truncate is off (WIPE_DB=false), so this
# is the only thing that bounds it. Deletes by captured_at, which is indexed. 2026-07-13.
#
# TWO THINGS WERE WRONG when this was finally brought under repo management (2026-09-08):
#
#   1. It carried the database password IN THE FILE. The credential is now read at run time from the
#      k8s secret, exactly as oe-archive-postgres.sh and signal-ledger-backup.sh do, and never written
#      to disk. The literal that was in the file must be treated as disclosed and rotated.
#
#   2. It never checked whether the DELETE worked. psql's status was discarded, the "retention run
#      done" line was printed unconditionally, and that echo then decided the exit status — so a
#      failing DELETE reported success every hour while the table grew without bound. The only way
#      anyone would have learned of it is the disk filling.
set -uo pipefail
LOG="${LOG:-$HOME/oe-ops/ibkr-raw-retention.log}"
PGHOST="${PGHOST:-192.168.100.252}"
PGUSER="${PGUSER:-options_flow}"
PGDB="${PGDB:-options_flow}"
RETENTION="${RETENTION:-16 hours}"
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

PGPASSWORD="${PGPASSWORD:-$(kubectl -n options-edge get secret options-edge-runtime-secrets \
  -o jsonpath='{.data.POSTGRES_PASSWORD}' 2>/dev/null | base64 -d)}"
if [ -z "$PGPASSWORD" ]; then
  log "FATAL: could not read POSTGRES_PASSWORD from the k8s secret — retention did NOT run"
  exit 1
fi
export PGPASSWORD

out="$(psql -h "$PGHOST" -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 \
        -c "DELETE FROM databento_option_raw_snapshot WHERE captured_at < now() - interval '$RETENTION'" 2>&1)"
rc=$?
printf '%s\n' "$out" >> "$LOG"
if [ "$rc" -ne 0 ]; then
  log "FATAL: retention DELETE failed (rc=$rc) — the table is NOT bounded this hour"
  exit "$rc"
fi
log "retention run done: $out"
