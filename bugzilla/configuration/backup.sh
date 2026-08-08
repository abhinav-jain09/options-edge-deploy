#!/usr/bin/env bash
#
# Take a verified backup of the internal Bugzilla before a provisioning run.
#
# The verification is the point. A failed `mariadb-dump` still produces a
# perfectly valid *empty* gzip that passes `gzip -t`, so this checks the dump's
# own completion footer, records a checksum, and only then prints BACKUP OK.
# If it does not print BACKUP OK, there is no usable backup.
#
#   ./backup.sh [output-directory]

set -euo pipefail

WEB=${BZ_WEB_CONTAINER:-options-edge-bugzilla-web}
DB=${BZ_DB_CONTAINER:-options-edge-bugzilla-db}
OUTDIR=${1:-$HOME}
TS=$(date +%Y%m%dT%H%M%S)
DUMP="$OUTDIR/bugzilla-pre-projects-$TS.sql.gz"
PARAMS="$OUTDIR/bugzilla-params-pre-projects-$TS.json"
CNF=/tmp/bz-backup-client.cnf

cleanup() { docker exec "$DB" rm -f "$CNF" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> writing a protected client option file inside $DB"
# Keeps the root password out of the client process's argv, where any other
# process in that container could read it from /proc.
docker exec "$DB" sh -c "umask 077; printf '[client]\nuser=root\npassword=%s\n' \"\$MARIADB_ROOT_PASSWORD\" > $CNF"

echo "==> dumping"
docker exec "$DB" sh -c \
  "mariadb-dump --defaults-file=$CNF --single-transaction --routines --triggers --events --hex-blob \"\$BZ_DB_NAME\"" \
  | gzip > "$DUMP"

echo "==> verifying the dump is complete"
gzip -t "$DUMP"
zgrep -q -- '-- Dump completed' "$DUMP" \
  || { echo "FATAL: $DUMP has no completion footer - the dump did NOT finish" >&2; exit 1; }

# Record the database's charset/collation: a restore that recreates the schema
# with the server defaults would silently change them.
docker exec "$DB" sh -c \
  "mysql --defaults-file=$CNF -BN -e \"SELECT CONCAT(DEFAULT_CHARACTER_SET_NAME,' ',DEFAULT_COLLATION_NAME) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='\$BZ_DB_NAME'\"" \
  > "$DUMP.charset"
test -s "$DUMP.charset" || { echo "FATAL: could not read the database charset" >&2; exit 1; }

echo "==> copying params.json"
docker cp "$WEB:/var/www/html/data/params.json" "$PARAMS"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$PARAMS" \
  || { echo "FATAL: copied params.json is not valid JSON" >&2; exit 1; }

( cd "$OUTDIR" && sha256sum "$(basename "$DUMP")" "$(basename "$PARAMS")" > "$(basename "$DUMP").sha256" )

echo
echo "BACKUP OK"
echo "  dump    $DUMP  ($(du -h "$DUMP" | cut -f1))"
echo "  charset $(cat "$DUMP.charset")"
echo "  params  $PARAMS"
echo "  sums    $DUMP.sha256"
echo
echo "Restore with: ./restore-backup.sh $DUMP"
