#!/usr/bin/env bash
#
# Take a verified backup of the internal Bugzilla before a provisioning run.
#
# The verification is the point. A failed `mariadb-dump` still produces a
# perfectly valid *empty* gzip that passes `gzip -t`, so this checks the dump's
# own completion footer, records the schema charset and a table manifest, and
# checksums all three. If it does not print BACKUP OK, there is no usable
# backup. restore-backup.sh refuses to run without every one of those artefacts.
#
#   ./backup.sh [output-directory]

set -euo pipefail
umask 077   # dumps contain every bug in the tracker; params.json is sensitive

WEB=${BZ_WEB_CONTAINER:-options-edge-bugzilla-web}
DB=${BZ_DB_CONTAINER:-options-edge-bugzilla-db}
OUTDIR=${1:-$HOME}
[ -d "$OUTDIR" ] || { echo "FATAL: no such directory: $OUTDIR" >&2; exit 2; }

# $$ keeps two runs in the same second from overwriting each other's artefacts.
TS=$(date +%Y%m%dT%H%M%S)-$$
BASE="$OUTDIR/bugzilla-pre-projects-$TS"
DUMP="$BASE.sql.gz"
PARAMS="$BASE.params.json"
CHARSET="$BASE.charset"
MANIFEST="$BASE.tables"
SUMS="$BASE.sha256"
CNF=/tmp/bz-backup-client.$$.cnf

cleanup() { docker exec "$DB" rm -f "$CNF" >/dev/null 2>&1 || true; }
trap cleanup EXIT

db_sql() { docker exec "$DB" sh -c "mysql --defaults-file=$CNF -BN -e \"$1\""; }

echo "==> writing a protected client option file inside $DB"
# Keeps the root password out of the client process's argv, where anything else
# in that container could read it from /proc.
docker exec "$DB" sh -c "umask 077; printf '[client]\nuser=root\npassword=%s\n' \"\$MARIADB_ROOT_PASSWORD\" > $CNF"

echo "==> waiting for Apache to drain"
# `apachectl stop` is asynchronous: an in-flight request can still commit after
# it returns, and that write would be missing from the dump we are about to
# call a backup.
for _ in $(seq 1 30); do
  if ! docker exec "$WEB" sh -c 'pgrep -x apache2 >/dev/null 2>&1 || pgrep -x httpd >/dev/null 2>&1'; then
    drained=1; break
  fi
  sleep 1
done
[ "${drained:-0}" = "1" ] || {
  echo "FATAL: Apache is still running in $WEB after 30s. Stop it first:" >&2
  echo "       docker exec $WEB apachectl stop" >&2
  exit 1
}
echo "    no Apache processes remain"

echo "==> checking the database exists"
DBNAME=$(docker exec "$DB" sh -c 'printf %s "$BZ_DB_NAME"')
[ -n "$DBNAME" ] || { echo "FATAL: \$BZ_DB_NAME is not set in $DB" >&2; exit 1; }
# It is interpolated into SQL that runs through `sh -c`, exactly as in the
# restore script - so hold it to the same bare-identifier allowlist.
printf '%s' "$DBNAME" | grep -Eq '^[A-Za-z0-9_]+$' \
  || { echo "FATAL: refusing database name '$DBNAME' - not a bare identifier" >&2; exit 1; }
FOUND=$(db_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='$DBNAME'")
[ "$FOUND" = "1" ] || { echo "FATAL: database '$DBNAME' does not exist" >&2; exit 1; }
echo "    $DBNAME"

echo "==> recording the schema charset and table manifest"
# A restore that recreated the schema with the server defaults would silently
# change these; the manifest is what proves an import was complete.
db_sql "SELECT CONCAT(DEFAULT_CHARACTER_SET_NAME,' ',DEFAULT_COLLATION_NAME) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='$DBNAME'" > "$CHARSET"
grep -Eq '^[A-Za-z0-9_]+ [A-Za-z0-9_]+$' "$CHARSET" \
  || { echo "FATAL: unexpected charset/collation: $(cat "$CHARSET")" >&2; exit 1; }
db_sql "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='$DBNAME' ORDER BY TABLE_NAME" | sort > "$MANIFEST"
test -s "$MANIFEST" || { echo "FATAL: the database has no tables" >&2; exit 1; }

echo "==> dumping"
docker exec "$DB" sh -c \
  "mariadb-dump --defaults-file=$CNF --single-transaction --routines --triggers --events --hex-blob \"\$BZ_DB_NAME\"" \
  | gzip > "$DUMP"

echo "==> verifying the dump is complete"
gzip -t "$DUMP"
# The footer is mariadb-dump's last non-empty line. Checking the actual last
# line - not "somewhere near the end" - is what makes truncation detectable.
LAST=$(gunzip -c "$DUMP" | awk 'NF {last = $0} END {print last}')
case "$LAST" in
  '-- Dump completed'*) ;;
  *) echo "FATAL: $DUMP does not end with a completion footer (last line: ${LAST:0:80}) - truncated dump" >&2; exit 1 ;;
esac

echo "==> copying params.json"
docker cp "$WEB:/var/www/html/data/params.json" "$PARAMS"
chmod 600 "$PARAMS"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$PARAMS" \
  || { echo "FATAL: copied params.json is not valid JSON" >&2; exit 1; }

( cd "$OUTDIR" && sha256sum \
    "$(basename "$DUMP")" "$(basename "$PARAMS")" \
    "$(basename "$CHARSET")" "$(basename "$MANIFEST")" > "$(basename "$SUMS")" )

echo
echo "BACKUP OK"
echo "  dump     $DUMP  ($(du -h "$DUMP" | cut -f1))"
echo "  charset  $(cat "$CHARSET")"
echo "  tables   $(wc -l < "$MANIFEST")"
echo "  params   $PARAMS"
echo "  sums     $SUMS"
echo
echo "Restore with: ./restore-backup.sh $DUMP"
