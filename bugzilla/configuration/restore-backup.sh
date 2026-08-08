#!/usr/bin/env bash
#
# Restore the internal Bugzilla from a backup taken by backup.sh.
#
# Every artefact is checksummed and validated BEFORE anything is destroyed, and
# Apache is started again only if every step succeeded - so a half-restored
# database can never end up serving traffic.
#
#   ./restore-backup.sh ~/bugzilla-pre-projects-<TS>.sql.gz

set -euo pipefail
umask 077

WEB=${BZ_WEB_CONTAINER:-options-edge-bugzilla-web}
DB=${BZ_DB_CONTAINER:-options-edge-bugzilla-db}
CNF=/tmp/bz-restore-client.$$.cnf

DUMP=${1:-}
[ -n "$DUMP" ] || { echo "usage: $0 <dump.sql.gz>" >&2; exit 2; }
[ -f "$DUMP" ] || { echo "FATAL: no such dump: $DUMP" >&2; exit 2; }

BASE=${DUMP%.sql.gz}
PARAMS="$BASE.params.json"
CHARSET="$BASE.charset"
MANIFEST="$BASE.tables"
SUMS="$BASE.sha256"

apache_stopped=0
restored=0
finish() {
  docker exec "$DB" rm -f "$CNF" >/dev/null 2>&1 || true
  if [ "$restored" -ne 1 ]; then
    echo >&2
    if [ "$apache_stopped" -eq 1 ]; then
      echo "RESTORE DID NOT COMPLETE - Apache has been left STOPPED on purpose." >&2
      echo "Fix the problem and re-run; do not start it by hand until this exits 0." >&2
    else
      echo "RESTORE ABORTED before anything was touched - the database and" >&2
      echo "Apache are exactly as they were." >&2
    fi
  fi
}
trap finish EXIT

echo "==> verifying the backup set before touching anything"
for f in "$PARAMS" "$CHARSET" "$MANIFEST" "$SUMS"; do
  [ -f "$f" ] || { echo "FATAL: missing backup artefact: $f" >&2; exit 1; }
done
( cd "$(dirname "$DUMP")" && sha256sum -c "$(basename "$SUMS")" )
gzip -t "$DUMP"
LAST=$(gunzip -c "$DUMP" | awk 'NF {last = $0} END {print last}')
case "$LAST" in
  '-- Dump completed'*) ;;
  *) echo "FATAL: $DUMP does not end with a completion footer (last line: ${LAST:0:80})" >&2; exit 1 ;;
esac
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$PARAMS"

# These values are interpolated into a root SQL statement, so they are
# allowlisted to bare identifiers rather than trusted because they came off
# disk. The file must be exactly ONE line: `grep -q` would otherwise pass on a
# file whose first line is hostile and whose second is harmless, and `read`
# takes the first.
[ "$(wc -l < "$CHARSET")" -eq 1 ] \
  || { echo "FATAL: $CHARSET must contain exactly one line" >&2; exit 1; }
read -r CS COLL REST_OF_LINE < "$CHARSET"
[ -z "${REST_OF_LINE:-}" ] \
  || { echo "FATAL: $CHARSET has unexpected trailing content" >&2; exit 1; }
printf '%s' "$CS"   | grep -Eq '^[A-Za-z0-9_]+$' \
  || { echo "FATAL: refusing charset '$CS' - not a bare identifier" >&2; exit 1; }
printf '%s' "$COLL" | grep -Eq '^[A-Za-z0-9_]+$' \
  || { echo "FATAL: refusing collation '$COLL' - not a bare identifier" >&2; exit 1; }
echo "==> will recreate the schema with CHARACTER SET $CS COLLATE $COLL"

docker exec "$DB" sh -c "umask 077; printf '[client]\nuser=root\npassword=%s\n' \"\$MARIADB_ROOT_PASSWORD\" > $CNF"
db_sql() { docker exec "$DB" sh -c "mysql --defaults-file=$CNF -BN -e \"$1\""; }

DBNAME=$(docker exec "$DB" sh -c 'printf %s "$BZ_DB_NAME"')
[ -n "$DBNAME" ] || { echo "FATAL: \$BZ_DB_NAME is not set in $DB" >&2; exit 1; }
# This name reaches a root DROP DATABASE. Anything but a bare identifier could
# change the statement or target another schema entirely.
printf '%s' "$DBNAME" | grep -Eq '^[A-Za-z0-9_]+$' \
  || { echo "FATAL: refusing database name '$DBNAME' - not a bare identifier" >&2; exit 1; }

# Confirm we are about to drop the schema this dump actually came from.
grep -Eq "^-- (Host|Server version|MySQL dump)" <(gunzip -c "$DUMP" | head -5) \
  || { echo "FATAL: $DUMP does not look like a mariadb-dump" >&2; exit 1; }
gunzip -c "$DUMP" | head -30 | grep -q "Database: $DBNAME" \
  || { echo "FATAL: $DUMP was not taken from database '$DBNAME'" >&2; exit 1; }
echo "==> target database: $DBNAME (matches the dump header)"

echo "==> stopping Apache (the container stays up)"
docker exec "$WEB" apachectl stop
apache_stopped=1

echo "==> recreating the schema"
db_sql "DROP DATABASE IF EXISTS \\\`$DBNAME\\\`; CREATE DATABASE \\\`$DBNAME\\\` CHARACTER SET $CS COLLATE $COLL;"

echo "==> importing"
gunzip -c "$DUMP" | docker exec -i "$DB" sh -c \
  "mysql --defaults-file=$CNF \"\$BZ_DB_NAME\""

echo "==> comparing the restored tables against the manifest"
db_sql "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='$DBNAME' ORDER BY TABLE_NAME" \
  | sort > "$MANIFEST.restored"
if ! diff -u "$MANIFEST" "$MANIFEST.restored"; then
  echo "FATAL: the restored schema does not match the manifest taken at backup time" >&2
  exit 1
fi
echo "    $(wc -l < "$MANIFEST") tables, exactly as recorded"
rm -f "$MANIFEST.restored"

echo "==> restoring params.json"
docker cp "$PARAMS" "$WEB:/var/www/html/data/params.json"
docker exec "$WEB" chown www-data:www-data /var/www/html/data/params.json

echo "==> restarting the web tier"
docker restart "$WEB" >/dev/null

restored=1
echo
echo "RESTORE OK. Now confirm the PRE-change shape - verify-projects.py asserts"
echo "the new model and is EXPECTED to fail against a restored database:"
echo "  curl -s -H \"X-BUGZILLA-API-KEY: \$BZ_API_KEY\" http://localhost:8092/rest/product_selectable"
