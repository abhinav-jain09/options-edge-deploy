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
gunzip -c "$DUMP" | tail -5 | grep -q -- '-- Dump completed' \
  || { echo "FATAL: $DUMP does not end with a completion footer" >&2; exit 1; }
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$PARAMS"

# These two values are interpolated into a root SQL statement, so they are
# allowlisted to bare identifiers rather than trusted because they came off disk.
grep -Eq '^[A-Za-z0-9_]+ [A-Za-z0-9_]+$' "$CHARSET" \
  || { echo "FATAL: $CHARSET is not '<charset> <collation>': $(cat "$CHARSET")" >&2; exit 1; }
read -r CS COLL < "$CHARSET"
echo "==> will recreate the schema with CHARACTER SET $CS COLLATE $COLL"

docker exec "$DB" sh -c "umask 077; printf '[client]\nuser=root\npassword=%s\n' \"\$MARIADB_ROOT_PASSWORD\" > $CNF"
db_sql() { docker exec "$DB" sh -c "mysql --defaults-file=$CNF -BN -e \"$1\""; }

DBNAME=$(docker exec "$DB" sh -c 'printf %s "$BZ_DB_NAME"')
[ -n "$DBNAME" ] || { echo "FATAL: \$BZ_DB_NAME is not set in $DB" >&2; exit 1; }
echo "==> target database: $DBNAME"

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
