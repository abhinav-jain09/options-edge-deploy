#!/usr/bin/env bash
#
# Restore the internal Bugzilla from a backup taken by backup.sh.
#
# Everything is verified BEFORE anything is destroyed, and Apache is only
# started again if every step succeeded - `set -e` plus an explicit trap, so a
# half-restored database can never end up serving traffic.
#
#   ./restore-backup.sh ~/bugzilla-pre-projects-<TS>.sql.gz

set -euo pipefail

WEB=${BZ_WEB_CONTAINER:-options-edge-bugzilla-web}
DB=${BZ_DB_CONTAINER:-options-edge-bugzilla-db}
CNF=/tmp/bz-restore-client.cnf

DUMP=${1:-}
[ -n "$DUMP" ] || { echo "usage: $0 <dump.sql.gz>" >&2; exit 2; }
[ -f "$DUMP" ] || { echo "FATAL: no such dump: $DUMP" >&2; exit 2; }

PARAMS=${DUMP%.sql.gz}
PARAMS=${PARAMS/bugzilla-pre-projects-/bugzilla-params-pre-projects-}.json
CHARSET_FILE="$DUMP.charset"

restored=0
finish() {
  docker exec "$DB" rm -f "$CNF" >/dev/null 2>&1 || true
  if [ "$restored" -ne 1 ]; then
    echo >&2
    echo "RESTORE DID NOT COMPLETE - Apache has been left STOPPED on purpose." >&2
    echo "Fix the problem and re-run; do not start it by hand until this exits 0." >&2
  fi
}
trap finish EXIT

echo "==> verifying the backup before touching anything"
gzip -t "$DUMP"
zgrep -q -- '-- Dump completed' "$DUMP" \
  || { echo "FATAL: $DUMP has no completion footer" >&2; exit 1; }
if [ -f "$DUMP.sha256" ]; then
  ( cd "$(dirname "$DUMP")" && sha256sum -c "$(basename "$DUMP").sha256" )
else
  echo "WARNING: no $DUMP.sha256 to check against" >&2
fi
[ -f "$PARAMS" ] || { echo "FATAL: missing params backup $PARAMS" >&2; exit 1; }
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$PARAMS"

CHARSET_CLAUSE=""
if [ -f "$CHARSET_FILE" ]; then
  read -r CS COLL < "$CHARSET_FILE"
  CHARSET_CLAUSE="CHARACTER SET $CS COLLATE $COLL"
  echo "==> will recreate the schema with $CHARSET_CLAUSE"
else
  echo "WARNING: no $CHARSET_FILE; the schema will use the server defaults" >&2
fi

echo "==> stopping Apache (the container stays up)"
docker exec "$WEB" apachectl stop

docker exec "$DB" sh -c "umask 077; printf '[client]\nuser=root\npassword=%s\n' \"\$MARIADB_ROOT_PASSWORD\" > $CNF"

echo "==> recreating the schema"
docker exec "$DB" sh -c \
  "mysql --defaults-file=$CNF -e 'DROP DATABASE IF EXISTS \`'\"\$BZ_DB_NAME\"'\`; CREATE DATABASE \`'\"\$BZ_DB_NAME\"'\` $CHARSET_CLAUSE;'"

echo "==> importing"
gunzip -c "$DUMP" | docker exec -i "$DB" sh -c \
  "mysql --defaults-file=$CNF \"\$BZ_DB_NAME\""

echo "==> checking the import landed"
COUNT=$(docker exec "$DB" sh -c \
  "mysql --defaults-file=$CNF -BN -e \"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='\$BZ_DB_NAME'\"")
[ "$COUNT" -gt 50 ] \
  || { echo "FATAL: only $COUNT tables after import; the restore is incomplete" >&2; exit 1; }
echo "    $COUNT tables"

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
