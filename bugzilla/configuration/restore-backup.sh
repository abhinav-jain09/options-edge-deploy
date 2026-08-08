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
#
# Read the header with awk's own early exit rather than `gunzip | head | grep`:
# under `set -o pipefail`, head closing the pipe makes gunzip die of SIGPIPE and
# the whole pipeline "fails" on a perfectly good multi-hundred-megabyte dump.
HEADER=$(gunzip -c "$DUMP" | awk 'NR <= 30; NR == 30 {exit}')
case "$HEADER" in
  *"MySQL dump"*|*"MariaDB dump"*) ;;
  *) echo "FATAL: $DUMP does not look like a mariadb-dump" >&2; exit 1 ;;
esac
# Anchored on both sides: a prefix match would let a dump of `bugs_old` pass as
# a dump of `bugs`, and we are about to DROP the target.
printf '%s\n' "$HEADER" | grep -Eq "^-- Host:.*Database: ${DBNAME}\$" \
  || { echo "FATAL: $DUMP was not taken from database '$DBNAME'" >&2
       printf '%s\n' "$HEADER" | grep -E "^-- Host:" >&2
       exit 1; }
echo "==> target database: $DBNAME (matches the dump header exactly)"

echo "==> stopping Apache and waiting for it to drain"
docker exec "$WEB" apachectl stop
apache_stopped=1
for _ in $(seq 1 30); do
  if ! docker exec "$WEB" sh -c 'pgrep -x apache2 >/dev/null 2>&1 || pgrep -x httpd >/dev/null 2>&1'; then
    drained=1; break
  fi
  sleep 1
done
# apachectl stop is asynchronous. Dropping the schema while a worker is still
# alive would let an in-flight request touch a database that is being replaced.
[ "${drained:-0}" = "1" ] \
  || { echo "FATAL: Apache workers are still running after 30s; refusing to drop the schema" >&2; exit 1; }
echo "    no Apache processes remain"

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

echo "==> flushing Bugzilla's memcached, if it has one"
# Restarting the web container does not clear a SHARED memcached service, and
# every cached object in it now describes a database that no longer exists.
docker exec "$WEB" perl -e '
  use lib qw(/var/www/html /var/www/html/lib /var/www/html/local/lib/perl5);
  use Bugzilla;
  my $mc = Bugzilla->memcached;
  if ($mc && $mc->enabled) { $mc->clear_all; print "    memcached flushed\n" }
  else                     { print "    no memcached configured\n" }
' || { echo "FATAL: could not flush memcached; stale objects would describe the old database" >&2; exit 1; }

echo "==> restoring params.json"
docker cp "$PARAMS" "$WEB:/var/www/html/data/params.json"
docker exec "$WEB" chown www-data:www-data /var/www/html/data/params.json

echo "==> restarting the web tier and waiting for it to answer"
docker restart "$WEB" >/dev/null
apache_stopped=0   # it is running again; the trap must not claim otherwise
for _ in $(seq 1 60); do
  # Any HTTP response proves Apache and Bugzilla are up. requirelogin=1 makes
  # an unauthenticated /rest/version return 401, which is a perfectly good
  # liveness signal - demanding 200 here would fail a healthy instance.
  if docker exec "$WEB" sh -c \
       'curl -s -o /dev/null -w "%{http_code}" http://localhost/rest/version 2>/dev/null' \
       | grep -Eq '^(200|401|403)$'; then
    healthy=1; break
  fi
  sleep 2
done
if [ "${healthy:-0}" != "1" ]; then
  echo "FATAL: the web tier did not respond within 120s after restart." >&2
  echo "       The DATA IS RESTORED, but the instance is not serving. Apache is" >&2
  echo "       RUNNING - stop it yourself if you need the instance dark:" >&2
  echo "         docker exec $WEB apachectl stop" >&2
  exit 1
fi
echo "    the web tier responded"

restored=1
echo
echo "RESTORE OK. Now confirm the PRE-change shape - verify-projects.py asserts"
echo "the new model and is EXPECTED to fail against a restored database:"
echo "  curl -s -H \"X-BUGZILLA-API-KEY: \$BZ_API_KEY\" http://localhost:8092/rest/product_selectable"
