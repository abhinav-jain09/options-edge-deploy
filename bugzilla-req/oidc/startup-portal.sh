#!/bin/bash
# Portal container entrypoint (REQ-5a/5b/5d, REQ-9, REQ-10b).
#
# Wraps the upstream startup: render the OIDC secret fragment FIRST (so Apache never starts without
# it), validate the Apache config, then hand off to upstream's checksetup + Apache flow. Upstream
# starts Apache before checksetup; here Apache starts only after the config validates and the DB is
# reachable, so the container never serves a half-initialised Bugzilla on the public listener.
set -euo pipefail

echo "[portal] rendering OIDC secret fragment"
/usr/local/bin/render-oidc-secrets.sh

# Secrets are read from the mount, not the environment, so the DB credentials the upstream
# checksetup flow expects are sourced here rather than passed via `environment:` in compose (REQ-9 —
# nothing sensitive in `docker inspect`).
set +x
SECRET_FILE=${PORTAL_SECRET_FILE:-/run/secrets/bugzilla-req.env}
set -a; . "$SECRET_FILE"; set +a
# Upstream's checksetup contract needs these; they come from the mounted file rather than compose
# `environment:`, so none of them is visible in `docker inspect` (REQ-9).
export MARIADB_ROOT_PASSWORD BZ_DB_PASS BZ_ADMIN_PASSWORD
: "${BZ_DB_PASS:?FATAL: BZ_DB_PASS not set in $SECRET_FILE}"
: "${BZ_ADMIN_PASSWORD:?FATAL: BZ_ADMIN_PASSWORD not set in $SECRET_FILE}"

echo "[portal] validating Apache configuration"
apachectl -t   # fail closed: a bad vhost/OIDC directive must stop the container, not serve open

echo "[portal] waiting for database ${BZ_DB_HOST}:${BZ_DB_PORT}"
for _ in $(seq 1 120); do
  nc -z "$BZ_DB_HOST" "$BZ_DB_PORT" && break
  sleep 2
done
nc -z "$BZ_DB_HOST" "$BZ_DB_PORT" || { echo "FATAL: database never became reachable" >&2; exit 1; }

# Upstream's startup.sh creates the database if absent, renders checksetup answers and runs
# checksetup.pl. It also starts Apache and then sleeps forever; we let it do the DB + checksetup
# work, but Apache is already validated above and upstream's `apachectl start` is idempotent.
echo "[portal] running upstream checksetup"

# Upstream's startup.sh prints an "Admin password: ..." banner on stdout. Container logs are a
# searched surface in V10 and are covered data under REQ-13, so the banner is filtered out and the
# literal value scrubbed from anything else it emits. The passwords are generated as hex, so they
# carry no sed metacharacters and this substitution is safe.
exec /root/docker/startup.sh 2>&1 \
  | sed -e '/Admin password/d' \
        -e "s/${BZ_ADMIN_PASSWORD}/<redacted>/g" \
        -e "s/${BZ_DB_PASS}/<redacted>/g" \
        -e "s/${MARIADB_ROOT_PASSWORD}/<redacted>/g"
