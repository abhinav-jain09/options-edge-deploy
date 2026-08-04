#!/bin/bash
# Portal container entrypoint (REQ-5a, REQ-5b, REQ-5d, REQ-9, REQ-10b).
#
# This deliberately does NOT delegate to upstream's docker/startup.sh. Three reasons, each a real
# defect if we had:
#   1. ORDERING — upstream starts Apache BEFORE checksetup, so on an upgrade the public listener
#      could serve while the schema/params were still being migrated. Here Apache starts LAST.
#   2. SECRET SCOPE — sourcing the secret file with `set -a` would export every credential into the
#      environment of Apache, mod_perl and every CGI, i.e. into /proc/*/environ. Here every consumer
#      reads the projected secret FILE directly: the Apache fragment is rendered from it, the mysql
#      client gets a 0600 defaults-file, and the answers renderer is handed the file path. No secret
#      is placed in the environment or in argv (REQ-9's authorized-location model).
#   3. LOG HYGIENE — upstream prints an "Admin password:" banner. We never print it, so no redaction
#      filter is needed (and no secret ever appears in a sed argv, which would itself be a leak).
set -euo pipefail

SECRET_FILE=${PORTAL_SECRET_FILE:-/run/secrets/bugzilla-req.env}
ANSWERS=/root/docker/portal-answers.txt
TMPL=/root/docker/checksetup-answers.tmpl
SECRET_CONF_DIR=/etc/apache2/conf-portal-secret

log() { echo "[portal] $*"; }

# ---------------------------------------------------------------------------------------------
# 0. Validate the secret mount before anything else (REQ-9: the mount is an authorized location, so
#    its properties are asserted rather than assumed — a directory here would mean the bind source
#    was missing on the host and Docker created one).
[ -f "$SECRET_FILE" ] || { echo "FATAL: $SECRET_FILE is not a regular file" >&2; exit 1; }
[ -L "$SECRET_FILE" ] && { echo "FATAL: $SECRET_FILE is a symlink" >&2; exit 1; }
[ -r "$SECRET_FILE" ] || { echo "FATAL: $SECRET_FILE is not readable" >&2; exit 1; }

# Read the values ONCE, in this shell only. They are never exported.
# shellcheck disable=SC1090
OIDC_CLIENT_SECRET=$(. "$SECRET_FILE" >/dev/null 2>&1; printf '%s' "${OIDC_CLIENT_SECRET:-}")
OIDC_CRYPTO_PASSPHRASE=$(. "$SECRET_FILE" >/dev/null 2>&1; printf '%s' "${OIDC_CRYPTO_PASSPHRASE:-}")
BZ_DB_PASS=$(.        "$SECRET_FILE" >/dev/null 2>&1; printf '%s' "${BZ_DB_PASS:-}")
BZ_ADMIN_PASSWORD=$(. "$SECRET_FILE" >/dev/null 2>&1; printf '%s' "${BZ_ADMIN_PASSWORD:-}")
MARIADB_ROOT_PASSWORD=$(. "$SECRET_FILE" >/dev/null 2>&1; printf '%s' "${MARIADB_ROOT_PASSWORD:-}")

for v in OIDC_CLIENT_SECRET OIDC_CRYPTO_PASSPHRASE BZ_DB_PASS BZ_ADMIN_PASSWORD MARIADB_ROOT_PASSWORD; do
  [ -n "${!v}" ] || { echo "FATAL: $v missing from $SECRET_FILE" >&2; exit 1; }
  # Enforce a safe alphabet. The generator emits hex, and this is what makes the Apache-fragment
  # rendering below injection-proof: a value containing a quote, backslash or newline could
  # otherwise terminate a directive early and alter the generated configuration.
  case "${!v}" in
    *[!0-9a-zA-Z_-]*) echo "FATAL: $v contains characters outside the safe alphabet" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------------------------
log "rendering the OIDC secret fragment (root-owned 0400)"
mkdir -p "$SECRET_CONF_DIR"
umask 077
FRAG_TMP=$(mktemp "$SECRET_CONF_DIR/.oidc-secret.XXXXXX")
{
  printf 'OIDCClientSecret "%s"\n'      "$OIDC_CLIENT_SECRET"
  printf 'OIDCCryptoPassphrase "%s"\n'  "$OIDC_CRYPTO_PASSPHRASE"
} > "$FRAG_TMP"
chown root:root "$FRAG_TMP"; chmod 0400 "$FRAG_TMP"
mv -f "$FRAG_TMP" "$SECRET_CONF_DIR/oidc-secret.conf"   # atomic: a partial fragment is never loaded
unset OIDC_CLIENT_SECRET OIDC_CRYPTO_PASSPHRASE

log "validating Apache configuration"
apachectl -t          # fail closed: a bad directive stops the container rather than serving open
apachectl -S 2>&1 | sed -n '1,12p'   # evidence: which vhost each listener actually resolves to

# ---------------------------------------------------------------------------------------------
log "waiting for ${BZ_DB_HOST}:${BZ_DB_PORT}"
for _ in $(seq 1 120); do nc -z "$BZ_DB_HOST" "$BZ_DB_PORT" && break; sleep 2; done
nc -z "$BZ_DB_HOST" "$BZ_DB_PORT" || { echo "FATAL: database never became reachable" >&2; exit 1; }

# Credentials reach the mysql client through a 0600 defaults-file, never through argv (a password in
# argv is world-readable via /proc/*/cmdline).
DEFAULTS=$(mktemp); chmod 600 "$DEFAULTS"
printf '[client]\nhost=%s\nport=%s\nuser=root\npassword=%s\n' \
  "$BZ_DB_HOST" "$BZ_DB_PORT" "$MARIADB_ROOT_PASSWORD" > "$DEFAULTS"
trap 'rm -f "$DEFAULTS" "$ANSWERS"' EXIT

if [ -z "$(printf "show databases like '%s'" "$BZ_DB_NAME" | mysql --defaults-file="$DEFAULTS" -BN)" ]; then
  log "creating database ${BZ_DB_NAME} and its app user"
  printf "CREATE DATABASE \`%s\`;
GRANT SELECT, INSERT, UPDATE, DELETE, INDEX, ALTER, CREATE, LOCK TABLES,
CREATE TEMPORARY TABLES, DROP, REFERENCES ON \`%s\`.* TO '%s'@'%%%%' IDENTIFIED BY '%s';
FLUSH PRIVILEGES;\n" "$BZ_DB_NAME" "$BZ_DB_NAME" "$BZ_DB_USER" "$BZ_DB_PASS" \
    | mysql --defaults-file="$DEFAULTS" -BN
else
  log "database ${BZ_DB_NAME} already present"
fi
rm -f "$DEFAULTS"
unset MARIADB_ROOT_PASSWORD

# ---------------------------------------------------------------------------------------------
# Answers file carries every REQ-5d parameter, so params are applied deterministically on each boot
# instead of by a human following a checklist. Written 0600 and removed by the EXIT trap.
log "rendering checksetup answers"
umask 077
# The renderer reads the two secrets straight from the projected secret FILE. They are not passed
# through the environment at all — not even a command-scoped assignment, which would still be
# readable in /proc/<pid>/environ while the renderer runs — and not through argv.
python3 /root/docker/render-answers.py "$TMPL" "$ANSWERS" "$SECRET_FILE"
chmod 600 "$ANSWERS"

log "running checksetup (schema + REQ-5d parameters)"
cd /var/www/html
perl checksetup.pl "$ANSWERS"
rm -f "$ANSWERS"
unset BZ_ADMIN_PASSWORD BZ_DB_PASS

log "checksetup complete"

# ---------------------------------------------------------------------------------------------
# Apache starts LAST and in the foreground, so the container's lifetime is Apache's lifetime — a
# crash exits the container and Docker's restart policy applies, instead of upstream's `sleep 1000`
# loop keeping a dead-web container "up".
log "starting Apache (public :80 OIDC-protected, admin :81 native)"
exec apachectl -DFOREGROUND
