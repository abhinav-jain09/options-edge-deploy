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
#
# DERIVED SECRET LOCATIONS — stated plainly, because REQ-9's allowlist is only honest if it names
# every copy, not just the projected mount:
#   * /etc/apache2/conf-portal-secret/oidc-secret.conf — root:root 0400, PERSISTS for the container's
#     lifetime by design (Apache reads it at every start/reload).
#   * a mysql defaults-file under /tmp — 0600, TRANSIENT, removed as soon as the DB work finishes.
#   * /root/docker/portal-answers.txt — 0600, TRANSIENT, removed after checksetup.
#   * /var/www/html/localconfig — written by checksetup and holding the database password. PERSISTS
#     for the container's lifetime by necessity (Bugzilla reads it on every request), and it lands
#     on the data volume, so it is also inside any volume backup. This is the copy easiest to
#     forget, and the one most likely to be missed by a naive V10 sweep.
# The two transient files are also removed by an EXIT trap, but a SIGKILL/OOM/node failure can leave
# residue: /tmp is tmpfs (gone on restart), while the answers file lives on the container's writable
# layer and is re-created 0600 on the next boot. V10's "absent everywhere else" must be read against
# THIS list.
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

# Read the values as DATA. The file is never sourced: `.` would execute whatever it contains, and a
# projected Secret is not trusted shell code — a stray `$(...)` would run before any validation
# could reject it. `read-secret` uses the same strict KEY=VALUE parse as render-answers.py, so the
# shell and the renderer can never disagree about quoting, comments, whitespace or duplicate keys.
read_secret() {
  python3 /root/docker/read-secret.py "$SECRET_FILE" "$1"
}

OIDC_CLIENT_SECRET=$(read_secret OIDC_CLIENT_SECRET) || exit 1
OIDC_CRYPTO_PASSPHRASE=$(read_secret OIDC_CRYPTO_PASSPHRASE) || exit 1
MARIADB_ROOT_PASSWORD=$(read_secret MARIADB_ROOT_PASSWORD) || exit 1
BZ_DB_PASS=$(read_secret BZ_DB_PASS) || exit 1
BZ_ADMIN_PASSWORD=$(read_secret BZ_ADMIN_PASSWORD) || exit 1

for v in OIDC_CLIENT_SECRET OIDC_CRYPTO_PASSPHRASE BZ_DB_PASS BZ_ADMIN_PASSWORD MARIADB_ROOT_PASSWORD; do
  [ -n "${!v}" ] || { echo "FATAL: $v missing from $SECRET_FILE" >&2; exit 1; }
  # Safe alphabet. The generator emits hex; this is what makes rendering into quoted Apache
  # directives and into the Perl answers file injection-proof.
  case "${!v}" in
    *[!0-9a-zA-Z_-]*) echo "FATAL: $v contains characters outside the safe alphabet" >&2; exit 1 ;;
  esac
done

# Non-secret settings are validated per their own grammar, not merely for quotes: they are
# interpolated into SQL identifiers and into unquoted Perl source, where a wrong shape is a defect.
case "$BZ_DB_NAME" in ''|*[!0-9a-zA-Z_]*) echo "FATAL: BZ_DB_NAME must be [0-9a-zA-Z_]" >&2; exit 1 ;; esac
case "$BZ_DB_USER" in ''|*[!0-9a-zA-Z_]*) echo "FATAL: BZ_DB_USER must be [0-9a-zA-Z_]" >&2; exit 1 ;; esac
case "$BZ_DB_PORT" in ''|*[!0-9]*)        echo "FATAL: BZ_DB_PORT must be numeric" >&2; exit 1 ;; esac

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

# Reconcile the database, the app account, its password and its grants on EVERY boot — not only
# when the database is absent. An existing database whose user was dropped, whose password was
# rotated, or whose grants drifted would otherwise be left broken with no signal. All three
# statements are idempotent.
#
# The existence test uses an exact-match query rather than SHOW DATABASES LIKE, which would treat
# `%` and `_` in the name as wildcards.
EXISTS=$(printf "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='%s'" "$BZ_DB_NAME" \
         | mysql --defaults-file="$DEFAULTS" -BN)
if [ "${EXISTS:-0}" = "0" ]; then
  log "creating database ${BZ_DB_NAME}"
  printf "CREATE DATABASE \`%s\`;\n" "$BZ_DB_NAME" | mysql --defaults-file="$DEFAULTS" -BN
else
  log "database ${BZ_DB_NAME} already present"
fi

log "reconciling the ${BZ_DB_USER} account and its grants"
printf "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';
ALTER USER '%s'@'%%' IDENTIFIED BY '%s';
GRANT SELECT, INSERT, UPDATE, DELETE, INDEX, ALTER, CREATE, LOCK TABLES,
CREATE TEMPORARY TABLES, DROP, REFERENCES ON \`%s\`.* TO '%s'@'%%';
FLUSH PRIVILEGES;\n" \
  "$BZ_DB_USER" "$BZ_DB_PASS" "$BZ_DB_USER" "$BZ_DB_PASS" "$BZ_DB_NAME" "$BZ_DB_USER" \
  | mysql --defaults-file="$DEFAULTS" -BN

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

# checksetup's answers file seeds a FRESH install only: Bugzilla/Config.pm:121-131 @ 276673ab6
# consults the answers hash inside `unless (exists $param->{$name})`, so an answer is ignored once
# the param exists in data/params.json. Convergence therefore needs an explicit pass, or a value
# changed by hand — or changed later in our template — would silently never take effect.
log "converging REQ-5d parameters"
perl /root/docker/reconcile-params.pl

log "checksetup complete"

# ---------------------------------------------------------------------------------------------
# Apache starts LAST and in the foreground, so the container's lifetime is Apache's lifetime — a
# crash exits the container and Docker's restart policy applies, instead of upstream's `sleep 1000`
# loop keeping a dead-web container "up".
log "starting Apache (public :80 OIDC-protected, admin :81 native)"
exec apachectl -DFOREGROUND
