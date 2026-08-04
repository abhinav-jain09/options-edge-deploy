#!/bin/bash
# Render the OIDC secret fragment at container start (REQ-9).
#
# Secrets arrive as a FILE MOUNT, never as container environment variables — so they do not appear
# in `docker inspect`, in healthcheck text, or in the process environment of anything but this
# script. The rendered fragment is the one additional authorized location, root-owned 0400, and V10
# asserts exactly that allowlist: present with the right ownership/mode here, absent everywhere else.
set -euo pipefail
set +x   # never trace the values

SECRET_FILE=${PORTAL_SECRET_FILE:-/run/secrets/bugzilla-req.env}
OUT_DIR=/etc/apache2/conf-portal-secret
OUT="$OUT_DIR/oidc-secret.conf"

if [ ! -r "$SECRET_FILE" ]; then
  echo "FATAL: secret file $SECRET_FILE is missing or unreadable — refusing to start" >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; . "$SECRET_FILE"; set +a

: "${OIDC_CLIENT_SECRET:?FATAL: OIDC_CLIENT_SECRET not set in $SECRET_FILE}"
: "${OIDC_CRYPTO_PASSPHRASE:?FATAL: OIDC_CRYPTO_PASSPHRASE not set in $SECRET_FILE}"

mkdir -p "$OUT_DIR"
umask 077
TMP=$(mktemp "$OUT_DIR/.oidc-secret.XXXXXX")
{
  printf 'OIDCClientSecret "%s"\n' "$OIDC_CLIENT_SECRET"
  printf 'OIDCCryptoPassphrase "%s"\n' "$OIDC_CRYPTO_PASSPHRASE"
} > "$TMP"
chown root:root "$TMP"
chmod 0400 "$TMP"
mv -f "$TMP" "$OUT"   # atomic replace, so a partially written fragment is never loaded

echo "OIDC secret fragment rendered (values not logged)"
