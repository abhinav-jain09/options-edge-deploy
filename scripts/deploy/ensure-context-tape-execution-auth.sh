#!/usr/bin/env bash
# Shared infra owns this credential. Never print, archive, rotate or pass it in command arguments.
set -euo pipefail
set +x
case "${ENVIRONMENT:-}" in dev|production) ;; *) echo 'FATAL: visual execution credentials are only for dev/production' >&2; exit 1;; esac
case "${DEPLOY_DRY_RUN:-true}" in true|false) ;; *) echo 'FATAL: invalid DEPLOY_DRY_RUN' >&2; exit 1;; esac
namespace=options-edge
secret=context-tape-execution-auth
validate() {
  python3 -c 'import base64,json,re,sys
v=json.load(sys.stdin)
try:
 token=base64.b64decode(v["data"]["token"],validate=True).decode("ascii")
 assert re.fullmatch(r"[A-Za-z0-9_-]{32,256}",token)
except Exception:
 sys.exit("FATAL: existing execution credential is invalid; explicit rotation required")'
}
existing="$(kubectl --request-timeout=10s -n "$namespace" get secret "$secret" --ignore-not-found -o json)"
if [ -n "$existing" ]; then
  printf '%s' "$existing" | validate
  unset existing
  echo 'Context Tape execution credential is present; preserved.'
  exit 0
fi
if [ "${DEPLOY_DRY_RUN:-true}" = true ]; then
  echo 'Plan: create the missing Context Tape execution credential; dry run changes nothing.'
  exit 0
fi
# The permitted Jenkins guard must have run before any mutating caller reaches here.
[ "${PERMITTED_SHA_GUARD:-}" = PASSED ] && [ -n "${BUILD_NUMBER:-}" ] && [ -n "${JOB_NAME:-}" ] || {
  echo 'FATAL: execution credential creation requires a permitted Jenkins run' >&2; exit 1;
}
umask 077
secret_file="$(mktemp)"
trap 'rm -f "$secret_file"' EXIT
python3 -c 'import secrets,sys;sys.stdout.write(secrets.token_urlsafe(48))' > "$secret_file"
# A concurrent creator may win. Re-read and validate the canonical credential either way.
if ! kubectl --request-timeout=10s -n "$namespace" create secret generic "$secret" --from-file="token=$secret_file" >/dev/null 2>&1; then
  echo 'Credential create did not complete; checking the existing resource.'
fi
kubectl --request-timeout=10s -n "$namespace" get secret "$secret" -o json | validate
echo 'Context Tape execution credential is ready.'
