#!/usr/bin/env bash
# Build + deploy the requirement-intake portal stack (REQ-4, REQ-5a, REQ-8).
#
# Jenkins-only: this script is the body of Jenkinsfile.bugzilla-req. It never runs on the prod host
# as a deployment source of truth — Jenkins drives it from the CI agent and delivers artifacts to
# .252 over ssh, matching the existing prod amd64 native-build pattern.
#
# Flow:
#   1. clone upstream Bugzilla at the PINNED commit into the build context (traceability, REQ-4)
#   2. build both images natively on .252 (amd64) and push them to the .252:5000 registry
#   3. resolve both to DIGESTS and record them (immutability, REQ-4)
#   4. capture the currently-running digests as last-known-good BEFORE touching anything (REQ-8)
#   5. deliver the compose file, `docker compose up -d`
#   6. bounded readiness polling; on failure classify state, keep diagnostics, redeploy LKG (REQ-8)
set -euo pipefail

PROD_HOST=${PROD_HOST:-192.168.100.252}
PROD_SSH=${PROD_SSH:-abhinav@${PROD_HOST}}
REGISTRY=${REGISTRY:-${PROD_HOST}:5000}
REMOTE_DEPLOY_DIR=${REMOTE_DEPLOY_DIR:-/home/options-edge/deploy/bugzilla-req}
REMOTE_BUILD_DIR=${REMOTE_BUILD_DIR:-/home/options-edge/build/bugzilla-req}
BUILD_TAG=${BUILD_TAG:?BUILD_TAG must be set (buildno-sha)}
DEPLOY_DRY_RUN=${DEPLOY_DRY_RUN:-true}

# The exact upstream commit the internal instance runs. Bumping this is a reviewed PR (REQ-12); it
# is what makes the REQ-5c source-pinned auth behaviour (Env.pm / Verify.pm / Bug.pm line anchors)
# apply to this image.
BUGZILLA_UPSTREAM_REPO=${BUGZILLA_UPSTREAM_REPO:-https://github.com/bugzilla/bugzilla}
BUGZILLA_UPSTREAM_COMMIT=${BUGZILLA_UPSTREAM_COMMIT:-276673ab6}

WEB_REPO="options-edge/bugzilla-req-web"
DB_REPO="options-edge/bugzilla-req-db"

say() { echo "=== $*"; }
rsh() { ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" "$@"; }

# ---------------------------------------------------------------------------------------------
say "1/6 preparing build context on ${PROD_HOST} (upstream @ ${BUGZILLA_UPSTREAM_COMMIT})"
rsh "set -euo pipefail
  rm -rf '${REMOTE_BUILD_DIR}' && mkdir -p '${REMOTE_BUILD_DIR}'
  cd '${REMOTE_BUILD_DIR}'
  git init -q .
  git remote add origin '${BUGZILLA_UPSTREAM_REPO}'
  git fetch -q --depth 1 origin '${BUGZILLA_UPSTREAM_COMMIT}'
  git checkout -q FETCH_HEAD
  echo \"upstream HEAD: \$(git rev-parse HEAD)\""

say "shipping the portal overlay into the build context"
tar -C "$(dirname "$0")/../../bugzilla-req" -cf - . | rsh "tar -C '${REMOTE_BUILD_DIR}' -xf -"

if [ "$DEPLOY_DRY_RUN" = "true" ]; then
  say "DEPLOY_DRY_RUN=true — context prepared and verified; no image is built, nothing is deployed"
  rsh "ls '${REMOTE_BUILD_DIR}/Dockerfile' '${REMOTE_BUILD_DIR}/oidc/000-portal-public.conf' '${REMOTE_BUILD_DIR}/docker-compose.yml' >/dev/null && echo 'context OK'"
  exit 0
fi

# ---------------------------------------------------------------------------------------------
say "2/6 building both images natively (amd64) on ${PROD_HOST}"
rsh "set -euo pipefail
  cd '${REMOTE_BUILD_DIR}'
  docker build -f Dockerfile           -t '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}' .
  docker build -f Dockerfile.mariadb   -t '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'  .
  docker push -q '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}'
  docker push -q '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'"

say "3/6 resolving digests (compose consumes digests, never tags)"
WEB_DIGEST=$(rsh "docker inspect --format '{{index .RepoDigests 0}}' '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}'")
DB_DIGEST=$(rsh  "docker inspect --format '{{index .RepoDigests 0}}' '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'")
[ -n "$WEB_DIGEST" ] && [ -n "$DB_DIGEST" ] || { echo "FATAL: digest resolution failed" >&2; exit 1; }
say "web=${WEB_DIGEST}"
say "db =${DB_DIGEST}"

# ---------------------------------------------------------------------------------------------
say "4/6 recording last-known-good (before any mutation)"
LKG_FILE="${REMOTE_DEPLOY_DIR}/last-known-good.env"
rsh "set -euo pipefail
  mkdir -p '${REMOTE_DEPLOY_DIR}' /home/options-edge/data/bugzilla-req/data /home/options-edge/data/bugzilla-req/mysql
  PREV_WEB=\$(docker inspect --format '{{.Image}}' options-edge-bugzilla-req-web 2>/dev/null || true)
  PREV_DB=\$(docker inspect  --format '{{.Image}}' options-edge-bugzilla-req-db  2>/dev/null || true)
  if [ -n \"\$PREV_WEB\" ]; then
    printf 'BUGZILLA_REQ_WEB_IMAGE=%s\nBUGZILLA_REQ_DB_IMAGE=%s\n' \"\$PREV_WEB\" \"\$PREV_DB\" > '${LKG_FILE}.prev'
    echo 'previous digests recorded for rollback'
  else
    echo 'no previous deployment — first install, rollback target is \"stack removed\"'
  fi"

say "5/6 delivering compose + starting the stack"
tar -C "$(dirname "$0")/../../bugzilla-req" -cf - docker-compose.yml | rsh "tar -C '${REMOTE_DEPLOY_DIR}' -xf -"
rsh "set -euo pipefail
  cd '${REMOTE_DEPLOY_DIR}'
  printf 'BUGZILLA_REQ_WEB_IMAGE=%s\nBUGZILLA_REQ_DB_IMAGE=%s\n' '${WEB_DIGEST}' '${DB_DIGEST}' > .env
  chmod 600 .env
  cp .env '${LKG_FILE}'
  docker compose pull -q 2>/dev/null || true
  docker compose up -d"

# ---------------------------------------------------------------------------------------------
say "6/6 bounded readiness polling (REQ-10b gates)"
READY=false
for i in $(seq 1 24); do
  # Readiness, not liveness: this must exercise Perl + CGI + the database, so it queries Bugzilla's
  # REST API on the ADMIN listener. With requirelogin on, Bugzilla answers with a specific
  # login-required error (Bugzilla.pm:333 @ 276673ab6) — reaching that error already proves the
  # whole stack booted, which is why no API key is needed (and none may be used: it would land in
  # `docker inspect` and break V10).
  BODY=$(rsh "curl -s -m 8 http://127.0.0.1:8095/rest/parameters || true" 2>/dev/null || true)
  if echo "$BODY" | grep -qE '"(parameters|error|code)"'; then READY=true; break; fi

  # Distinguish 'not yet ready' from a permanent failure so a bad config is not retried for minutes.
  if rsh "docker ps -a --filter name=options-edge-bugzilla-req-web --format '{{.Status}}'" | grep -qiE 'exited|dead'; then
    say "web container is in a terminal state — stopping the poll early"
    break
  fi
  sleep 10
done

if [ "$READY" != "true" ]; then
  say "READINESS FAILED — capturing diagnostics BEFORE any rollback overwrites the state"
  rsh "docker ps -a --filter name=options-edge-bugzilla-req --format '{{.Names}} {{.Status}}'; \
       docker logs --tail 80 options-edge-bugzilla-req-web 2>&1 | tail -80; \
       docker logs --tail 40 options-edge-bugzilla-req-db  2>&1 | tail -40" || true
  if rsh "test -f '${LKG_FILE}.prev'"; then
    say "redeploying last-known-good digests"
    rsh "cd '${REMOTE_DEPLOY_DIR}' && cp '${LKG_FILE}.prev' .env && docker compose up -d" || true
    say "LKG redeployed. NOTE: an image rollback is only valid within one Bugzilla schema — \
checksetup migrates forward only, so a rollback across a schema-migrating bump needs a DB restore."
  else
    say "first install: no LKG to fall back to. Leaving the stack down rather than half-serving."
    rsh "cd '${REMOTE_DEPLOY_DIR}' && docker compose down" || true
  fi
  exit 1
fi

say "stack is READY"
rsh "docker ps --filter name=options-edge-bugzilla-req --format '{{.Names}} {{.Status}}'"
say "V-lan socket evidence (both portal ports MUST be loopback-only):"
rsh "ss -ltn | grep -E ':(8093|8095)' || echo 'NO LISTENERS FOUND'"
say "internal Bugzilla untouched (REQ-6 spot check):"
rsh "docker ps --filter name=options-edge-bugzilla-web --format '{{.Names}} {{.Status}}'"
