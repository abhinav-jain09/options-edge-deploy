#!/usr/bin/env bash
# Build + deploy the requirement-intake portal stack (REQ-4, REQ-5a, REQ-8).
#
# Jenkins-only: this is the body of Jenkinsfile.bugzilla-req. Jenkins never runs ON the production
# machine; it drives the build over ssh and delivers artifacts, matching the existing prod amd64
# native-build pattern.
#
# Flow: clone upstream at the pinned commit -> build both images -> resolve and VALIDATE digests ->
# record last-known-good (validated digests only) -> deliver compose -> bounded readiness polling ->
# on failure: classify, capture sanitized diagnostics, roll back to LKG and verify the rollback.
set -euo pipefail

PROD_HOST=${PROD_HOST:-192.168.100.252}
PROD_SSH=${PROD_SSH:-abhinav@${PROD_HOST}}
REGISTRY=${REGISTRY:-${PROD_HOST}:5000}
REMOTE_DEPLOY_DIR=${REMOTE_DEPLOY_DIR:-/home/options-edge/deploy/bugzilla-req}
REMOTE_BUILD_DIR=${REMOTE_BUILD_DIR:-/home/options-edge/build/bugzilla-req}
REMOTE_EVIDENCE_DIR=${REMOTE_EVIDENCE_DIR:-/home/options-edge/deploy/bugzilla-req/evidence}
BUILD_TAG=${BUILD_TAG:?BUILD_TAG must be set (prod-<build>-<sha>)}
DEPLOY_DRY_RUN=${DEPLOY_DRY_RUN:-true}

# The exact upstream commit the internal instance runs. Bumping it is a reviewed PR (REQ-12) and is
# what keeps the REQ-5c source-pinned auth behaviour (Env.pm / Verify.pm / Bug.pm anchors) true here.
BUGZILLA_UPSTREAM_REPO=${BUGZILLA_UPSTREAM_REPO:-https://github.com/bugzilla/bugzilla}
BUGZILLA_UPSTREAM_COMMIT=${BUGZILLA_UPSTREAM_COMMIT:-276673ab6}

WEB_REPO="options-edge/bugzilla-req-web"
DB_REPO="options-edge/bugzilla-req-db"
HERE="$(cd "$(dirname "$0")/../.." && pwd)"

say()  { echo "=== $*"; }
rsh()  { ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" "$@"; }
die()  { echo "FATAL: $*" >&2; exit 1; }

# A digest-form reference is `host/repo@sha256:<64 hex>`. Compose's `${VAR:?}` only proves non-empty,
# so a tag would satisfy it and silently defeat digest-only deployment — this is the real gate.
assert_digest() {
  local what=$1 ref=$2 repo=$3
  [[ "$ref" == *"/${repo}@sha256:"* ]] || die "$what is not a digest reference for ${repo}: '${ref}'"
  [[ "${ref##*@sha256:}" =~ ^[0-9a-f]{64}$ ]] || die "$what has a malformed sha256: '${ref}'"
}

# ---------------------------------------------------------------------------------------------
say "1/7 preparing build context on ${PROD_HOST} (upstream @ ${BUGZILLA_UPSTREAM_COMMIT})"
rsh "set -euo pipefail
  rm -rf '${REMOTE_BUILD_DIR}' && mkdir -p '${REMOTE_BUILD_DIR}'
  cd '${REMOTE_BUILD_DIR}'
  git init -q .
  git remote add origin '${BUGZILLA_UPSTREAM_REPO}'
  git fetch -q --depth 1 origin '${BUGZILLA_UPSTREAM_COMMIT}'
  git checkout -q FETCH_HEAD
  git rev-parse HEAD > .upstream-commit
  echo \"upstream HEAD: \$(cat .upstream-commit)\""

say "overlaying the portal build context"
tar -C "${HERE}/bugzilla-req" -cf - . | rsh "tar -C '${REMOTE_BUILD_DIR}' -xf -"

if [ "$DEPLOY_DRY_RUN" = "true" ]; then
  say "DEPLOY_DRY_RUN=true — context prepared and verified; no image built, nothing deployed"
  rsh "cd '${REMOTE_BUILD_DIR}' && ls Dockerfile Dockerfile.mariadb oidc/000-portal-public.conf \
       oidc/001-portal-admin.conf oidc/entrypoint.sh docker-compose.yml >/dev/null && echo 'context OK'"
  exit 0
fi

# ---------------------------------------------------------------------------------------------
say "2/7 building both images natively (amd64) on ${PROD_HOST}"
# The build is niced and memory-capped: it runs apt + CPAN on the machine that also serves the live
# trading stack, so it must not win a CPU/RAM fight with it (R-12 shared-host contention).
rsh "set -euo pipefail
  cd '${REMOTE_BUILD_DIR}'
  nice -n 15 docker build --memory 4g --cpu-shares 512 -f Dockerfile         -t '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}' .
  nice -n 15 docker build --memory 2g --cpu-shares 512 -f Dockerfile.mariadb -t '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'  .
  docker push -q '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}'
  docker push -q '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'"

say "3/7 resolving + validating digests"
# Select the RepoDigest belonging to this repository rather than trusting index 0: an image can
# carry several, and picking the wrong one would deploy something other than what we just pushed.
WEB_DIGEST=$(rsh "docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' '${REGISTRY}/${WEB_REPO}:${BUILD_TAG}' | grep -F '${WEB_REPO}@sha256:' | head -1" | tr -d '\r')
DB_DIGEST=$(rsh  "docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' '${REGISTRY}/${DB_REPO}:${BUILD_TAG}'  | grep -F '${DB_REPO}@sha256:'  | head -1" | tr -d '\r')
assert_digest "web image" "$WEB_DIGEST" "$WEB_REPO"
assert_digest "db image"  "$DB_DIGEST"  "$DB_REPO"
say "web=${WEB_DIGEST}"
say "db =${DB_DIGEST}"

say "4/7 recording build evidence (REQ-4 traceability)"
rsh "set -euo pipefail
  mkdir -p '${REMOTE_EVIDENCE_DIR}'
  E='${REMOTE_EVIDENCE_DIR}/${BUILD_TAG}'
  mkdir -p \"\$E\"
  cp '${REMOTE_BUILD_DIR}/.upstream-commit' \"\$E/upstream-commit.txt\"
  printf '%s\n%s\n' '${WEB_DIGEST}' '${DB_DIGEST}' > \"\$E/image-digests.txt\"
  # Exact package inventory of the built web image: what an apt build cannot lock as an INPUT, we at
  # least record as an OUTPUT, which is the honest limit of REQ-4's traceability claim.
  docker run --rm --entrypoint dpkg '${WEB_DIGEST}' -l > \"\$E/dpkg-web.txt\" 2>/dev/null || \
    docker run --rm '${WEB_DIGEST}' dpkg -l > \"\$E/dpkg-web.txt\"
  # The OIDC module version is security-relevant: the Require-claim regex form and OIDCWhiteListedClaims
  # support are version-dependent, so the version that answered the compatibility gate is recorded.
  docker run --rm '${WEB_DIGEST}' dpkg -s libapache2-mod-auth-openidc > \"\$E/mod-auth-openidc.txt\" 2>/dev/null || true
  echo \"evidence written to \$E\"; ls \"\$E\""

say "5/7 module + config compatibility gate (before anything is exposed)"
# A syntax check on the CI agent proves nothing about the image. This runs the real binary against
# the real config inside the built image. A missing directive here is a launch blocker: without
# OIDCWhiteListedClaims every token claim would reach CGI, and without the Require-claim regex form
# the email contract silently degrades to `valid-user`.
rsh "set -euo pipefail
  docker run --rm --entrypoint /bin/bash '${WEB_DIGEST}' -c '
    set -e
    mkdir -p /etc/apache2/conf-portal-secret
    printf \"OIDCClientSecret \\\"placeholder\\\"\nOIDCCryptoPassphrase \\\"placeholder\\\"\n\" \
      > /etc/apache2/conf-portal-secret/oidc-secret.conf
    apachectl -t
    apachectl -S
  '" || die "the built image failed its own Apache/OIDC configuration gate"

# ---------------------------------------------------------------------------------------------
say "6/7 recording last-known-good, then deploying"
LKG="${REMOTE_DEPLOY_DIR}/last-known-good.env"
rsh "set -euo pipefail
  mkdir -p '${REMOTE_DEPLOY_DIR}' /home/options-edge/data/bugzilla-req/data /home/options-edge/data/bugzilla-req/mysql
  # LKG is the PREVIOUS .env — i.e. registry digest references that can actually be pulled again.
  # Container .Image is a local image ID, which would make a rollback depend on that ID still
  # existing on this host. A stale .prev is removed when there is nothing running, so failure
  # handling can never mistake it for a valid rollback target.
  if docker inspect options-edge-bugzilla-req-web >/dev/null 2>&1 \
     && docker inspect options-edge-bugzilla-req-db >/dev/null 2>&1 \
     && [ -f '${REMOTE_DEPLOY_DIR}/.env' ]; then
    cp '${REMOTE_DEPLOY_DIR}/.env' '${LKG}.prev'
    echo 'previous digests recorded for rollback'
  else
    rm -f '${LKG}.prev'
    echo 'no complete previous deployment — first install; rollback target is \"stack down\"'
  fi"

tar -C "${HERE}/bugzilla-req" -cf - docker-compose.yml | rsh "tar -C '${REMOTE_DEPLOY_DIR}' -xf -"
rsh "set -euo pipefail
  cd '${REMOTE_DEPLOY_DIR}'
  umask 077
  printf 'BUGZILLA_REQ_WEB_IMAGE=%s\nBUGZILLA_REQ_DB_IMAGE=%s\n' '${WEB_DIGEST}' '${DB_DIGEST}' > .env
  cp .env '${LKG}'
  docker compose up -d --remove-orphans"

# ---------------------------------------------------------------------------------------------
say "7/7 bounded readiness polling (REQ-10b)"
# Readiness must exercise Perl + CGI + the database, so it queries Bugzilla's REST API on the ADMIN
# listener and asserts a SPECIFIC shape. With requirelogin on, Bugzilla answers 401 with
# {"error":true,"code":410,...} (Bugzilla.pm:333 @ 276673ab6) — reaching that error already proves
# the whole stack booted. No API key is used: one would land in `docker inspect` and break V10.
ready_probe() {
  rsh "curl -s -m 8 -o /tmp/portal-ready.json -w '%{http_code}' http://127.0.0.1:8095/rest/parameters 2>/dev/null; \
       echo ' '; python3 -c \"
import json,sys
try:
    d=json.load(open('/tmp/portal-ready.json'))
except Exception:
    print('BADJSON'); sys.exit()
print('READY' if ('parameters' in d or d.get('code')==410) else 'UNEXPECTED')
\" 2>/dev/null" | tr -d '\r' | tr '\n' ' '
}

terminal_failure() {
  rsh "docker inspect -f '{{.State.Status}}:{{.State.Restarting}}:{{.RestartCount}}:{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
       options-edge-bugzilla-req-web 2>/dev/null || echo missing" | tr -d '\r'
}

READY=false
for _ in $(seq 1 30); do
  P=$(ready_probe)
  case "$P" in *READY*) READY=true; say "readiness probe: $P"; break ;; esac
  S=$(terminal_failure)
  # Permanent-failure classification: a container that exited, is missing, is flapping, or has gone
  # unhealthy will not become ready by waiting, so stop early and keep the diagnostics.
  case "$S" in
    missing|exited*|dead*|*:unhealthy) say "terminal container state '$S' — stopping the poll early"; break ;;
  esac
  case "$S" in *:true:*) say "container is restarting (state '$S')" ;; esac
  R=${S#*:*:}; R=${R%%:*}
  if [ "${R:-0}" -ge 3 ] 2>/dev/null; then say "restart loop detected (count $R) — stopping early"; break; fi
  sleep 10
done

if [ "$READY" != "true" ]; then
  say "READINESS FAILED — capturing SANITIZED diagnostics before any rollback"
  # Allowlisted lines only. Raw container logs are a V10 surface and covered data (REQ-13); the
  # entrypoint never prints a secret, but a third-party stack trace might echo one, so the log is
  # filtered rather than trusted.
  rsh "docker ps -a --filter name=options-edge-bugzilla-req --format '{{.Names}} {{.Status}}'; \
       docker logs --tail 200 options-edge-bugzilla-req-web 2>&1 \
         | grep -aiE '^\[portal\]|FATAL|error|denied|refused|Syntax|checksetup|DBD|Can.t connect' \
         | tail -40; \
       docker logs --tail 100 options-edge-bugzilla-req-db 2>&1 \
         | grep -aiE 'error|fatal|denied|ready for connections|InnoDB' | tail -20" || true

  if rsh "test -f '${LKG}.prev'"; then
    say "rolling back to last-known-good"
    rsh "cd '${REMOTE_DEPLOY_DIR}' && cp '${LKG}.prev' .env && docker compose up -d --remove-orphans" \
      || die "LKG rollback command FAILED — the portal stack is in an undefined state, investigate now"
    ROLLED_OK=false
    for _ in $(seq 1 18); do
      case "$(ready_probe)" in *READY*) ROLLED_OK=true; break ;; esac
      sleep 10
    done
    [ "$ROLLED_OK" = "true" ] || die "LKG rollback did NOT become ready — portal stack is down, investigate now"
    say "LKG rollback verified ready. NOTE: an image rollback is only valid within one Bugzilla \
schema — checksetup migrates forward only, so rolling back across a schema-migrating bump needs a \
database restore from the REQ-10a backups."
  else
    say "first install: no LKG. Bringing the stack DOWN rather than leaving it half-serving."
    rsh "cd '${REMOTE_DEPLOY_DIR}' && docker compose down" || true
  fi
  exit 1
fi

say "stack is READY"
rsh "docker ps --filter name=options-edge-bugzilla-req --format '{{.Names}} {{.Status}}'"
say "V-lan evidence — both portal ports MUST be loopback-only, and 8094 must be untouched:"
rsh "ss -ltn | grep -E ':(8093|8095|8094)' || echo 'NO LISTENERS FOUND'"
say "REQ-6 internal-Bugzilla no-op evidence (before/after comparison is the Jenkins job's gate):"
rsh "docker inspect -f '{{.Name}} image={{.Image}} mounts={{range .Mounts}}{{.Source}},{{end}}' \
     options-edge-bugzilla-web options-edge-bugzilla-db 2>/dev/null || echo 'internal stack not found'"
