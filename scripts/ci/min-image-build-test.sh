#!/usr/bin/env bash
# Tests for scripts/deploy/min-image-build.sh with a stubbed registry (curl is a function here).
# Every refusal path must refuse, the happy path must allow, and a missing MIN_IMAGE_BUILD file
# must be a no-op — the gate is sourced into EVERY service-deploy.
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/deploy/min-image-build.sh
export DEPLOY_PLATFORM=linux/arm64 REGISTRY_SCHEME=http
export PINNED_IMAGE="reg:5000/options-edge-x:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/k8s/services/svc"
cd "$TMP"; cp -R "$OLDPWD/scripts" .   # the gate reads k8s/services/<svc>/MIN_IMAGE_BUILD relative to cwd
export SERVICE=svc
LABELS='{}'          # what the stub registry serves as the config's Labels
INDEX=true           # serve an OCI index first (multi-arch), else a bare manifest
curl() {             # stub: last arg is the URL
  local url="${!#}"
  case "$url" in
    */manifests/sha256:1111*) if $INDEX; then
        printf '{"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","platform":{"os":"linux","architecture":"arm64"}},{"digest":"sha256:3333","platform":{"os":"linux","architecture":"amd64"}}]}'
      else
        printf '{"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"sha256:cfg"}}'
      fi ;;
    */manifests/sha256:2222*) printf '{"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"sha256:cfg"}}' ;;
    */blobs/sha256:cfg) printf '{"config":{"Labels":%s}}' "$LABELS" ;;
    *) echo "stub: unexpected url $url" >&2; return 22 ;;
  esac
}
pass=0; fail=0
expect() { # expect <allow|refuse> <description>
  local want="$1" desc="$2" rc=0
  require_min_image_build >/dev/null 2>"$TMP/err" || rc=$?
  if { [ "$want" = allow ] && [ $rc -eq 0 ]; } || { [ "$want" = refuse ] && [ $rc -ne 0 ]; }; then
    pass=$((pass+1)); echo "ok   $desc"
  else
    fail=$((fail+1)); echo "FAIL $desc (rc=$rc) $(cat "$TMP/err")"
  fi
}
rm -f k8s/services/svc/MIN_IMAGE_BUILD
LABELS='{}'; expect allow "no MIN_IMAGE_BUILD file: gate is a no-op"
echo "options-edge-processing 1678" > k8s/services/svc/MIN_IMAGE_BUILD
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'; expect allow "build == min allowed"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/2000/"}'; expect allow "build > min allowed"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1677/"}'; expect refuse "build < min refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'; INDEX=false; expect allow "bare (single-arch) manifest allowed"; INDEX=true
LABELS='{}'; expect refuse "no jenkins-build label refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/other-job/9999/"}'; expect refuse "label from another job refused"
LABELS='{"options-edge.jenkins-build":"garbage"}'; expect refuse "unparseable label refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'
echo "options-edge-processing notanumber" > k8s/services/svc/MIN_IMAGE_BUILD; expect refuse "malformed MIN_IMAGE_BUILD refused"
echo "options-edge-processing 1678" > k8s/services/svc/MIN_IMAGE_BUILD
curl() { return 7; }; expect refuse "registry unreachable refused (fails closed)"
echo "min-image-build-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
