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
mkdir -p "$TMP/k8s/services/svc" "$TMP/k8s/services/other"
cd "$TMP"; cp -R "$OLDPWD/scripts" .   # the gate reads k8s/services/<svc>/MIN_IMAGE_BUILD relative to cwd
export SERVICE=svc
MIN="http://j:8085 options-edge-processing 1678"
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
echo "$MIN" > k8s/services/svc/MIN_IMAGE_BUILD
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'; expect allow "build == min allowed"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678"}'; expect allow "label without trailing slash allowed"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/2000/"}'; expect allow "build > min allowed"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1677/"}'; expect refuse "build < min refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'; INDEX=false; expect allow "bare (single-arch) manifest allowed"; INDEX=true
LABELS='{}'; expect refuse "no jenkins-build label refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/other-job/9999/"}'; expect refuse "label from another job refused"
LABELS='{"options-edge.jenkins-build":"garbage"}'; expect refuse "unparseable label refused"
LABELS='{"options-edge.jenkins-build":"https://evil.example/job/options-edge-processing/9999/"}'; expect refuse "right job+number on ANOTHER Jenkins host refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/evil/job/options-edge-processing/9999/"}'; expect refuse "extra path between base and /job/ refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'
echo "options-edge-processing 1678" > k8s/services/svc/MIN_IMAGE_BUILD; expect refuse "MIN_IMAGE_BUILD without the Jenkins base refused"
echo "http://j:8085 options-edge-processing notanumber" > k8s/services/svc/MIN_IMAGE_BUILD; expect refuse "malformed build number refused"
printf '%s' "$MIN" > k8s/services/svc/MIN_IMAGE_BUILD; expect allow "file without trailing newline still read"
echo "$MIN" > k8s/services/svc/MIN_IMAGE_BUILD
PINNED_IMAGE="reg:5000/options-edge-x:dev"; expect refuse "non-digest image refused"
PINNED_IMAGE="reg:5000/options-edge-x:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"

# --- the monolithic render path ---
cat > services.yaml <<'YAML'
services:
  - { name: svc,   deployments: [svc-service] }
  - { name: other, deployments: [other-service] }
YAML
echo "$MIN" > k8s/services/other/MIN_IMAGE_BUILD
render() { # <svc image> [<other image>|-]
  { printf 'apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: svc-service\nspec:\n  template:\n    spec:\n      containers:\n        - name: a\n          image: %s\n' "$1"
    if [ "${2:-}" != "-" ] && [ -n "${2:-}" ]; then
      printf -- '---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: other-service\nspec:\n  template:\n    spec:\n      containers:\n        - name: b\n          image: %s\n' "$2"; fi
  } > "$TMP/render.yaml"; printf '%s' "$TMP/render.yaml"
}
expect_render() { local want="$1" desc="$2" rc=0
  require_min_image_builds_in_render "$TMP/render.yaml" >/dev/null 2>"$TMP/err" || rc=$?
  if { [ "$want" = allow ] && [ $rc -eq 0 ]; } || { [ "$want" = refuse ] && [ $rc -ne 0 ]; }; then
    pass=$((pass+1)); echo "ok   render: $desc"; else fail=$((fail+1)); echo "FAIL render: $desc (rc=$rc) $(cat "$TMP/err")"; fi
}
GOOD="reg:5000/options-edge-x:dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'
render "$GOOD" "$GOOD" >/dev/null; expect_render allow "both declaring services new enough"
render "$GOOD" - >/dev/null; expect_render allow "a declaring service that does not render here is skipped"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1677/"}'
render "$GOOD" "$GOOD" >/dev/null; expect_render refuse "an old image in the render refused"
LABELS='{"options-edge.jenkins-build":"http://j:8085/job/options-edge-processing/1678/"}'
render "reg:5000/options-edge-x:dev" - >/dev/null; expect_render refuse "a non-digest image in the render refused"
rm k8s/services/svc/MIN_IMAGE_BUILD k8s/services/other/MIN_IMAGE_BUILD
render "reg:5000/options-edge-x:dev" "reg:5000/options-edge-x:dev" >/dev/null; expect_render allow "no declaring services: render path is a no-op"
echo "$MIN" > k8s/services/svc/MIN_IMAGE_BUILD
curl() { return 7; }; expect refuse "registry unreachable refused (fails closed)"
echo "min-image-build-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
