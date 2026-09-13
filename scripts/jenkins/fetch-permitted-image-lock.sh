#!/usr/bin/env bash
# fetch-permitted-image-lock.sh <job> <build-number> <permitted-sha> <image-name>
#
# Deployment Permission Rule: when service-deploy triggers the processing image build under a permitted
# processing SHA, the image it then deploys must be THE image that build produced — not whatever the
# mutable :dev/:prod tag resolves to a moment later, which another build may have moved. The processing
# job archives .jenkins-tmp/options-edge-image-lock.env for every build: the full source commit, and
# for every image it pushed the digest-pinned reference plus that image's own commit. This reads the
# lock of the SPECIFIC child build (anonymous read of the controller, like the rest of the build's
# API use), verifies the lock was built from the permitted commit, and prints the digest-pinned
# reference of the one image the deploy needs. Anything missing, ambiguous or mismatched refuses.
#
# stdout: exactly one line, `<registry>/<image>:<tag>@sha256:<64 hex>`. Diagnostics go to stderr.
set -euo pipefail
job="${1:?usage: fetch-permitted-image-lock.sh <job> <build-number> <permitted-sha> <image-name>}"
build="${2:?build number}"
expected="${3:?permitted sha}"
image="${4:?image name}"

refuse() {
  echo "fetch-permitted-image-lock: REFUSED — $*" >&2
  exit 1
}

[ -n "${JENKINS_URL:-}" ] || refuse "JENKINS_URL is not set"
case "$build" in ''|*[!0-9]*) refuse "build number '$build' is not a number" ;; esac
case "$expected" in *[!0-9a-f]*|'') refuse "permitted sha '$expected' is not a commit id" ;; esac
[ "${#expected}" -eq 40 ] || refuse "permitted sha '$expected' is not a full commit id"
case "$image" in *[!A-Za-z0-9._-]*|'') refuse "image name '$image' is not a plain image name" ;; esac

url="${JENKINS_URL%/}/job/${job}/${build}/artifact/.jenkins-tmp/options-edge-image-lock.env"
lock="$(curl -sfg --max-time 30 "$url")" || refuse "could not read the image lock of $job #$build ($url)"

commit="$(printf '%s\n' "$lock" | sed -n 's/^OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT=//p' | head -1)"
[ -n "$commit" ] || refuse "lock of $job #$build carries no OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT"
[ "$commit" = "$expected" ] || refuse "lock of $job #$build was built from $commit, not the permitted $expected"

# The image line: VAR=<registry>/<image>:<tag>@sha256:<64 hex>. Exactly one may match.
lines="$(printf '%s\n' "$lock" | grep -E "^[A-Z0-9_]+_IMAGE=[^=@]*/${image}:[^@ ]+@sha256:[0-9a-f]{64}$" || true)"
n="$(printf '%s\n' "$lines" | grep -c . || true)"
[ "$n" -eq 1 ] || refuse "lock of $job #$build has $n digest-pinned entries for image '$image' (need exactly 1)"
var="${lines%%=*}"
ref="${lines#*=}"
per_image="$(printf '%s\n' "$lock" | sed -n "s/^${var}_GIT_COMMIT=//p" | head -1)"
[ "$per_image" = "$expected" ] || refuse "lock entry $var was built from '${per_image:-<none>}', not the permitted $expected"

echo "fetch-permitted-image-lock: $job #$build built $image from $expected -> $ref" >&2
printf '%s\n' "$ref"
