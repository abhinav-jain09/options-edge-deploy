#!/usr/bin/env bash
# fetch-permitted-image-lock.sh <job> <build-number> <permitted-sha> <image-name> [permitted-contracts-sha]
#
# Deployment Permission Rule: when a deploy job triggers an image build under a permitted source SHA,
# the image it then deploys must be THE image that build produced — not whatever the mutable
# :dev/:prod tag resolves to a moment later, which another build may have moved. Every guarded image
# job archives .jenkins-tmp/options-edge-image-lock.env for every build: the full source commit, the
# guard version it ran, the contracts commit it compiled in (when it has one), and for every image it
# pushed the digest-pinned reference plus that image's own commit. This reads the lock of the SPECIFIC
# child build (anonymous read of the controller, like the rest of the build's API use), verifies it
# was built from the permitted source commit — and, when a permitted contracts SHA is given, from the
# permitted contracts commit — and prints the digest-pinned reference of the one image the deploy
# needs. Anything missing, ambiguous or mismatched refuses.
#
# stdout: exactly one line, `<registry>/<image>:<tag>@sha256:<64 hex>`. Diagnostics go to stderr.
set -euo pipefail
job="${1:?usage: fetch-permitted-image-lock.sh <job> <build-number> <permitted-sha> <image-name> [permitted-contracts-sha]}"
build="${2:?build number}"
expected="${3:?permitted sha}"
image="${4:?image name}"
expected_contracts="${5:-}"

refuse() {
  echo "fetch-permitted-image-lock: REFUSED — $*" >&2
  exit 1
}
is_sha() { case "$1" in *[!0-9a-f]*|'') return 1 ;; esac; [ "${#1}" -eq 40 ]; }

[ -n "${JENKINS_URL:-}" ] || refuse "JENKINS_URL is not set"
case "$build" in ''|*[!0-9]*) refuse "build number '$build' is not a number" ;; esac
is_sha "$expected" || refuse "permitted sha '$expected' is not a full commit id"
[ -z "$expected_contracts" ] || is_sha "$expected_contracts" || refuse "permitted contracts sha '$expected_contracts' is not a full commit id"
case "$image" in *[!A-Za-z0-9._-]*|'') refuse "image name '$image' is not a plain image name" ;; esac
case "$job" in ''|*' '*) refuse "job '$job' is not a job name" ;; esac
path=""
IFS='/' read -r -a segs <<< "$job"
for s in "${segs[@]}"; do path="$path/job/$s"; done

url="${JENKINS_URL%/}${path}/${build}/artifact/.jenkins-tmp/options-edge-image-lock.env"
lock="$(curl -sfg --max-time 30 "$url")" || refuse "could not read the image lock of $job #$build ($url)"

field() { printf '%s\n' "$lock" | sed -n "s/^$1=//p" | head -1; }
commit="$(field OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT)"
[ -n "$commit" ] || refuse "lock of $job #$build carries no OPTIONS_EDGE_IMAGE_LOCK_GIT_COMMIT"
[ "$commit" = "$expected" ] || refuse "lock of $job #$build was built from $commit, not the permitted $expected"
if [ -n "$expected_contracts" ]; then
  contracts="$(field OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT)"
  [ -n "$contracts" ] || refuse "lock of $job #$build carries no OPTIONS_EDGE_IMAGE_LOCK_CONTRACTS_GIT_COMMIT — the child did not record the contracts it compiled in (older definition?)"
  [ "$contracts" = "$expected_contracts" ] || refuse "lock of $job #$build compiled contracts $contracts, not the permitted $expected_contracts"
fi
guard_version="$(field OPTIONS_EDGE_IMAGE_LOCK_GUARD_VERSION)"

# The image line: VAR=<registry>/<image>:<tag>@sha256:<64 hex>. Exactly one may match.
lines="$(printf '%s\n' "$lock" | grep -E "^[A-Z0-9_]+_IMAGE=[^=@]*/${image}:[^@ ]+@sha256:[0-9a-f]{64}$" || true)"
n="$(printf '%s\n' "$lines" | grep -c . || true)"
[ "$n" -eq 1 ] || refuse "lock of $job #$build has $n digest-pinned entries for image '$image' (need exactly 1)"
var="${lines%%=*}"
ref="${lines#*=}"
per_image="$(field "${var}_GIT_COMMIT")"
[ "$per_image" = "$expected" ] || refuse "lock entry $var was built from '${per_image:-<none>}', not the permitted $expected"

echo "fetch-permitted-image-lock: $job #$build built $image from $expected${expected_contracts:+ with contracts $expected_contracts}${guard_version:+ under guard $guard_version} -> $ref" >&2
printf '%s\n' "$ref"
