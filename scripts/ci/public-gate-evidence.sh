#!/usr/bin/env bash
# public-gate-evidence.sh — earn the PGL-072 gate's evidence, or produce nothing.
#
# verify-public-gate.sh refuses to run the public Gamma Lab above zero replicas unless a JSON record
# exists for the EXACT image digest being deployed, recording PGL-050, PGL-051 and PGL-052 as PASS.
# Nothing produced that record, which is why a freshly built image could not be deployed publicly at
# all. This is the producer.
#
# THE ONE PROPERTY THAT MATTERS: this script RUNS THE TESTS ITSELF and writes the evidence only if
# they pass. It does not accept a result as an argument. A producer that took `--pgl-050 PASS` would
# be a formatter for someone else's claim, and the gate's whole point is that a claim is not
# evidence — "evidence that can be committed is evidence that can be written by anyone opening a
# pull request".
#
# WHAT THE EVIDENCE ASSERTS, and what it does not:
#
#   asserts     revision R passed the PGL-050/051/052 tests, and the registry ASSOCIATES digest D
#               with R through a versioned tag that some pusher created
#   does NOT    prove D was built from R. Nothing here can: the tag is an assertion by whoever
#               pushed it, and this registry authenticates nobody.
#
# See the trust boundary below. This is a control against attesting the WRONG THING by mistake.
#
# Usage:
#   public-gate-evidence.sh --repo <options-edge checkout> --image <ref:tag> --revision <sha>
#                           --ci-build-url <url> --out-dir <dir OUTSIDE this repository>
#                           [--expect-digest <sha256:...>]
set -euo pipefail

REPO="" IMAGE="" REVISION="" CI_BUILD_URL="" OUT_DIR="" EXPECT_DIGEST=""
REQUIRED_IDS=(PGL-050 PGL-051 PGL-052)
# PublicBoardUpstreamRequestTest is the one that OBSERVES the real outbound request and asserts no
# Authorization, no Cookie and no forwarded identity header, on the dedicated upstream — it carries
# all three ids. PublicSurfaceIsolationTest corroborates PGL-051 from the other direction (the
# public image constructs no internal client at all).
# The SAME classes the evidence for the currently-running public image recorded (7 classes, 52
# tests, via options-edge-web-deploy #963). A narrower producer would quietly LOWER the bar for
# every future attestation, which is the opposite of the point.
TESTS="PublicBoardUpstreamRequestTest,PublicBoardUpstreamTest,PublicSurfaceSecurityChainTest,PublicSurfaceRoutesTest,PublicLogHygieneTest,PublicSurfaceTest,PublicSurfaceStartupInvariantTest,PublicSurfaceIsolationTest"

WORK_HDR="$(mktemp)"
trap 'rm -f "$WORK_HDR"' EXIT
die() { echo "EVIDENCE FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

need_value() { [ "$2" -ge 2 ] || die "$1 requires a value"; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)           need_value "$1" $#; REPO="$2"; shift 2 ;;
    --image)          need_value "$1" $#; IMAGE="$2"; shift 2 ;;
    --revision)       need_value "$1" $#; REVISION="$2"; shift 2 ;;
    --ci-build-url)   need_value "$1" $#; CI_BUILD_URL="$2"; shift 2 ;;
    --out-dir)        need_value "$1" $#; OUT_DIR="$2"; shift 2 ;;
    --expect-digest)  need_value "$1" $#; EXPECT_DIGEST="$2"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# NOTE: the Jenkins agent is macOS, whose /bin/bash is 3.2.57 — `${v,,}` is a bash 4 construct and
# fails there with "bad substitution" at RUNTIME while still parsing cleanly. Lowercased with tr.
for v in REPO IMAGE CI_BUILD_URL OUT_DIR; do
  if [ -z "${!v}" ]; then
    die "--$(printf '%s' "$v" | tr '[:upper:]_' '[:lower:]-') is required"
  fi
done
command -v jq >/dev/null || die "jq is required"

echo "==> public gate evidence (PGL-050/051/052)"
echo "  repo:     $REPO"
echo "  image:    $IMAGE"
echo "  revision: $REVISION"

# ------------------------------------------------- 0. DERIVE the revision from the registry
#
# THE PROVENANCE PROBLEM, AND WHY THIS SOLVES IT. Evidence has to say "revision R passed, and
# revision R is what digest D runs". Being TOLD R by the caller proves nothing — a caller can test
# any revision and staple the result to any image, which is exactly the false attestation the gate
# exists to prevent. The image carries no `org.opencontainers.image.revision` label, so for a while
# it looked as though only a change to the shared build could fix this.
#
# WHAT THIS ACTUALLY ESTABLISHES — and it is not cryptographic proof. The web build pushes a
# second, versioned tag alongside the mutable one:
#   VERSIONED_IMAGE="${IMAGE%:*}:prod-${BUILD_NUMBER}-$(git rev-parse --short=12 HEAD)"
# so the registry holds `prod-970-8daf949c18d1` pointing at the same digest as `:prod`. The tag NAME
# carries the build number and the revision. Finding the versioned tag that shares the deployed
# digest recovers the revision FROM THE REGISTRY rather than from whoever is running this script —
# which is the property that matters against the failure this gate is really about: a well-meaning
# operator attesting the wrong thing.
#
# TRUST BOUNDARY, stated rather than glossed: a tag name is an ASSERTION by whoever pushed it. This
# registry authenticates nobody, so anyone able to push could mint prod-<n>-<sha> pointing at any
# digest and this derivation would believe it. That is worth being explicit about — and it is also
# not the weak link: anyone who can write arbitrary tags can push a malicious image to :prod
# directly, which no evidence requirement would catch. Closing it properly needs the registry to
# restrict pushes and refuse tag overwrites, or a signed attestation (cosign et al) binding image to
# source. Until then this defends against MISTAKE, not against an attacker who already owns the
# registry, and the evidence should be read that way.
resolve_revision() {
  local target="$1" registry repo_path url tags page t d found=""
  registry="${IMAGE%%/*}"
  repo_path="${IMAGE#*/}"; repo_path="${repo_path%:*}"
  # Paginate. A truncated listing that happens to omit the genuine tag would otherwise leave only
  # whatever else matched, which is the wrong way for this to fail.
  url="http://${registry}/v2/${repo_path}/tags/list?n=1000"
  tags=""
  while [ -n "$url" ]; do
    page="$(curl -sS -m 30 -D "$WORK_HDR" "$url")" || die "could not list tags at $url"
    # jq, not sed: a hand-rolled parse of JSON is its own source of wrong answers.
    tags="$tags $(printf '%s' "$page" | jq -r '.tags[]? // empty')"
    url="$(tr -d '\r' < "$WORK_HDR" | sed -n 's/^[Ll]ink:.*<\([^>]*\)>.*rel="next".*/\1/p' | tail -1)"
    [ -n "$url" ] && case "$url" in /*) url="http://${registry}${url}" ;; esac
  done
  for t in $tags; do
    # EXACTLY the shape the build emits: prod-<build>-<12 hex>. A looser pattern accepts more
    # things that were never produced by that build.
    # A glob cannot express this: `prod-[0-9]*-*` is one digit then anything, so `prod-1oops-<sha>`
    # would pass. Validate the WHOLE tag against the exact shape the build emits.
    printf '%s' "$t" | grep -qE '^prod-[0-9]+-[0-9a-f]{12}$' || continue
    d="$(curl -sS -m 30 -o /dev/null -D - \
      -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
      -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
      "http://${registry}/v2/${repo_path}/manifests/${t}" \
      | tr -d '\r' | awk 'tolower($1)=="docker-content-digest:"{print $2}' | tail -1)"
    [ "$d" = "$target" ] || continue
    if [ -n "$found" ] && [ "$found" != "${t##*-}" ]; then
      die "two different revisions claim this digest: $found and ${t##*-}.
       One of them is wrong, and picking either would be a guess."
    fi
    found="${t##*-}"
  done
  [ -n "$found" ] || return 1
  printf '%s' "$found"
}

# ---------------------------------------------------------------- 1. the revision must be REAL
# `-d .git` is WRONG for a git worktree, where .git is a FILE pointing at the real git dir. Ask git.
git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "$REPO is not a git checkout"
# `git diff` misses UNTRACKED files, and an untracked Java source, test, resource or
# .mvn/maven.config changes what Maven compiles and runs while the evidence still names the
# committed HEAD. --porcelain covers tracked AND untracked.
DIRT="$(git -C "$REPO" status --porcelain)"
[ -z "$DIRT" ] || die "the checkout at $REPO is not clean:
$DIRT
       Evidence must name a revision anyone else can check out and re-run."
ok "checkout is clean"

# ---------------------------------------------------------------- 2. resolve the digest BEFORE
resolve_digest() {
  local ref="$1" registry repo_path tag
  registry="${ref%%/*}"
  repo_path="${ref#*/}"; tag="${repo_path##*:}"; repo_path="${repo_path%:*}"
  curl -sS -m 30 -o /dev/null -D - \
    -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
    "http://${registry}/v2/${repo_path}/manifests/${tag}" \
    | tr -d '\r' | awk 'tolower($1)=="docker-content-digest:"{print $2}' | tail -1
}
DIGEST_BEFORE="$(resolve_digest "$IMAGE")"
[[ "$DIGEST_BEFORE" == sha256:* ]] || die "could not resolve a digest for $IMAGE (got '$DIGEST_BEFORE')"
ok "digest before tests: $DIGEST_BEFORE"

if [[ -n "$EXPECT_DIGEST" && "$EXPECT_DIGEST" != "$DIGEST_BEFORE" ]]; then
  die "the caller expected $EXPECT_DIGEST but $IMAGE resolves to $DIGEST_BEFORE.
       Something re-pushed this tag between the build and this attestation."
fi

# The revision is DERIVED from the registry, never taken on trust. If --revision was supplied it is
# treated as a CHECK on that derivation, not as the source of it.
DERIVED_REV="$(resolve_revision "$DIGEST_BEFORE" || true)"
[ -n "$DERIVED_REV" ] || die "no versioned prod-<build>-<sha> tag in the registry shares the digest
       $DIGEST_BEFORE.
       Without one there is nothing tying this image to a revision, and an attestation that cannot
       name what it tested is not evidence. (Was this image pushed by the normal web build?)"
ok "registry ties $DIGEST_BEFORE to revision $DERIVED_REV"

# expand the 12-char tag sha to the full commit, and require the checkout to BE it
FULL_REV="$(git -C "$REPO" rev-parse "$DERIVED_REV^{commit}" 2>/dev/null || true)"
[ -n "$FULL_REV" ] || die "the registry names revision $DERIVED_REV, which is not in $REPO.
       Fetch it before attesting — the tests must run on the revision the image was built from."
if [ -n "$REVISION" ] && [ "$REVISION" != "$FULL_REV" ] && [ "$REVISION" != "$DERIVED_REV" ]; then
  die "the caller asked to record $REVISION, but the registry says this digest was built from
       $FULL_REV. Recording the caller's answer would be exactly the substitution the gate exists
       to prevent."
fi
REVISION="$FULL_REV"

HEAD_SHA="$(git -C "$REPO" rev-parse HEAD)"
[ "$HEAD_SHA" = "$REVISION" ] \
  || die "the checkout is at $HEAD_SHA but this digest was built from $REVISION.
       The tests run against the working tree, so check that revision out first."
ok "checkout is at the revision this image was built from"

# ---------------------------------------------------------------- 3. RUN the tests
# No result is accepted as input. If this fails, the script exits and no evidence exists.
#
# THE REPORTS DIRECTORY IS DESTROYED FIRST. Without this, a Maven run that exits 0 without executing
# the named tests is indistinguishable from one that ran them, because reports from a PREVIOUS run
# in the same workspace still satisfy the postcondition below — a false attestation by leftovers.
REPORT_DIR="$REPO/target/surefire-reports"
rm -rf "$REPORT_DIR"
echo "==> running $TESTS at $REVISION"
( cd "$REPO" && mvn -B -q -Dtest="$TESTS" -DfailIfNoSpecifiedTests=true test ) \
  || die "the PGL tests did not pass at $REVISION — no evidence written.
       That is the correct outcome: the gate exists because a public user's bearer must not reach
       the internal namespace, and these tests are what observe that it does not."

# Maven's exit code alone is not enough. While building this, a `-q` run exited 0 and I could find
# no surefire report — the report is written under the FULLY QUALIFIED name, so my check was wrong
# rather than the run. But the near-miss is the point: "green" and "actually executed the tests I
# named" are different claims, and this file exists to make only the second one. So each named test
# must have produced a report showing a positive run count and no failures or errors.
TOTAL_TESTS=0
for t in $(printf '%s' "$TESTS" | tr ',' ' '); do
  # The XML report is machine-readable; the .txt is formatted for humans and was never a contract.
  xml=""
  for cand in "$REPORT_DIR"/TEST-*."$t".xml; do
    [ -f "$cand" ] && xml="$cand" && break
  done
  [ -n "$xml" ] || die "$t produced no surefire XML in a directory this run created from empty.
       Maven exited 0, but nothing proves this test ran, and an attestation from a run that executed
       nothing is exactly the 'wish' the gate refuses."
  runs="$(sed -n 's/.*[^a-z]tests="\([0-9]*\)".*/\1/p' "$xml" | head -1)"
  fails="$(sed -n 's/.*failures="\([0-9]*\)".*/\1/p' "$xml" | head -1)"
  errs="$(sed -n 's/.*errors="\([0-9]*\)".*/\1/p' "$xml" | head -1)"
  [ -n "$runs" ] && [ "$runs" -gt 0 ] || die "$t reported $runs tests — a zero-test run is not a pass"
  [ "${fails:-1}" = "0" ] && [ "${errs:-1}" = "0" ] \
    || die "$t reported failures=$fails errors=$errs"
  TOTAL_TESTS=$((TOTAL_TESTS + runs))
  ok "$t tests=$runs failures=0 errors=0"
done
ok "total tests executed: $TOTAL_TESTS"

ok "PGL tests passed"

# ---------------------------------------------------------------- 4. the digest must NOT have moved
DIGEST_AFTER="$(resolve_digest "$IMAGE")"
[[ "$DIGEST_AFTER" == "$DIGEST_BEFORE" ]] \
  || die "$IMAGE moved during the run: $DIGEST_BEFORE -> $DIGEST_AFTER.
       The attestation would name an artifact nobody tested."
ok "digest unchanged across the run"

# ---------------------------------------------------------------- 5. write it OUTSIDE this repo
DEPLOY_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
mkdir -p "$OUT_DIR"
OUT_REAL="$(cd "$OUT_DIR" && pwd -P)"
[[ "$OUT_REAL" != "$DEPLOY_REPO_ROOT"/* && "$OUT_REAL" != "$DEPLOY_REPO_ROOT" ]] \
  || die "refusing to write evidence inside the deploy repository ($OUT_REAL).
       The gate rejects it there for the same reason: committable evidence is not evidence."

EVIDENCE_FILE="$OUT_REAL/public-gate-${DIGEST_BEFORE/:/-}.json"
REQ_JSON="$(printf '%s\n' "${REQUIRED_IDS[@]}" | jq -R . | jq -s 'map({(.): "PASS"}) | add')"
jq -n \
  --arg imageDigest "$DIGEST_BEFORE" \
  --arg image "$IMAGE" \
  --arg sourceRevision "$REVISION" \
  --arg ciBuildUrl "$CI_BUILD_URL" \
  --arg producedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg tests "$TESTS" \
  --argjson requirements "$REQ_JSON" \
  --argjson testsRun "$TOTAL_TESTS" \
  '{imageDigest:$imageDigest, image:$image, sourceRevision:$sourceRevision, ciBuildUrl:$ciBuildUrl,
    producedAt:$producedAt, tests:($tests|split(",")), requirements:$requirements,
    testRun:{testsRun:$testsRun, failures:0, errors:0},
    producedBy:"scripts/ci/public-gate-evidence.sh"}' > "$EVIDENCE_FILE.tmp.$$"
# Validate then RENAME. Truncating the real path up front would destroy previously valid evidence
# on a failure, or leave partial JSON where the gate expects a record.
jq empty "$EVIDENCE_FILE.tmp.$$" || { rm -f "$EVIDENCE_FILE.tmp.$$"; die "produced invalid JSON"; }
mv "$EVIDENCE_FILE.tmp.$$" "$EVIDENCE_FILE"

ok "wrote $EVIDENCE_FILE"
cat "$EVIDENCE_FILE"
