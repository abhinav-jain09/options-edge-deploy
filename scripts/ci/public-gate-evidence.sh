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
# WHAT THE EVIDENCE HONESTLY ASSERTS, and what it does not:
#
#   asserts     the source revision R passed the PGL-050/051/052 tests, and at the moment of writing
#               the tag T resolved to digest D
#   does NOT    prove by itself that D was built FROM R — the image carries no revision label today
#
# That gap is closed by WHERE this runs, not by what it writes: the caller builds the image from R
# and calls this immediately afterwards, in the same pipeline run, so the tie between R and D is
# first-hand. The digest is re-resolved after the tests and required to be unchanged, so a
# concurrent push to the same mutable tag cannot silently swap the artifact under the attestation.
#
# The stronger fix is to stamp `org.opencontainers.image.revision` into the image at build time,
# after which the R->D tie is verifiable by anyone, forever, from the registry alone. That belongs
# in the application repo's Dockerfile and its build Jenkinsfile, and is deliberately NOT bundled
# here: that Jenkinsfile builds every service image on the platform.
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
TESTS="PublicBoardUpstreamRequestTest,PublicSurfaceIsolationTest"

die() { echo "EVIDENCE FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)           REPO="$2"; shift 2 ;;
    --image)          IMAGE="$2"; shift 2 ;;
    --revision)       REVISION="$2"; shift 2 ;;
    --ci-build-url)   CI_BUILD_URL="$2"; shift 2 ;;
    --out-dir)        OUT_DIR="$2"; shift 2 ;;
    --expect-digest)  EXPECT_DIGEST="$2"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# NOTE: the Jenkins agent is macOS, whose /bin/bash is 3.2.57 — `${v,,}` is a bash 4 construct and
# fails there with "bad substitution" at RUNTIME while still parsing cleanly. Lowercased with tr.
for v in REPO IMAGE REVISION CI_BUILD_URL OUT_DIR; do
  if [ -z "${!v}" ]; then
    die "--$(printf '%s' "$v" | tr '[:upper:]_' '[:lower:]-') is required"
  fi
done
command -v jq >/dev/null || die "jq is required"

echo "==> public gate evidence (PGL-050/051/052)"
echo "  repo:     $REPO"
echo "  image:    $IMAGE"
echo "  revision: $REVISION"

# ---------------------------------------------------------------- 1. the revision must be REAL
# `-d .git` is WRONG for a git worktree, where .git is a FILE pointing at the real git dir. Ask git.
git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "$REPO is not a git checkout"
HEAD_SHA="$(git -C "$REPO" rev-parse HEAD)"
[[ "$HEAD_SHA" == "$REVISION" ]] \
  || die "the checkout is at $HEAD_SHA but the evidence would claim $REVISION.
       The tests run against the working tree, so the revision recorded must be the revision tested."
git -C "$REPO" diff --quiet && git -C "$REPO" diff --cached --quiet \
  || die "the checkout at $REPO has uncommitted changes. Evidence must name a revision anyone else
       can check out and re-run; a dirty tree is not that revision."
ok "checkout is clean and at $REVISION"

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

# ---------------------------------------------------------------- 3. RUN the tests
# No result is accepted as input. If this fails, the script exits and no evidence exists.
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
for t in $(printf '%s' "$TESTS" | tr ',' ' '); do
  report="$(ls "$REPO"/target/surefire-reports/*."$t".txt 2>/dev/null | head -1)"
  [ -n "$report" ] || die "$t produced no surefire report. Maven exited 0, but nothing proves this
       test ran — and an attestation from a run that executed nothing is exactly the 'wish' the gate
       refuses."
  grep -qE "Tests run: [1-9][0-9]*, Failures: 0, Errors: 0" "$report" \
    || die "$t did not report a clean, non-empty run:
$(grep -E 'Tests run:' "$report" | head -1)"
  ok "$t $(grep -oE 'Tests run: [0-9]+' "$report" | head -1)"
done

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
  '{imageDigest:$imageDigest, image:$image, sourceRevision:$sourceRevision, ciBuildUrl:$ciBuildUrl,
    producedAt:$producedAt, tests:($tests|split(",")), requirements:$requirements,
    producedBy:"scripts/ci/public-gate-evidence.sh"}' > "$EVIDENCE_FILE"

ok "wrote $EVIDENCE_FILE"
cat "$EVIDENCE_FILE"
