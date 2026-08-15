#!/usr/bin/env bash
# verify-public-gate.sh — refuse to run the public Gamma Lab without the evidence that makes it safe.
#
# WHY THIS EXISTS
# ---------------
# PGL-072 says the public web deployment stays at zero replicas until three requirements pass ON THE
# EXACT IMAGE DIGEST it will run:
#
#   PGL-050  the caller's bearer is stripped before the upstream request
#   PGL-051  a dedicated, validated, non-gateway upstream is used
#   PGL-052  a test has OBSERVED the real outbound request and found no credential on it
#
# The first two describe intent. Only the third checks — and the difference is not academic: with the
# Gate-1 image a board request still resolved through the feed gateway carrying the caller's token,
# and every build was green. A rule that depends on someone remembering it at the moment they are
# scaling something up is not a control, so this is that control.
#
# WHAT IT REFUSES
#   * replicas > 0 with no evidence file
#   * replicas > 0 with evidence for a DIFFERENT image digest than the manifest deploys
#   * evidence that does not record all three requirement ids as passing
#
# WHAT IT DOES NOT DO — READ THIS BEFORE TRUSTING IT
#   It cannot re-run the tests; those live in the application repo. It verifies that a CI-produced
#   record exists, names this exact digest, and reports all three ids as PASS.
#
#   That record is NOT cryptographically attested. Anyone who can write to the Jenkins agent's
#   filesystem can write a passing one. What this gate genuinely prevents is the realistic failure —
#   scaling up while nobody has checked, or while the evidence belongs to a different build — not a
#   determined insider. Two mitigations narrow it, and neither closes it:
#
#     * the evidence must live OUTSIDE this repository, so "commit a JSON file saying PASS" is not
#       the path of least resistance, and a passing gate cannot be introduced by a pull request;
#     * it must name the CI build that produced it and the source revision tested, so a human
#       reviewing a scale-up has something to check rather than a bare assertion.
#
#   Proper attestation — a signed provenance statement bound to the image digest, verified here — is
#   the real answer and is tracked as follow-up work. Until it exists, do not describe this gate as
#   proof that the tests ran; describe it as proof that someone recorded that they did.
#
# USAGE
#   verify-public-gate.sh [--overlay k8s/services/bleedingoptions-gamma-lab/overlays/production]
#                         [--evidence /path/outside/the/repo/public-gate-<digest>.json]
set -euo pipefail

# The RENDERED overlay, not the base file. An earlier version parsed the committed base with
# indentation-sensitive awk while Jenkins applied kustomize output — so an overlay patch or image
# transformer could change replicas or the image AFTER the check, which is exactly the class of edit
# this gate claims to prevent.
OVERLAY="k8s/services/bleedingoptions-gamma-lab/overlays/production"
DEPLOYMENT_NAME="bleedingoptions-gamma-lab"
EVIDENCE_DIR="${PUBLIC_GATE_EVIDENCE_DIR:-evidence}"
REQUIRED_IDS=(PGL-050 PGL-051 PGL-052)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --overlay)   OVERLAY="$2"; shift 2 ;;
    --evidence)  EVIDENCE_FILE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

die() { echo "GATE FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

[[ -d "$OVERLAY" ]] || die "overlay not found: $OVERLAY"
command -v jq >/dev/null || die "jq is required"
command -v yq >/dev/null || die "yq is required"

# Render exactly what will be applied, then select the Deployment STRUCTURALLY. A structural read
# cannot be fooled by indentation or by a field appearing in a comment.
RENDERED="$(mktemp)"
trap 'rm -f "$RENDERED"' EXIT
kubectl kustomize "$OVERLAY" >"$RENDERED" || die "could not render $OVERLAY"

REPLICAS="$(yq -r "select(.kind == \"Deployment\" and .metadata.name == \"$DEPLOYMENT_NAME\") | .spec.replicas" "$RENDERED")"
[[ -n "$REPLICAS" && "$REPLICAS" != "null" ]] \
  || die "could not read .spec.replicas for Deployment/$DEPLOYMENT_NAME from the rendered $OVERLAY"

echo "==> public Gamma Lab gate (PGL-072)"
echo "  overlay:  $OVERLAY (rendered)"
echo "  replicas: $REPLICAS"

if [[ "$REPLICAS" == "0" ]]; then
  # The safe state. Nothing to prove, because nothing will run.
  ok "replicas=0 — the public workload is not scheduled; gate satisfied trivially."
  exit 0
fi

IMAGE="$(yq -r "select(.kind == \"Deployment\" and .metadata.name == \"$DEPLOYMENT_NAME\") | .spec.template.spec.containers[0].image" "$RENDERED")"
[[ -n "$IMAGE" && "$IMAGE" != "null" ]] || die "could not read the image from the rendered $OVERLAY"
echo "  image:    $IMAGE"

# The digest is what the evidence must be tied to. A tag can be re-pushed; a digest cannot, which is
# the entire reason the requirement is written against one.
if [[ "$IMAGE" != *"@sha256:"* ]]; then
  die "the public deployment is scaled to $REPLICAS but its image is not digest-pinned ($IMAGE).
       A tag can be re-pushed under the same name, so evidence gathered against it proves nothing
       about what will actually run. Pin the image to @sha256:... and re-run."
fi
DIGEST="${IMAGE##*@}"

EVIDENCE_FILE="${EVIDENCE_FILE:-$EVIDENCE_DIR/public-gate-${DIGEST/:/-}.json}"
[[ -f "$EVIDENCE_FILE" ]] || die "the public deployment is scaled to $REPLICAS but there is no gate
       evidence for the digest it runs.
         expected: $EVIDENCE_FILE
       That evidence is produced by the application repo's CI from a real run of the PGL-050/051/052
       tests. Until it exists, this deployment must stay at replicas: 0 — with the current code path
       unverified, a board request may still carry the caller's bearer into the internal namespace."

jq empty "$EVIDENCE_FILE" 2>/dev/null || die "$EVIDENCE_FILE is not valid JSON"

# The evidence must NOT live in this repository. If it could, the way to satisfy this gate would be to
# add a file saying PASS — which is not evidence, it is a wish, and it would arrive through a pull
# request that looks like configuration.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
EVIDENCE_REAL="$(cd "$(dirname "$EVIDENCE_FILE")" && pwd -P)/$(basename "$EVIDENCE_FILE")"
if [[ "$EVIDENCE_REAL" == "$REPO_ROOT"/* ]]; then
  die "the gate evidence is inside this repository ($EVIDENCE_REAL).
       It must come from the CI workspace that ran the tests. Evidence that can be committed is
       evidence that can be written by anyone opening a pull request."
fi

# Provenance a human can actually check when reviewing a scale-up.
for field in ciBuildUrl sourceRevision; do
  value="$(jq -r --arg f "$field" '.[$f] // empty' "$EVIDENCE_FILE")"
  [[ -n "$value" ]] || die "$EVIDENCE_FILE does not record '$field'. Without the build that produced
       it and the revision it tested, this file asserts a result with nothing behind it."
  ok "$field: $value"
done

EV_DIGEST="$(jq -r '.imageDigest // empty' "$EVIDENCE_FILE")"
[[ -n "$EV_DIGEST" ]] || die "$EVIDENCE_FILE does not record an imageDigest"
[[ "$EV_DIGEST" == "$DIGEST" ]] || die "the evidence is for a DIFFERENT image.
         manifest: $DIGEST
         evidence: $EV_DIGEST
       Evidence from another build says nothing about this one — that is the entire point of pinning."
ok "evidence matches the deployed digest"

for id in "${REQUIRED_IDS[@]}"; do
  status="$(jq -r --arg id "$id" '.requirements[$id] // "MISSING"' "$EVIDENCE_FILE")"
  [[ "$status" == "PASS" ]] \
    || die "$id is '$status' in $EVIDENCE_FILE. All of ${REQUIRED_IDS[*]} must be PASS before the
       public workload may carry a replica."
  ok "$id PASS"
done

echo "GATE PASS: the public workload may run on $DIGEST."
