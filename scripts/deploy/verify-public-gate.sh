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
# WHAT IT DOES NOT DO
#   It cannot re-run the tests; those live in the application repo. It checks that someone ran them
#   against this digest and recorded it. That is a deliberately modest claim, and it is why the
#   evidence file must be written by CI from a real test run rather than by hand.
#
# USAGE
#   verify-public-gate.sh [--manifest k8s/bleedingoptions/gamma-lab-deployment.yaml]
#                         [--evidence evidence/public-gate-<digest>.json]
set -euo pipefail

MANIFEST="k8s/bleedingoptions/gamma-lab-deployment.yaml"
EVIDENCE_DIR="${PUBLIC_GATE_EVIDENCE_DIR:-evidence}"
REQUIRED_IDS=(PGL-050 PGL-051 PGL-052)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)  MANIFEST="$2"; shift 2 ;;
    --evidence)  EVIDENCE_FILE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

die() { echo "GATE FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

[[ -f "$MANIFEST" ]] || die "manifest not found: $MANIFEST"
command -v jq >/dev/null || die "jq is required"

# `replicas:` under the Deployment. Deliberately a plain read of the file rather than a cluster query:
# this runs BEFORE the apply, on what is about to be applied.
REPLICAS="$(awk '/^kind: Deployment$/{d=1} d && /^  replicas:/{print $2; exit}' "$MANIFEST")"
[[ -n "$REPLICAS" ]] || die "could not read replicas from $MANIFEST"

echo "==> public Gamma Lab gate (PGL-072)"
echo "  manifest: $MANIFEST"
echo "  replicas: $REPLICAS"

if [[ "$REPLICAS" == "0" ]]; then
  # The safe state. Nothing to prove, because nothing will run.
  ok "replicas=0 — the public workload is not scheduled; gate satisfied trivially."
  exit 0
fi

IMAGE="$(awk '/^          image: /{print $2; exit}' "$MANIFEST")"
[[ -n "$IMAGE" ]] || die "could not read the image from $MANIFEST"
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
