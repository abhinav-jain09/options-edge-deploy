#!/usr/bin/env bash
# The ONE canonical YAML corpus has two copies (this repository's scripts/ci/fixtures/zerodte/corpus — the source — and the Job's test
# resources in options-edge-processing) and ONE pinned digest literal in each runner (CORPUS_DIGEST in scripts/ci/zerodte_attestation.py and
# in the Job's YamlSubsetCorpusTest). Each copy is held to its own literal by its own runner; THIS gate holds the two LITERALS to each other
# (Codex 7b r4 / #921 r3): the peer's literal is read from the peer repository at a ref and must equal ours. A peer that cannot be read, or
# that carries no literal (or two), is a REFUSAL — never a pass by absence. Run in the provisioning pipeline's validation stage, before any
# cluster is touched, and by an operator before a coordinated release.
#
#   ZERODTE_PEER_SOURCE   a local checkout of the peer repository: its file is read instead of cloning (tests; an operator with a checkout)
#   ZERODTE_PEER_REPO     the peer repository to clone (default git@github.com:abhinav-jain09/options-edge-processing.git)
#   ZERODTE_PEER_REF      the peer branch to read (default main)
set -euo pipefail
cd "$(dirname "$0")/../.."
OURS_FILE="scripts/ci/zerodte_attestation.py"
PEER_FILE="vix-option-inteligence-service/src/test/java/com/optionsedge/processing/zerodte/provisioning/YamlSubsetCorpusTest.java"
PEER_REPO="${ZERODTE_PEER_REPO:-git@github.com:abhinav-jain09/options-edge-processing.git}"
PEER_REF="${ZERODTE_PEER_REF:-main}"
literal() { # literal <text> <what> → the ONE CORPUS_DIGEST = "<64 hex>" literal, or a refusal
  local n hits
  hits="$(printf '%s\n' "$1" | grep -oE 'CORPUS_DIGEST = "[0-9a-f]{64}"' || true)"
  n="$(printf '%s\n' "$hits" | grep -c . || true)"
  [ "$n" = 1 ] || { echo "FAIL: $2 carries $n CORPUS_DIGEST literal(s); exactly one is required" >&2; return 1; }
  printf '%s' "$hits" | sed 's/.*"\([0-9a-f]*\)".*/\1/'
}
OURS="$(literal "$(cat "$OURS_FILE")" "$OURS_FILE")" || exit 1
if [ -n "${ZERODTE_PEER_SOURCE:-}" ]; then
  [ -f "$ZERODTE_PEER_SOURCE/$PEER_FILE" ] || { echo "FAIL: the peer checkout $ZERODTE_PEER_SOURCE has no $PEER_FILE"; exit 1; }
  PEER_TEXT="$(cat "$ZERODTE_PEER_SOURCE/$PEER_FILE")"
  WHERE="$ZERODTE_PEER_SOURCE/$PEER_FILE"
else
  T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
  git clone -q --depth 1 --no-checkout --branch "$PEER_REF" "$PEER_REPO" "$T/peer" 2>"$T/err" \
    || { echo "FAIL: the peer repository could not be read ($PEER_REPO at $PEER_REF): $(tr '\n' ' ' < "$T/err")"; exit 1; }
  PEER_TEXT="$(git -C "$T/peer" show "HEAD:$PEER_FILE" 2>"$T/err")" \
    || { echo "FAIL: the peer repository has no $PEER_FILE at $PEER_REF: $(tr '\n' ' ' < "$T/err")"; exit 1; }
  WHERE="$PEER_REPO@$PEER_REF ($(git -C "$T/peer" rev-parse HEAD)):$PEER_FILE"
fi
PEER="$(literal "$PEER_TEXT" "$WHERE")" || exit 1
echo "ours: $OURS ($OURS_FILE)"
echo "peer: $PEER ($WHERE)"
[ "$OURS" = "$PEER" ] || { echo "FAIL: the two repositories pin DIFFERENT corpus versions — the corpus and its literal must change in both (zerodte_attestation.py --corpus-manifest prints the value; copy the corpus verbatim)"; exit 1; }
echo "=== validate-zerodte-corpus-peer: OK (both repositories pin $OURS) ==="
