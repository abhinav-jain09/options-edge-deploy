#!/usr/bin/env bash
# The ONE canonical YAML corpus has two copies (this repository's scripts/ci/fixtures/zerodte/corpus — the source — and the Job's test
# resources in options-edge-processing) and ONE pinned digest literal in each runner (CORPUS_DIGEST in scripts/ci/zerodte_attestation.py and
# in the Job's YamlSubsetCorpusTest). Each copy is held to its own literal by its own runner; THIS gate holds the two LITERALS to each other
# (Codex 7b r4 / #921 r3): the peer's literal is read from the peer repository at a ref and must equal ours. A peer that cannot be read, or
# that carries no literal (or two), is a REFUSAL — never a pass by absence. Run in the provisioning pipeline's validation stage, before any
# cluster is touched, and by an operator before a coordinated release.
#
#   ZERODTE_PEER_SOURCE      a local checkout of the peer repository: its file is read instead of cloning (tests; an operator with a checkout)
#   ZERODTE_PEER_REPO        the peer repository to clone (default git@github.com:abhinav-jain09/options-edge-processing.git)
#   ZERODTE_PEER_REF         the peer ref to read: a 40-hex COMMIT (fetched by SHA — immutable, what a release records) or a branch (default main)
#   ZERODTE_PEER_REQUIRE_SHA true ⇒ a branch name is refused: the release pipeline names the exact processing commit (PROCESSING_PEER_SHA)
# What this proves: the two SOURCE revisions declare one corpus version. What it does not: that the digest-pinned service image was built
# from that revision — that is artifact provenance, recorded by the deployment, not judged here.
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
  # a local checkout is a developer's or a test's source: when the release requires an immutable commit, no override is honoured
  [ "${ZERODTE_PEER_REQUIRE_SHA:-false}" != true ] || { echo "FAIL: ZERODTE_PEER_REQUIRE_SHA=true — a local checkout (ZERODTE_PEER_SOURCE) is not an immutable commit of the peer repository; the release reads the peer at its 40-hex SHA only"; exit 1; }
  [ -f "$ZERODTE_PEER_SOURCE/$PEER_FILE" ] || { echo "FAIL: the peer checkout $ZERODTE_PEER_SOURCE has no $PEER_FILE"; exit 1; }
  PEER_TEXT="$(cat "$ZERODTE_PEER_SOURCE/$PEER_FILE")"
  WHERE="$ZERODTE_PEER_SOURCE/$PEER_FILE"
else
  T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
  case "$PEER_REF" in
    *[!0-9a-f]*|'') IS_SHA=false ;;
    *) [ "${#PEER_REF}" -eq 40 ] && IS_SHA=true || IS_SHA=false ;;
  esac
  if [ "${ZERODTE_PEER_REQUIRE_SHA:-false}" = true ] && [ "$IS_SHA" != true ]; then
    echo "FAIL: ZERODTE_PEER_REQUIRE_SHA=true — the peer ref must be the 40-hex processing COMMIT a release records, not '$PEER_REF' (a branch moves)"; exit 1
  fi
  if [ "$IS_SHA" = true ]; then
    git init -q "$T/peer" && git -C "$T/peer" fetch -q --depth 1 "$PEER_REPO" "$PEER_REF" 2>"$T/err" \
      || { echo "FAIL: the peer repository could not be read ($PEER_REPO at commit $PEER_REF): $(tr '\n' ' ' < "$T/err")"; exit 1; }
    PEER_HEAD="$(git -C "$T/peer" rev-parse FETCH_HEAD)"
    [ "$PEER_HEAD" = "$PEER_REF" ] || { echo "FAIL: the peer served commit $PEER_HEAD for $PEER_REF"; exit 1; }
  else
    git clone -q --depth 1 --no-checkout --branch "$PEER_REF" "$PEER_REPO" "$T/peer" 2>"$T/err" \
      || { echo "FAIL: the peer repository could not be read ($PEER_REPO at $PEER_REF): $(tr '\n' ' ' < "$T/err")"; exit 1; }
    PEER_HEAD="$(git -C "$T/peer" rev-parse HEAD)"
  fi
  PEER_TEXT="$(git -C "$T/peer" show "$PEER_HEAD:$PEER_FILE" 2>"$T/err")" \
    || { echo "FAIL: the peer repository has no $PEER_FILE at $PEER_REF: $(tr '\n' ' ' < "$T/err")"; exit 1; }
  WHERE="$PEER_REPO@$PEER_REF ($PEER_HEAD):$PEER_FILE"
fi
PEER="$(literal "$PEER_TEXT" "$WHERE")" || exit 1
echo "ours: $OURS ($OURS_FILE)"
echo "peer: $PEER ($WHERE)"
[ "$OURS" = "$PEER" ] || { echo "FAIL: the two repositories pin DIFFERENT corpus versions — the corpus and its literal must change in both (zerodte_attestation.py --corpus-manifest prints the value; copy the corpus verbatim)"; exit 1; }
echo "=== validate-zerodte-corpus-peer: OK (both repositories pin $OURS) ==="
