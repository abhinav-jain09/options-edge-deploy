#!/usr/bin/env bash
# The cross-repository corpus gate (scripts/ci/validate-zerodte-corpus-peer.sh) through its cases: a peer checkout that pins the same literal
# passes; a different literal, a missing file, no literal, two literals are each refused by their own reason; and the GIT path (a clone of a
# throwaway repository at a ref) both passes and refuses — an unreadable repository is a refusal, never a pass by absence.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
PEER_FILE="vix-option-inteligence-service/src/test/java/com/optionsedge/processing/zerodte/provisioning/YamlSubsetCorpusTest.java"
OURS="$(grep -oE 'CORPUS_DIGEST = "[0-9a-f]{64}"' scripts/ci/zerodte_attestation.py | sed 's/.*"\([0-9a-f]*\)".*/\1/')"
OTHER="$(printf 'f%.0s' $(seq 64))"
peer() { mkdir -p "$1/$(dirname "$PEER_FILE")"; printf 'class YamlSubsetCorpusTest {\n    static final String CORPUS_DIGEST = "%s";\n}\n' "$2" > "$1/$PEER_FILE"; }
pass=0; fail=0
expect() { # expect <name> <want_rc> <want substring> [VAR=value ...]
  local name="$1" want_rc="$2" want="$3"; shift 3
  local out rc
  out="$(env "$@" bash scripts/ci/validate-zerodte-corpus-peer.sh 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name (rc=$rc)"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -3 | sed 's/^/       | /'; fi
}
peer "$T/same" "$OURS";   expect "a peer checkout pinning the same literal"  0 "OK (both repositories pin $OURS)" ZERODTE_PEER_SOURCE="$T/same"
peer "$T/other" "$OTHER"; expect "a peer checkout pinning another literal"   1 "pin DIFFERENT corpus versions" ZERODTE_PEER_SOURCE="$T/other"
mkdir -p "$T/empty";      expect "a peer checkout without the file"          1 "has no $PEER_FILE" ZERODTE_PEER_SOURCE="$T/empty"
peer "$T/none" "$OURS"; sed -i.bak 's/CORPUS_DIGEST/CORPUS_DIGEST_X/' "$T/none/$PEER_FILE"
expect "a peer file without the literal"                                      1 "carries 0 CORPUS_DIGEST literal(s)" ZERODTE_PEER_SOURCE="$T/none"
peer "$T/two" "$OURS"; printf '    static final String ALSO = "%s";\n' "CORPUS_DIGEST = \"$OTHER\"" >> "$T/two/$PEER_FILE"
expect "a peer file with two literals"                                        1 "carries 2 CORPUS_DIGEST literal(s)" ZERODTE_PEER_SOURCE="$T/two"
# the git path: a throwaway peer repository with a main branch pinning ours and a branch pinning another
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
peer "$T/repo" "$OURS"; git -C "$T/repo" init -q -b main && git -C "$T/repo" add -A && git -C "$T/repo" commit -q -m same
git -C "$T/repo" checkout -q -b drift && peer "$T/repo" "$OTHER" && git -C "$T/repo" commit -q -am drift && git -C "$T/repo" checkout -q main
expect "git: the peer at main pins the same literal"   0 "OK (both repositories pin $OURS)" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF=main
expect "git: the peer at a drifted branch"             1 "pin DIFFERENT corpus versions" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF=drift
expect "git: a ref that does not exist"                1 "the peer repository could not be read" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF=nope
expect "git: a repository that does not exist"         1 "the peer repository could not be read" ZERODTE_PEER_REPO="file://$T/nowhere" ZERODTE_PEER_REF=main
SAME_SHA="$(git -C "$T/repo" rev-parse main)"; DRIFT_SHA="$(git -C "$T/repo" rev-parse drift)"
git -C "$T/repo" config uploadpack.allowReachableSHA1InWant true   # GitHub serves reachable commits by SHA; a bare local repository needs this switch
expect "git: the exact commit (by SHA) pinning the same literal" 0 "OK (both repositories pin $OURS)" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF="$SAME_SHA" ZERODTE_PEER_REQUIRE_SHA=true
expect "git: the exact commit (by SHA) of the drift"   1 "pin DIFFERENT corpus versions" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF="$DRIFT_SHA" ZERODTE_PEER_REQUIRE_SHA=true
expect "git: a branch name when a SHA is required"     1 "must be the 40-hex processing COMMIT" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF=main ZERODTE_PEER_REQUIRE_SHA=true
expect "a local checkout when a SHA is required"       1 "a local checkout (ZERODTE_PEER_SOURCE) is not an immutable commit" ZERODTE_PEER_SOURCE="$T/same" ZERODTE_PEER_REQUIRE_SHA=true ZERODTE_PEER_REF="$SAME_SHA"
expect "git: a SHA that does not exist"                1 "the peer repository could not be read" ZERODTE_PEER_REPO="file://$T/repo" ZERODTE_PEER_REF="$(printf '0%.0s' $(seq 40))" ZERODTE_PEER_REQUIRE_SHA=true
echo "zerodte corpus peer gate: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== validate-zerodte-corpus-peer-test: OK ==="; exit 0; }
echo "=== validate-zerodte-corpus-peer-test: FAILED ==="; exit 1
