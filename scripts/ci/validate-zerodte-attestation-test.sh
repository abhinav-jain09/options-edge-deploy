#!/usr/bin/env bash
# The attestation / provisioning validators REFUSE every violation they claim to, and ACCEPT the lawful change — each case asserts the
# exit status AND the reason, so a refusal for another reason is a failure too. Fixtures are built here from the shipped files (the
# chain hashes through the validator's own entry-hash command, which the golden vectors hold to Java).
set -euo pipefail
cd "$(dirname "$0")/../.."
Z="python3 scripts/ci/zerodte_attestation.py"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
pass=0; fail=0
expect() { # expect <name> <want_rc> <want substring> -- <command...>
  local name="$1" want_rc="$2" want="$3"; shift 3; [ "$1" = "--" ] && shift
  local out rc
  out="$("$@" 2>&1)" && rc=0 || rc=$?
  if [ "$rc" = "$want_rc" ] && printf '%s' "$out" | grep -qF -- "$want"; then pass=$((pass+1)); echo "  ok   $name"; else fail=$((fail+1)); echo "  FAIL $name: rc=$rc want $want_rc; want [$want]"; printf '%s\n' "$out" | tail -3 | sed 's/^/       | /'; fi
}
L1=6b2c7c1a-5d3e-4a8f-9b41-2f0d7e9c4a10; L2=9e4f1d6b-7a2c-4c35-8d0e-5b1a3f8c2e77
ZERO="$(printf '0%.0s' $(seq 64))"
ID1=a1b2c3d4e5f60718293a4b5c6d7e8f90; ID2=0f1e2d3c4b5a69788796a5b4c3d2e1f0
H1="$($Z entry-hash SPX $L1 $ID1 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$ZERO")"
H2="$($Z entry-hash SPX $L2 $ID2 cluster-prod-Z 2026-10-04T13:30:00Z abhinav.jain "$H1")"
lineages() { printf 'lineages:\n  - id: %s\n    name: dev\n    parent: null\n    approvedBy: Abhinav Jain\n    date: "2026-10-03"\n  - id: %s\n    name: production\n    parent: null\n    approvedBy: Abhinav Jain\n    date: "2026-10-03"\n' "$L1" "$L2"; }
entry() { printf '  - symbol: %s\n    environmentLineageId: %s\n    ledgerTopicId: "%s"\n    clusterId: "%s"\n    createdAt: "%s"\n    operator: %s\n    prevEntryHash: "%s"\n' "$@"; }
E1="$(entry SPX $L1 $ID1 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$ZERO")"
E2="$(entry SPX $L2 $ID2 cluster-prod-Z 2026-10-04T13:30:00Z abhinav.jain "$H1")"
{ lineages; printf 'entries: []\n'; } > "$T/base0.yaml"
{ lineages; printf 'entries:\n%s\n' "$E1"; } > "$T/base1.yaml"
{ lineages; printf 'entries:\n%s\n%s\n' "$E1" "$E2"; } > "$T/two.yaml"
echo "--- the chain, one-shot, lineages ---"
expect "golden vectors agree with Java"            0 "golden vectors agree" -- $Z --vectors
expect "empty entries verify"                      0 "0 entries, chain intact" -- $Z verify "$T/base0.yaml"
expect "a chain of two verifies"                   0 "2 entries, chain intact, tail=$H2" -- $Z verify "$T/two.yaml"
expect "tail is the last entry's hash"             0 "$H2" -- $Z tail "$T/two.yaml"
{ lineages; printf 'entries:\n%s\n' "$(entry SPX $L1 $ID1 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$H1")"; } > "$T/bad.yaml"
expect "first link not zero"                       1 "entries[0].prevEntryHash breaks the chain" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n%s\n' "$(entry SPX $L1 $ID1 cluster-dev-A 2026-10-03T12:00:00Z someone.else "$ZERO")" "$E2"; } > "$T/bad.yaml"
expect "altered predecessor"                       1 "entries[1].prevEntryHash breaks the chain" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n' "$E2"; } > "$T/bad.yaml"
expect "removed predecessor"                       1 "entries[0].prevEntryHash breaks the chain" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n%s\n' "$E1" "$(entry SPX $L1 $ID2 cluster-dev-A 2026-10-05T12:00:00Z abhinav.jain "$H1")"; } > "$T/bad.yaml"
expect "one-shot: a second entry for (SPX, dev)"   1 "SECOND attestation for (SPX" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n' "$(entry SPX 11111111-2222-4333-8444-555555555555 $ID1 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$ZERO")"; } > "$T/bad.yaml"
expect "unknown lineage"                           1 "names an unknown lineage" -- $Z verify "$T/bad.yaml"
{ lineages | sed "s/    parent: null/    parent: 11111111-2222-4333-8444-555555555555/"; printf 'entries: []\n'; } > "$T/bad.yaml"
expect "clone without a known parent"              1 "names a KNOWN parent" -- $Z verify "$T/bad.yaml"
{ lineages | python3 -c "import sys; print(sys.stdin.read().replace('    parent: null', '    parent: $L1', 1), end='')"; printf 'entries: []\n'; } > "$T/bad.yaml"
expect "self-parent"                               1 "not its own parent" -- $Z verify "$T/bad.yaml"
{ lineages; lineages | tail -5; printf 'entries: []\n'; } > "$T/bad.yaml"
expect "duplicate lineage id"                      1 "a lineage id occurs once" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n' "$(entry SPX $L1 $ID1 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$ZERO" | sed 's/prevEntryHash: "\(.*\)"/prevEntryHash: \1/')"; } > "$T/bad.yaml"
expect "unquoted prevEntryHash"                    1 "prevEntryHash is a QUOTED string" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n' "$(printf '%s' "$E1" | sed 's/ledgerTopicId: "\(.*\)"/ledgerTopicId: \1/')"; } > "$T/bad.yaml"
expect "unquoted ledgerTopicId"                    1 "ledgerTopicId is a QUOTED string" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries:\n%s\n    extra: 1\n' "$E1"; } > "$T/bad.yaml"
expect "unknown entry key"                         1 "unknown key: extra" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries: !!seq []\n'; } > "$T/bad.yaml"
expect "a tag"                                     1 "not accepted" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries: &e []\n'; } > "$T/bad.yaml"
expect "an anchor"                                 1 "not accepted" -- $Z verify "$T/bad.yaml"
{ lineages; printf 'entries: []\nentries: []\n'; } > "$T/bad.yaml"
expect "a duplicate key"                           1 "duplicate key entries" -- $Z verify "$T/bad.yaml"
echo "--- append-only against a base ---"
expect "append of a valid entry over base0"        0 "append-only against $T/base0.yaml: OK" -- $Z verify "$T/base1.yaml" --base "$T/base0.yaml"
expect "append of a second entry over base1"       0 "append-only against $T/base1.yaml: OK" -- $Z verify "$T/two.yaml" --base "$T/base1.yaml"
expect "removal of an entry vs base"               1 "an entry of the base version was removed or altered" -- $Z verify "$T/base0.yaml" --base "$T/base1.yaml"
{ lineages; printf 'entries:\n%s\n' "$(entry SPX $L1 $ID2 cluster-dev-A 2026-10-03T12:00:00Z abhinav.jain "$ZERO")"; } > "$T/alt.yaml"
expect "alteration of an entry vs base"            1 "an entry of the base version was removed or altered" -- $Z verify "$T/alt.yaml" --base "$T/base1.yaml"
{ lineages | head -6; printf 'entries: []\n'; } > "$T/fewer.yaml"
expect "removal of a lineage vs base"              1 "a lineage of the base version was removed or altered" -- $Z verify "$T/fewer.yaml" --base "$T/base0.yaml"
{ lineages | sed 's/approvedBy: Abhinav Jain/approvedBy: Someone Else/'; printf 'entries: []\n'; } > "$T/relabel.yaml"
expect "alteration of a lineage vs base"           1 "a lineage of the base version was removed or altered" -- $Z verify "$T/relabel.yaml" --base "$T/base0.yaml"
{ lineages; printf '  - id: 22222222-3333-4444-8555-666666666666\n    name: dev-clone\n    parent: %s\n    approvedBy: Abhinav Jain\n    date: "2026-10-04"\nentries: []\n' "$L1"; } > "$T/clone.yaml"
expect "a new cloned lineage with a known parent" 0 "append-only against $T/base0.yaml: OK" -- $Z verify "$T/clone.yaml" --base "$T/base0.yaml"
echo "--- the provisioning declaration ---"
P=deploy/zerodte/provisioning/dev.yaml
expect "the shipped dev declaration"               0 '"generation": 1' -- $Z provisioning "$P"
sed 's/^eraId: 1$/eraId: 0/' "$P" > "$T/p.yaml";                                   expect "eraId 0"                      1 "eraId is in [1" -- $Z provisioning "$T/p.yaml"
sed 's/^migration: false$/migration: false\npreviousGeneration: 0/' "$P" > "$T/p.yaml"; expect "previousGeneration without migration" 1 "previousGeneration" -- $Z provisioning "$T/p.yaml"
sed 's/^generation: 1$/generation: 2/; s/^migration: false$/migration: true/' "$P" > "$T/p.yaml"; expect "migration without predecessor" 1 "present exactly when migration is true" -- $Z provisioning "$T/p.yaml"
sed 's/^recreatedTopics: \[\]$/recreatedTopics:\n  - other.topic/' "$P" > "$T/p.yaml"; expect "recreated not in topology"  1 "a recreated topic is a topic of THIS generation" -- $Z provisioning "$T/p.yaml"
python3 -c "import sys; print(open('$P').read().replace('    partitions: 1', '    partitions: 2', 1), end='')" > "$T/p.yaml"; expect "FRAMES with two partitions"   1 "FRAMES has exactly one partition" -- $Z provisioning "$T/p.yaml"
sed 's/    retentionMs: 86400000/    retentionMs: 3600000/' "$P" > "$T/p.yaml";      expect "PULSE retention not one day"  1 "PULSE retention is exactly one day" -- $Z provisioning "$T/p.yaml"
sed 's/^symbol: SPX$/symbol: spx/' "$P" > "$T/p.yaml";                               expect "lower-case symbol"            1 "symbol is not in its domain" -- $Z provisioning "$T/p.yaml"
sed 's/^operator: abhinav.jain$/operator: abhinav.jain\nextra: 1/' "$P" > "$T/p.yaml"; expect "unknown key"                1 "unknown key: extra" -- $Z provisioning "$T/p.yaml"
sed 's/^generation: 1$/generation: "1"/' "$P" > "$T/p.yaml";                         expect "a quoted integer"             1 "generation is an integer" -- $Z provisioning "$T/p.yaml"
sed 's/    dependencyMode: EMBEDDED/    dependencyMode: COMPACTED/' "$P" > "$T/p.yaml"; expect "unknown dependency mode"  1 "dependencyMode is one of" -- $Z provisioning "$T/p.yaml"
echo "--- a base this checkout does not hold is a REFUSAL, never a shape-only pass ---"
expect "missing base ref"                          1 "is not a known ref in this checkout" -- bash scripts/ci/validate-zerodte-attestation.sh --base refs/no/such/base
echo "--- the shipped files through the repository validators ---"
# The shipped attestation carries the UNAPPROVED marker until the OWNER writes their name (the owner's edit alone). Until then BOTH validators
# refuse it on EXACTLY that marker (a refusal for any other reason is a defect); after the owner's edit both pass. The case states which.
if grep -q '^    approvedBy: UNAPPROVED$' deploy/zerodte/virgin-attestation.yaml; then
  expect "validate-zerodte-provisioning (shipped, UNAPPROVED: refused on the marker only)" 1 "carries the UNAPPROVED marker" -- bash scripts/ci/validate-zerodte-provisioning.sh
  expect "validate-zerodte-attestation (shipped, UNAPPROVED: refused on the marker only)"  1 "carries the UNAPPROVED marker" -- bash scripts/ci/validate-zerodte-attestation.sh --base HEAD
  sed 's/^    approvedBy: UNAPPROVED$/    approvedBy: Test Owner/' deploy/zerodte/virgin-attestation.yaml > "$T/approved.yaml"
  expect "the shipped attestation, approved, verifies" 0 "chain intact" -- $Z verify "$T/approved.yaml"
else
  expect "validate-zerodte-provisioning (shipped, approved)" 0 "validate-zerodte-provisioning: OK" -- bash scripts/ci/validate-zerodte-provisioning.sh
  expect "validate-zerodte-attestation (shipped, approved)"  0 "validate-zerodte-attestation: OK" -- bash scripts/ci/validate-zerodte-attestation.sh --base HEAD
fi
echo "zerodte attestation/provisioning validators: $pass ok, $fail failed"
[ "$fail" -eq 0 ] && { echo "=== validate-zerodte-attestation-test: OK ==="; exit 0; }
echo "=== validate-zerodte-attestation-test: FAILED ==="; exit 1
