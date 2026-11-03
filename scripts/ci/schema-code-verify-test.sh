#!/usr/bin/env bash
# Negative controls for schema-code-verify.sh with a stub `mvn` and stub processing checkouts (docs/schema-governance R5b, R10, A-criteria):
# a requirements file that disagrees with the manifest, a failing module test, no requirements file at all, and a service the manifest does not bind must each fail.
set -euo pipefail
cd "$(dirname "$0")/../.."
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
cat > "$T/bin/mvn" <<'M'
#!/usr/bin/env bash
[ "${STUB_MVN:-ok}" = ok ] || { echo "stub mvn: tests FAILED" >&2; exit 1; }
exit 0
M
chmod +x "$T/bin/mvn"
HASH="$(python3 -c 'import json;print(next(e["schemaHash"] for e in json.load(open("schema-manifest/manifest.json"))["entries"] if e.get("subject")=="early-auction.verdicts.shadow-value" and e.get("version")==2))')"
mk() { rm -rf "$T/p"; mkdir -p "$T/p/$1/src/main/resources"; printf '%s' "$2" > "$T/p/$1/src/main/resources/schema-requirements.json"; }
GOOD='{"produces":[{"subject":"early-auction.verdicts.shadow-value","id":2811,"schemaHash":"'"$HASH"'"}],"consumes":[]}'
fail=0
: > "$T/empty-backlog"; export SCHEMA_R6_BACKLOG="$T/empty-backlog"   # the stub checkouts are not the real tree; the real ratchet has its own tests
expect() { local name="$1" want="$2" out rc=0; shift 2; out=$("$@" 2>&1) || rc=$?
  if [ "$want" = ok ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out" | tail -4; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure '$want' (rc=$rc)"; echo "$out" | tail -4; fail=1; }; fi; }
run() { PATH="$T/bin:$PATH" bash scripts/ci/schema-code-verify.sh "$T/p"; }
mk early-auction-verdict-service "$GOOD"; expect "requirements equal to the manifest and passing module tests" ok run
mk early-auction-verdict-service "${GOOD/$HASH/0000000000000000000000000000000000000000000000000000000000000000}"; expect "a requirements hash that differs from the manifest" "requirements disagree with the manifest" run
mk early-auction-verdict-service '{"produces":[{"subject":"early-auction.verdicts.shadow-value","id":3059,"schemaHash":"x"}],"consumes":[]}'; expect "an image that would need the dev-only v3 (wrong id and hash)" "requirements disagree" run
mk early-auction-verdict-service "$GOOD"; expect "module tests failing against the manifest" "tests failed against the manifest" env STUB_MVN=fail PATH="$T/bin:$PATH" bash scripts/ci/schema-code-verify.sh "$T/p"
rm -rf "$T/p"; mkdir -p "$T/p/some-service/src/main/resources"; expect "no requirements file anywhere is an error, not 'verified'" "no schema-requirements.json found" run
mk ghost-service "$GOOD"; expect "a service the manifest binds to no environment" "is bound to no environment" run
exit $fail
