#!/usr/bin/env bash
# verify-embedded-contracts.sh <fat-jar> <expected-sha256>
#
# Artifact identity, by content: the contracts jar the build installed and bound (its sha256 recorded
# right after the guarded install) must be THE contracts jar packaged inside the Spring Boot fat jar
# about to become the image — not whatever a shared Maven repository held by the time `mvn package`
# ran. The fat jar carries the dependency verbatim under BOOT-INF/lib/, so it is extracted and hashed;
# exactly one contracts jar must be embedded, and it must hash to the recorded value. Refuses otherwise.
set -euo pipefail
jar="${1:?fat jar}"; expected="${2:?expected sha256}"
[ -f "$jar" ] || { echo "verify-embedded-contracts: no such jar: $jar" >&2; exit 1; }
printf '%s' "$expected" | grep -Eq '^[0-9a-f]{64}$' || { echo "verify-embedded-contracts: expected sha256 '$expected' is malformed" >&2; exit 1; }
entries="$(unzip -Z1 "$jar" | grep -E '^BOOT-INF/lib/options-edge-contracts-[^/]*\.jar$' || true)"
n="$(printf '%s\n' "$entries" | grep -c . || true)"
[ "$n" -eq 1 ] || { echo "verify-embedded-contracts: $jar embeds $n contracts jar(s) under BOOT-INF/lib (need exactly 1)" >&2; exit 1; }
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
unzip -p "$jar" "$entries" > "$tmp"
actual="$(bash "$(dirname "${BASH_SOURCE[0]}")/permitted-sha-guard-version.sh" "$tmp")"
if [ "$actual" != "$expected" ]; then
  echo "verify-embedded-contracts: $jar embeds $entries with sha256 $actual, but the bound contracts jar is $expected — the packaged artifact does not contain the permitted contracts" >&2
  exit 1
fi
echo "verify-embedded-contracts: $jar embeds $entries = $expected (bound contracts jar)"
