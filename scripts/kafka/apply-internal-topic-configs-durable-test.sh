#!/usr/bin/env bash
# The DURABLE_CHANGELOG_PATTERNS routing in apply-internal-topic-configs.sh: which internal topics
# get the compact-only policy. A glob that is too wide silently gives an unrelated app permanent
# retention; one that is too narrow lets the 10 h delete policy empty a ledger (2026-10-02).
set -euo pipefail
cd "$(dirname "$0")/../.."
# Source ONLY the pattern list and the predicate: cut them out of the script so no Kafka CLI runs.
eval "$(sed -n '/^DURABLE_CHANGELOG_PATTERNS=(/,/^)/p' scripts/kafka/apply-internal-topic-configs.sh)"
eval "$(sed -n '/^is_durable_changelog()/,/^}/p' scripts/kafka/apply-internal-topic-configs.sh)"
pass=0; fail=0
expect() { # <durable|not> <topic>
  if is_durable_changelog "$2"; then got=durable; else got=not; fi
  if [ "$got" = "$1" ]; then pass=$((pass+1)); echo "ok   $1  $2"; else fail=$((fail+1)); echo "FAIL expected $1 got $got  $2"; fi
}
expect durable oi-next-publication-dev-oi-nextpub-ledger-changelog
expect durable oi-next-publication-prod-oi-nextpub-ledger-changelog
expect durable oi-next-publication-es4-oi-nextpub-ledger-changelog
expect durable option-price-behavior-dev-opb-by-strike-aggregate-inc-changelog
expect not     oi-next-publication-dev-session-by-symbol-repartition
expect not     unrelated-dev-oi-nextpub-ledger-changelog
expect not     oi-next-publication-dev-oi-nextpub-ledger-changelog-extra
expect not     databento-gex-dev-some-store-changelog
expect not     oi-shadow-dev-oi-shadow-state-changelog
echo "apply-internal-topic-configs-durable-test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
