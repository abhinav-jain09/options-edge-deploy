#!/usr/bin/env bash
# Regenerate expected.tsv from the REAL contract classes. Run this when the contracts' CalendarArtifact or
# CanonicalBytes change, or when a vector is added; commit the result. Needs a JDK (21) and, in ~/.m2, the
# options-edge-contracts jar named by CONTRACTS_VERSION plus jackson databind/core/annotations/datatype-jsr310
# at JACKSON_VERSION (the versions the vol-premium service builds with).
#   CONTRACTS_VERSION=0.2.0-land-c JACKSON_VERSION=2.19.2 scripts/ci/fixtures/vol-premium-calendar/regenerate-expected.sh
set -euo pipefail
cd "$(dirname "$0")"
CONTRACTS_VERSION="${CONTRACTS_VERSION:-0.2.0-land-c}"
JACKSON_VERSION="${JACKSON_VERSION:-2.19.2}"
M2="${M2:-$HOME/.m2/repository}"
CP="$M2/com/optionsedge/options-edge-contracts/$CONTRACTS_VERSION/options-edge-contracts-$CONTRACTS_VERSION.jar"
for j in core/jackson-databind core/jackson-core core/jackson-annotations datatype/jackson-datatype-jsr310; do
  CP="$CP:$M2/com/fasterxml/jackson/$j/$JACKSON_VERSION/$(basename "$j")-$JACKSON_VERSION.jar"
done
IFS=: read -ra parts <<< "$CP"; for f in "${parts[@]}"; do [ -f "$f" ] || { echo "FATAL: missing $f" >&2; exit 1; }; done
OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' EXIT
javac -cp "$CP" -d "$OUT" CalendarVectors.java
java -cp "$CP:$OUT" CalendarVectors vectors > expected.tsv
sed -i.bak "1s|\$| contracts $CONTRACTS_VERSION, jackson $JACKSON_VERSION.|" expected.tsv && rm -f expected.tsv.bak
echo "wrote expected.tsv ($(grep -vc '^#' expected.tsv) vectors)"
