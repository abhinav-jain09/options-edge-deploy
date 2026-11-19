#!/usr/bin/env bash
# Read-only ES4 preflight for the Databento-to-Rithmic shadow publisher.
#
# The publisher intentionally writes separate Kafka topics but uses the two
# released Rithmic Avro contracts.  Avro carries its schema id in each record,
# so it must prove those canonical subjects exist and have the expected names
# before a pod may start.  It never creates or changes a registry subject.
set -euo pipefail

URL="${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required}"
for expected in 'es.rithmic.depth-value:RithmicDepthFrame' 'es.rithmic.mbo-value:RithmicMboEvent'; do
  subject="${expected%%:*}"; record="${expected#*:}"
  response="$(curl -fsS --max-time 10 "${URL}/subjects/${subject}/versions/latest")" \
    || { echo "FAIL: canonical Rithmic subject ${subject} is unavailable at ${URL}" >&2; exit 1; }
  schema="$(printf '%s' "$response" | jq -r '.schema // empty')"
  [ -n "$schema" ] || { echo "FAIL: ${subject} returned no schema" >&2; exit 1; }
  name="$(printf '%s' "$schema" | jq -r '.name // empty')"
  namespace="$(printf '%s' "$schema" | jq -r '.namespace // empty')"
  [ "$name" = "$record" ] && [ "$namespace" = 'com.optionsedge.contracts.rithmic' ] \
    || { echo "FAIL: ${subject} is not ${namespace}.${name}; expected com.optionsedge.contracts.rithmic.${record}" >&2; exit 1; }
  echo "schema preflight OK: ${subject} -> ${namespace}.${name}"
done
