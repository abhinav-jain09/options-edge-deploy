#!/usr/bin/env bash
# assert-target-fresh.sh <tgt bootstrap> <tgt topic> <group> <src bootstrap> <src topic> <confluent bin>
# The earliest/newest sampling cannot see the middle of the log, so the middle is guaranteed by PROVENANCE instead: the target was created
# EMPTY (every end offset 0) and only this mirror's group writes it. A non-empty target is accepted only when this mirror's durable group
# already holds committed offsets on the source (a re-install of the same mirror); otherwise someone else wrote it and it must be wiped.
# NOT enforceable here: the brokers have no ACLs, so "no other writer" is an operator attestation (the preflight checks for the two known
# byte copiers separately).
set -euo pipefail
TB="${1:?}"; TT="${2:?}"; GROUP="${3:?}"; SB="${4:?}"; ST="${5:?}"; KBIN="${6:?}"
ends=$("${KAFKA_GET_OFFSETS_CMD:-$KBIN/kafka-get-offsets}" --bootstrap-server "$TB" --topic "$TT" 2>/dev/null) || { echo "FAIL: kafka-get-offsets failed for $TT on $TB"; exit 1; }
total=$(printf '%s\n' "$ends" | awk -F: -v t="$TT" '$1 == t && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ { s += $3; n++ } END { if (n == 0) exit 3; print s }') || { echo "FAIL: no end offsets for $TT"; exit 1; }
if [ "$total" -eq 0 ]; then echo "  target $TT is empty (created fresh): the mirror will be its only writer"; exit 0; fi
g=$("${KAFKA_CONSUMER_GROUPS_CMD:-$KBIN/kafka-consumer-groups}" --bootstrap-server "$SB" --describe --group "$GROUP" --offsets 2>/dev/null) || { echo "FAIL: cannot describe group $GROUP"; exit 1; }
n=$(printf '%s\n' "$g" | awk -v g="$GROUP" -v t="$ST" '$1 == g && $2 == t && $4 ~ /^[0-9]+$/ { n++ } END { print n+0 }')
[ "$n" -ge 1 ] || { echo "FAIL: target $TT holds $total record(s) but group $GROUP has never committed on the source: not written by this mirror. Wipe and recreate the target topic under its own name."; exit 1; }
echo "  target $TT holds $total record(s); group $GROUP has committed offsets, so this is a re-install of the same mirror"
