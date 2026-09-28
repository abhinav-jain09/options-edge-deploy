#!/usr/bin/env bash
# ensure-zn-gex-topics.sh — zn-gex-service's two topics must EXIST with their declared shape
# BEFORE the service rolls out (same barrier as ensure-gamma-ladder-path-topics /
# ensure-multileg-topics / ensure-oi-anchor-topic).
#
# Why: apply-topics.sh lives in the MONOLITHIC deploy job, not the per-service fast path
# (Jenkinsfile.zn-gex-service), and reconciles ~150 topics across the whole estate — running it
# from a single-service pipeline would blow past this service's blast radius (the whole point of
# the standalone-service-deployment design; validate-services.sh enforces exactly this for k8s
# kinds, and Kafka topics deserve the same discipline). On a FIRST deploy neither topic exists,
# and the broker either auto-creates them with cluster defaults (wrong partition count — dev's
# broker runs num.partitions=32) or, with auto-create off, the idempotent producer fails and the
# pod crash-loops on startup. Declared in scripts/kafka/topics.env (both plain delete, default
# 1-day retention, NOT compacted, NOT reset-preserved — see the comment there):
#   rates.databento.zn.options.raw.v1         1 partition, cleanup.policy=delete, retention.ms=86400000
#   rates.databento.zn.options.gex.strike.v1  1 partition, cleanup.policy=delete, retention.ms=86400000
#
# CREATE if absent, then VERIFY every topic — including one just created — and fail closed. Never
# alters an existing topic. Every CLI call's status is checked explicitly: ensure() runs in an
# `if` context, where bash ignores `set -e` (same trap ensure-gamma-ladder-path-topics.sh's header
# comment warns about — nothing here may rely on errexit inside that context).
set -euo pipefail

RAW_TOPIC="${KAFKA_ZN_RAW_TOPIC:-rates.databento.zn.options.raw.v1}"
GEX_TOPIC="${KAFKA_ZN_GEX_TOPIC:-rates.databento.zn.options.gex.strike.v1}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset — refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"

kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/topic-config-parse.sh"   # extract(): the ONE tested config parser

echo "=== ensure-zn-gex-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

ensure() { # <topic>
  local topic="$1" out desc parts cfg policy retention
  if ! grep -qxF "$topic" <<<"$LIST"; then
    echo "creating '$topic' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=$RETENTION_MS)"
    if ! out=$(kt --create --topic "$topic" --partitions 1 --replication-factor "$RF" \
                  --config cleanup.policy=delete --config retention.ms="$RETENTION_MS" 2>&1); then
      echo "FAIL: could not create '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
  if ! desc=$(kt --describe --topic "$topic" 2>&1); then
    echo "FAIL: could not describe '$topic':"; printf '%s\n' "$desc"; return 1
  fi
  parts=$(awk '{for (i = 1; i < NF; i++) if ($i == "PartitionCount:") { print $(i + 1); exit }}' <<<"$desc")
  if [ "${parts:-}" != "1" ]; then
    echo "FAIL: '$topic' has PartitionCount=${parts:-unknown}, expected 1 (matches topics.env's declared shape)."
    return 1
  fi
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe 2>&1); then
    echo "FAIL: could not read the config of '$topic':"; printf '%s\n' "$cfg"; return 1
  fi
  policy=$(extract 'cleanup\.policy' "$cfg")
  # ${policy:-} (empty default), NOT ${policy:-delete}: an unparsable/absent extraction must FAIL
  # this check, not be treated as "delete" by default (that would let this claim "verified
  # delete" without actually verifying it — Codex round-2 review finding).
  if [ "${policy:-}" != "delete" ]; then
    echo "FAIL: '$topic' has cleanup.policy='${policy:-<unset/unparsable>}', expected 'delete' (plain, not compacted)."
    return 1
  fi
  # Codex round-3 NASA-grade finding: RETENTION_MS was declared and used at CREATE time but never
  # checked for an EXISTING topic — a pre-existing topic with the wrong retention (e.g. from a
  # manual fix, or a future topics.env change) passed this "verify" step unnoticed.
  retention=$(extract 'retention\.ms' "$cfg")
  if [ "${retention:-}" != "$RETENTION_MS" ]; then
    echo "FAIL: '$topic' has retention.ms='${retention:-<unset/unparsable>}', expected '$RETENTION_MS'."
    return 1
  fi
  echo "ok: '$topic' — partitions=1 cleanup.policy=delete retention.ms=$RETENTION_MS"
}

rc=0
if ! ensure "$RAW_TOPIC"; then rc=1; fi
if ! ensure "$GEX_TOPIC"; then rc=1; fi
exit $rc
