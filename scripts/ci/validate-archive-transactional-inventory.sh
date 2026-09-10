#!/usr/bin/env bash
# validate-archive-transactional-inventory.sh — the archive's transactional declarations must be
# COMPLETE and CONSISTENT, because the checkpoint rule differs between a declared topic and an
# undeclared one.
#
# WHY: oe-archive-kafka.sh checkpoints a DECLARED transactional topic at the greatest offset it
# actually captured, and every other topic at the high-water mark the offsets tool reported. On a
# topic written inside Kafka transactions the high-water mark is not a boundary any capture reached —
# an unresolved transaction can commit after the run, and the next run starts past it. That is a
# permanent, silent gap.
#
# Deploy Codex round 3 found dealer-ledger (prod AND dev), corridor-gauge, unified-sr, the calibration
# scorer and es.futures.auction all writing inside transactions and all missing from the list. Round 5
# then showed the list cannot simply be abolished: the archiver reads Kafka's text output, and on a
# topic whose values can contain a newline the offsets and the payload share one unframed stream, so a
# payload can forge a boundary. The rule is therefore applied where it is sound (declared JSON topics)
# and the LIST is what this gate protects.
#
# What is checked:
#   1. every declared topic exists in that environment's archive inventory (a typo silently declares
#      nothing at all);
#   2. every topic this repository KNOWS is written by an exactly-once producer, and is archived, is
#      declared for that environment;
#   3. the known-writer table below is kept next to the manifests it is derived from, and every entry
#      names the manifest that enables EOS, so it can be re-derived rather than trusted.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
ENVFILE=scripts/ops/archive/oe-topics.env
fails=0

# --- the writers this repository enables exactly-once, and where that is declared ------------------
# format: <env>|<topic>|<manifest or source that enables EOS>
KNOWN_EOS=$(cat <<'TABLE'
es4|es.futures.footprint.strike|es-footprint-strike: one Kafka transaction per bar (ES-FOOTPRINT-STRIKE-INTERACTION.md R6)
es4|es.futures.cvd.levels|es-cvd: TransactionalStatePublisher
es4|es.futures.auction|es-amt: EsAmtRuntime begins/commits a transaction per auction record
prod|option-price-behavior-by-option|k8s/services/option-price-behavior/overlays/production/manifest.yaml (OPB_EXACTLY_ONCE=true)
prod|option-price-behavior-by-strike|k8s/services/option-price-behavior/overlays/production/manifest.yaml (OPB_EXACTLY_ONCE=true)
prod|option-price-behavior-session|k8s/services/option-price-behavior/overlays/production/manifest.yaml (OPB_EXACTLY_ONCE=true)
prod|dealer-ledger-profile|k8s/services/dealer-ledger/overlays/production/manifest.yaml (EOS)
prod|dealer-ledger-state|k8s/services/dealer-ledger/overlays/production/manifest.yaml (EOS)
prod|dealer-ledger-signal-fired|k8s/services/dealer-ledger/overlays/production/manifest.yaml (EOS)
prod|dealer-ledger-outcome-scored|dealer-ledger-calibration ScorerConfig: EOS on by default
prod|corridor-gauge-state|corridor-gauge: CorridorGaugeStreams enables EOS unconditionally
prod|corridor-gauge-event-log|corridor-gauge: CorridorGaugeStreams enables EOS unconditionally
prod|options.spx.strike-sr.current|unified-sr: UnifiedSrSettings enables EOS unconditionally
prod|options.spx.strike-sr.history|unified-sr: UnifiedSrSettings enables EOS unconditionally
dev|dealer-ledger-profile|k8s/services/dealer-ledger/overlays/dev/manifest.yaml (EOS)
dev|dealer-ledger-state|k8s/services/dealer-ledger/overlays/dev/manifest.yaml (EOS)
dev|dealer-ledger-signal-fired|k8s/services/dealer-ledger/overlays/dev/manifest.yaml (EOS)
dev|dealer-ledger-outcome-scored|dealer-ledger-calibration ScorerConfig: EOS on by default
TABLE
)

inventory_for() {   # $1=env -> the topics that environment actually archives
  case "$1" in
    es4)  ( . "$ENVFILE"; printf '%s' "$OE_ES4_TOPICS" ) ;;
    prod) ( . "$ENVFILE"; printf '%s' "$OE_ALL_TOPICS_prod" ) ;;
    dev)  ( . "$ENVFILE"; printf '%s %s' "$DEALER_LEDGER_EVIDENCE" "$OE_ALL_TOPICS_prod" ) ;;
  esac
}
declared_for() {    # $1=env -> the topics declared transactional there
  ( . "$ENVFILE"; eval "printf '%s' \"\${OE_TRANSACTIONAL_TOPICS_$1:-}\"" )
}
contains() { case " $2 " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

for env in es4 prod dev; do
  inv="$(inventory_for "$env")"
  dec="$(declared_for "$env")"
  # 1. a declared topic that is not archived declares nothing — almost always a typo
  for t in $dec; do
    if ! contains "$t" "$inv"; then
      echo "FAIL: OE_TRANSACTIONAL_TOPICS_$env declares '$t', which is not in that environment's archive inventory" >&2
      fails=1
    fi
  done
  # 2. a known exactly-once writer that IS archived must be declared, or it silently keeps the
  #    high-water-mark checkpoint and can lose whatever commits after a capture
  while IFS='|' read -r e t why; do
    [ -n "${e:-}" ] || continue
    [ "$e" = "$env" ] || continue
    contains "$t" "$inv" || continue
    if ! contains "$t" "$dec"; then
      echo "FAIL: '$t' is archived on $env and is written exactly-once ($why) but is not in OE_TRANSACTIONAL_TOPICS_$env" >&2
      fails=1
    fi
  done <<< "$KNOWN_EOS"
done

# 3. the strike log is the reason this rule exists: assert it explicitly rather than relying on the
#    table being read
contains es.futures.footprint.strike "$(declared_for es4)" || {
  echo "FAIL: es.futures.footprint.strike must be declared transactional on es4 (ES-FOOTPRINT-STRIKE-INTERACTION.md R6)" >&2
  fails=1
}

# 4. the archiver must still gate on the declaration — a refactor that drops the gate would make the
#    payload-derived boundary universal again, which rounds 4 and 5 rejected
grep -q 'if is_transactional_topic "\$topic"; then' scripts/ops/archive/oe-archive-kafka.sh || {
  echo "FAIL: oe-archive-kafka.sh no longer gates the proved-boundary probe on is_transactional_topic" >&2
  fails=1
}
grep -q 'is_strict_topic "\$topic" || is_transactional_topic "\$topic"' scripts/ops/archive/oe-archive-kafka.sh || {
  echo "FAIL: oe-archive-kafka.sh no longer gates print.offset on the declaration" >&2
  fails=1
}

if [ "$fails" -ne 0 ]; then
  echo "=== validate-archive-transactional-inventory: FAILED ==="
  exit 1
fi
echo "checked $(declared_for es4 | wc -w | tr -d ' ') es4, $(declared_for prod | wc -w | tr -d ' ') prod and $(declared_for dev | wc -w | tr -d ' ') dev transactional declarations against the archive inventories"
echo "=== validate-archive-transactional-inventory: OK ==="
