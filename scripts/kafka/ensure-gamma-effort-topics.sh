#!/usr/bin/env bash
# ensure-gamma-effort-topics.sh - gamma-effort-service's TWO topics must be right BEFORE the service publishes (the first-deploy barrier, as
# ensure-gamma-zone-topics.sh). DEV ONLY: the service renders on dev alone (services.yaml envs [dev]); refuses to run unless ENVIRONMENT is exactly "dev".
#
#   NO BROKER-WIDE CHANGE. The shapes are the ones scripts/kafka/topics.env declares (one partition each, exact):
#   options.spx.gamma-effort.current  1 partition, compact, retention.ms EXACTLY -1, retention.bytes EXACTLY -1 (pure compact)  the newest reading per session
#   options.spx.gamma-effort.log      1 partition, delete,  retention.ms EXACTLY -1, retention.bytes EXACTLY 536870912 (512 MiB)  EVERY emission, in order: the corpus
#
# WHY THE BARRIER EXISTS AT ALL when the service itself creates what is missing. The service deliberately CREATES-IF-ABSENT and then only CHECKS the partition count: it
# never reconciles a topic's configuration, because a pod start that overwrites a declared shape with whatever the running binary believes is how this fleet has lost a
# compaction setting before. So the declared shape - in particular the 512 MiB byte cap, which the service does not know - has to be applied by something that reads
# topics.env, and that is this script. An existing topic is never recreated here, and its partition count, cleanup policy and retention.ms are never altered: any
# difference in those fails closed.
#
# ONE EXCEPTION, AND WHY IT HAD TO BE MADE: the log's retention.bytes is RAISED to the declared cap when the live topic is below it, and a cap BELOW the live value is
# refused. Until 2026-11-14 this script refused any difference in either direction, and the consequence was measured: the 2026-11-13 raise from 256 to 512 MiB (#1637)
# was merged into topics.env and NEVER REACHED THE CLUSTER, because the only thing that applies the cap is the create path and the topic already existed. The live
# topic was still 256 MiB a day later - 21.8 sessions of corpus against the design's section 11 floor of 20 to 25, the exact shortfall the raise was made to avoid -
# and raising this script's literal alone would have made the barrier reject the existing topic and fail EVERY gamma-effort deploy until someone altered it by hand.
# A declared value that no deploy can apply is not a declaration; it is a comment. So the raise is applied here, which is also what makes the two directions safe to
# treat differently: RAISING a byte cap deletes nothing, while LOWERING one deletes the oldest corpus records immediately - and this topic IS the corpus, the only
# thing that settles the design's calibration question. A lower cap is therefore a deliberate, reviewed operation, never a deploy's side effect. This is the rule
# ensure-level-absorption-topics.sh already applies to its own events log ("never lowered"), not a new one invented here.
#
# WHY THE LOG MUST NOT BE COMPACTED, checked here and not merely declared: the key is the session, so compaction would keep one record per session and discard exactly the
# intraday history the topic exists to accumulate. That corpus is the ONLY way the design's own calibration question gets settled - its section 12.3 measured the shipped
# parameters producing "aggressor ahead" on 53% of cash minutes, near maximum entropy - so a single wrong cleanup.policy here silently removes the service's whole purpose.
#
# The INPUT topics (es.futures.footprint.bars, options.databento.gex.strike) belong to other owners and are not created or altered here.
# dev-cleanup.sh does NOT run this barrier: its topic pass recreates the shapes declared in scripts/kafka/topics.env.
set -euo pipefail

if [ "${ENVIRONMENT:-}" != "dev" ]; then
  echo "FAIL: ensure-gamma-effort-topics.sh is DEV ONLY and fails closed: ENVIRONMENT must be exactly 'dev' (got '${ENVIRONMENT:-<unset>}'); gamma-effort has no production topics - refusing."
  exit 1
fi
# The DECLARED names, not overridable. An override would let this script create an arbitrary topic
# with gamma-effort's shape, so the barrier would no longer match topics.env in both directions --
# and the name it creates is the one nothing else preserves: reset, dev-clean and never-recreate all
# list these two literals. A renamed log would be swept by every one of those paths while looking,
# from inside this script, perfectly correct.
CURRENT="options.spx.gamma-effort.current"
LOG="options.spx.gamma-effort.log"
if [ -n "${KAFKA_GAMMA_EFFORT_CURRENT_TOPIC:-}" ] && [ "${KAFKA_GAMMA_EFFORT_CURRENT_TOPIC}" != "$CURRENT" ]; then
  echo "FAIL: KAFKA_GAMMA_EFFORT_CURRENT_TOPIC is set to '${KAFKA_GAMMA_EFFORT_CURRENT_TOPIC}', but this barrier only ever ensures the declared '$CURRENT' - refusing."
  exit 1
fi
if [ -n "${KAFKA_GAMMA_EFFORT_LOG_TOPIC:-}" ] && [ "${KAFKA_GAMMA_EFFORT_LOG_TOPIC}" != "$LOG" ]; then
  echo "FAIL: KAFKA_GAMMA_EFFORT_LOG_TOPIC is set to '${KAFKA_GAMMA_EFFORT_LOG_TOPIC}', but this barrier only ever ensures the declared '$LOG' - refusing. A renamed corpus log is preserved by nothing."
  exit 1
fi
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS unset - refusing to verify against an unknown cluster}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
# MIN_RF is read by the SOURCED ensure-topic-contract.lib.sh, not by this file. Dropping it because
# nothing here mentions it made the first live run die with "MIN_RF: unbound variable" under set -u,
# after it had already created the first topic. A sourced library's inputs are part of this script's
# contract even when this script never names them.
MIN_RF="${GAMMA_EFFORT_MIN_REPLICATION_FACTOR:-1}"
MIN_ISR="${GAMMA_EFFORT_MIN_INSYNC_REPLICAS:-1}"
kt() { kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
kc() { kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@"; }
HERE="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
source "$HERE/topic-config-parse.sh"
# shellcheck source=/dev/null
source "$HERE/ensure-topic-contract.lib.sh"

echo "=== ensure-gamma-effort-topics on $KAFKA_BOOTSTRAP_SERVERS ==="
if ! LIST=$(kt --list 2>&1); then
  echo "FAIL: cannot list topics — refusing to deploy on an unverified cluster:"; printf '%s\n' "$LIST"; exit 1
fi

# The declared cap, scripts/kafka/topics.env's OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES entry for this topic. It is a literal here, as in every sibling barrier, and
# scripts/ci/validate-barrier-byte-caps.sh is what keeps the two from drifting: it parses this assignment and that declaration and fails the build when they disagree.
# That validator exists because this pair DID disagree for a day and nothing noticed - the only tell was a comment whose own arithmetic contradicted itself.
LOG_BYTES=536870912

# retention.bytes is not part of the shared routine's contract: read the EFFECTIVE value (--all, what a service's own describeConfigs sees) and compare exactly.
# `--describe --all` prints SYNONYMS as well as the effective value, which is why the comparison goes through extract()'s own boundary rather than a bare grep: a bare
# grep counts one value several times and refuses a correct topic.
#
# ONE read, used by both the raise and the check, so the value the barrier acts on and the value it then verifies cannot be parsed two different ways. A guard that
# parses differently from the thing it guards is checking a different question.
read_retention_bytes() { # <topic> -> the effective retention.bytes on stdout, or empty
  local topic="$1" cfg
  if ! cfg=$(kc --entity-type topics --entity-name "$topic" --describe --all 2>&1); then
    echo "FAIL: could not read the config of '$topic':" >&2; printf '%s\n' "$cfg" >&2; return 1
  fi
  extract 'retention\.bytes' "$cfg"
}

# Bring an EXISTING topic up to the declared cap. Raise only: see the header. A live value at or above the declared one is left alone here and judged by
# check_retention_bytes, which is what refuses the lower-cap case by name.
raise_retention_bytes() { # <topic> <declared retention.bytes>
  local topic="$1" want="$2" cur out
  cur=$(read_retention_bytes "$topic") || return 1
  case "${cur:-}" in ''|*[!0-9]*) return 0 ;; esac   # -1 (unlimited) or unreadable: not a raise, left to the exact check
  if [ "$cur" -lt "$want" ]; then
    echo "raising '$topic' retention.bytes $cur -> $want (the declared cap in scripts/kafka/topics.env; never lowered here)"
    if ! out=$(kc --entity-type topics --entity-name "$topic" --alter --add-config "retention.bytes=$want" 2>&1); then
      echo "FAIL: could not raise retention.bytes of '$topic':"; printf '%s\n' "$out"; return 1
    fi
  fi
}

check_retention_bytes() { # <topic> <exact retention.bytes>
  local topic="$1" want="$2" bytes
  bytes=$(read_retention_bytes "$topic") || return 1
  case "${bytes:-}" in
    ''|*[!0-9-]*|-|?*-*|-[!1]*|-1?*) echo "FAIL: '$topic' reports retention.bytes='${bytes:-<unreadable>}', expected exactly $want."; return 1 ;;
  esac
  if [ "$bytes" != "$want" ]; then
    if [ "$bytes" -gt "$want" ] 2>/dev/null; then
      echo "FAIL: '$topic' has retention.bytes=$bytes, ABOVE the declared $want (scripts/kafka/topics.env). Lowering a cap on the corpus log deletes its oldest records"
      echo "      immediately, so it is a deliberate reviewed operation and never a deploy's side effect: alter the topic by hand, or raise the declaration to match."
    else
      echo "FAIL: '$topic' has retention.bytes=$bytes, expected exactly $want (the declared shape in scripts/kafka/topics.env)."
    fi
    return 1
  fi
  echo "ok: '$topic' — retention.bytes=$bytes"
}

rc=0

# options.spx.gamma-effort.current — pure compact. Created here WITH retention.bytes=-1, then verified by the shared routine with create-if-absent=no, so this path can
# never leave a topic at the broker's default shape.
if ! grep -qxF "$CURRENT" <<<"$LIST"; then
  echo "creating '$CURRENT' (1 partition, RF=$RF, cleanup.policy=compact, retention.ms=-1, retention.bytes=-1)"
  if ! out=$(kt --create --if-not-exists --topic "$CURRENT" --partitions 1 --replication-factor "$RF" \
                --config cleanup.policy=compact --config retention.ms=-1 --config retention.bytes=-1 \
                --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
    echo "FAIL: could not create '$CURRENT':"; printf '%s\n' "$out"; rc=1
  else
    LIST="$LIST"$'\n'"$CURRENT"
  fi
fi
if [ "$rc" -eq 0 ]; then
  ensure_topic "$CURRENT" compact "exact:-1" 1048576 no || rc=1
  check_retention_bytes "$CURRENT" -1 || rc=1
fi

# options.spx.gamma-effort.log — the corpus. DELETE only, never compacted, retention.ms exactly -1, and the byte cap is the only thing that ever removes a record.
if ! grep -qxF "$LOG" <<<"$LIST"; then
  echo "creating '$LOG' (1 partition, RF=$RF, cleanup.policy=delete, retention.ms=-1, retention.bytes=$LOG_BYTES)"
  if ! out=$(kt --create --if-not-exists --topic "$LOG" --partitions 1 --replication-factor "$RF" \
                --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes="$LOG_BYTES" \
                --config max.message.bytes=1048576 --config min.insync.replicas="$MIN_ISR" 2>&1); then
    echo "FAIL: could not create '$LOG':"; printf '%s\n' "$out"; rc=1
  else
    LIST="$LIST"$'\n'"$LOG"
  fi
fi
if [ "$rc" -eq 0 ]; then
  ensure_topic "$LOG" delete "exact:-1" 1048576 no || rc=1
  raise_retention_bytes "$LOG" "$LOG_BYTES" || rc=1
  check_retention_bytes "$LOG" "$LOG_BYTES" || rc=1
fi

if [ "$rc" -ne 0 ]; then
  echo "=== ensure-gamma-effort-topics FAILED - the gamma effort service must not publish ==="
  exit 1
fi
echo "=== ensure-gamma-effort-topics OK ==="
