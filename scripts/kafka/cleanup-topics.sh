#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${ENVIRONMENT:?ENVIRONMENT is required}"
if [[ "${KAFKA_CLEANUP_TOPICS:-false}" != "true" ]]; then
  echo "Kafka cleanup disabled. Set KAFKA_CLEANUP_TOPICS=true to enable."
  exit 0
fi
if [[ "$ENVIRONMENT" == "production" && "${ALLOW_PROD_KAFKA_CLEANUP:-false}" != "true" ]]; then
  echo "Refusing production Kafka cleanup without ALLOW_PROD_KAFKA_CLEANUP=true" >&2
  exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/topics.env"
# TOPIC_SET is REFUSED rather than handled. The approved list below is built from the default
# OPTIONS_EDGE_TOPICS with no topic-set switching, so under TOPIC_SET=es4 this script would iterate
# the SPX topics against an es4 broker -- deleting the wrong cluster's data.
#
# This refusal is deliberately the FIRST thing after the declarations are sourced. It used to sit
# below the approved-list construction, which was harmless only because nothing read the list in
# between -- and the prod-only merge added below has a predicate ("production AND the default topic
# set", the same one apply-topics.sh uses) whose second half is now true by construction instead of
# by reading order.
if [[ -n "${TOPIC_SET:-}" ]]; then
  echo "Refusing cleanup with TOPIC_SET='$TOPIC_SET': this script only handles the default topic set." >&2
  echo "Its approved-topic list comes from OPTIONS_EDGE_TOPICS and does not switch on TOPIC_SET." >&2
  exit 1
fi

# --- What the repo DECLARES for this environment (the delete-unwanted question) -----------------
# Two different lists, because this script asks two different questions and they had been sharing
# one answer:
#
#   approved_file  -- the topics this cleanup MANAGES: the ones its destructive modes delete and
#                     recreate, or shrink. Built from OPTIONS_EDGE_TOPICS, exactly as before.
#   declared_file  -- the topics the repo DECLARES for this environment at all. Only the
#                     KAFKA_DELETE_UNWANTED_TOPICS loop consults it, and only to answer "is this
#                     topic ours?" before deleting it as junk.
#
# THE BUG. declared_file did not exist; the unwanted loop consulted approved_file, which is built
# from OPTIONS_EDGE_TOPICS alone. Every topic declared ONLY for production --
# OPTIONS_EDGE_PROD_ONLY_TOPICS, which apply-topics.sh merges under exactly the predicate below --
# was therefore matched by neither the approved list nor PROTECTED_TOPIC_REGEX, and a production
# cleanup with KAFKA_DELETE_UNWANTED_TOPICS=true deleted it as "unwanted": the one environment
# those topics exist on was the one environment that swept them.
#
# WHY IT MATTERS EVEN THOUGH APPLY-TOPICS RUNS LATER IN THE SAME BUILD. The Kafka Cleanup stage is
# gated on (DEPLOY_TARGET=all && KAFKA_CLEANUP_TOPICS && !DEPLOY_DRY_RUN) while the Kafka Topics
# stage that would recreate them is additionally gated on !SKIP_KAFKA_TOPICS. So a
# SKIP_KAFKA_TOPICS=true "code-only redeploy" with cleanup enabled deletes them and never recreates
# them, leaving the producing service's own auto-create to stamp broker defaults. Two of the three
# (es.futures.cvd.levels, options.es-cvd-spx-levels) are declared PURE COMPACT + retention.ms=-1 --
# a last-value-per-key view that plain-delete defaults silently destroy. That is the same trap the
# compaction comments in topics.env document, reached through the cleanup path instead.
#
# WHY THE PROD-ONLY TOPICS ARE **NOT** MERGED INTO approved_file. Merging there would have been the
# smaller diff, and it is wrong: the destructive modes iterate approved_file, so on production the
# prod-only topics would start being delete-recreated and retention-shrunk when they are not today.
# Combined with the SKIP_KAFKA_TOPICS gap above -- cleanup runs, apply does not -- that turns a
# protection fix into a way to leave es.futures.cvd.levels absent, or pinned at retention.ms=1000,
# with nothing in the build to restore it. A topic that survives a given flag combination today
# must still survive it after a change whose entire purpose is to stop deleting it.
#
# EXACT PREDICATE, matching apply-topics.sh: ENVIRONMENT EXPLICITLY 'production'. ENVIRONMENT is a
# hard requirement at the top of this script, so there is no unset case to fail closed on; and the
# "default topic set" half holds by construction, since every non-empty TOPIC_SET was refused above.
# Anything other than 'production' leaves the prod-only topics undeclared, which is correct -- on
# dev they genuinely are, and SHOULD be swept.
#
# The other PROD_ONLY_* sets (pure-compact, exact-partition, retention overrides) are deliberately
# NOT consulted: this list only ever answers "is this topic declared?", never what shape it should
# have. The prod-only RESET-PRESERVED list is already handled, by reset-preserved-topics.sh below.
approved_file="$(mktemp)"
declared_file="$(mktemp)"
trap 'rm -f "$approved_file" "$declared_file"' EXIT
for entry in $OPTIONS_EDGE_TOPICS; do echo "${entry%%:*}" >> "$approved_file"; done
cp "$approved_file" "$declared_file"
if [[ "$ENVIRONMENT" == "production" ]]; then
  echo "[cleanup-topics] ENVIRONMENT=production: prod-only topics are DECLARED and will not be swept as unwanted (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>})"
  for entry in ${OPTIONS_EDGE_PROD_ONLY_TOPICS:-}; do echo "${entry%%:*}" >> "$declared_file"; done
else
  echo "[cleanup-topics] ENVIRONMENT='$ENVIRONMENT': prod-only topics (${OPTIONS_EDGE_PROD_ONLY_TOPICS:-<none>}) are NOT declared here and remain sweepable -- they are declared for production only."
fi

# Topics declared RESET-PRESERVED hold data that cannot be rebuilt, and this script's destructive
# modes must skip them.
#
# Driven by OPTIONS_EDGE_RESET_PRESERVED_TOPICS, NOT by retention.ms=-1. The two are different
# claims: retention=-1 says "do not age this out by time", preservation says "this cannot be
# reconstructed". options.spx.spot-vol-regime.current needs the first and must NOT get the second --
# it is live state its service republishes seconds after starting, and inferring preservation from
# retention would have made dev unable to wipe it.
#
# PROTECTED_TOPIC_REGEX does not cover this: it is consulted only when deleting UNWANTED topics.
# Both destructive paths below iterate the APPROVED list, where every preserved topic also appears.
#
# shellcheck source=/dev/null
. "$SCRIPT_DIR/reset-preserved-topics.sh"
durable_file="$(mktemp)"
trap 'rm -f "$approved_file" "$declared_file" "$durable_file"' EXIT
printf '%s\n' $RESET_PRESERVED_TOPICS | sort -u > "$durable_file"
is_durable() { grep -qx "$1" "$durable_file"; }
if [[ -s "$durable_file" ]]; then
  echo "Durable topics exempt from destructive cleanup: $(tr '\n' ' ' < "$durable_file")"
fi

describe_topic() {
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$1" 2>/dev/null | sed -n '/^Topic:/p' || true
}

topic_id_from_description() {
  echo "$1" | head -1 | sed -n 's/.*TopicId: \([^[:space:]]*\).*/\1/p'
}

wait_for_topic_absent() {
  local topic="$1"
  local previous_topic_id="${2:-}"
  local attempts="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"

  if [[ -z "$previous_topic_id" ]]; then
    echo "Topic $topic was absent before cleanup; no delete wait needed."
    return 0
  fi

  for ((i = 1; i <= attempts; i++)); do
    description="$(describe_topic "$topic")"
    if [[ -z "$description" ]]; then
      return 0
    fi
    current_topic_id="$(topic_id_from_description "$description")"
    if [[ -n "$previous_topic_id" && -n "$current_topic_id" && "$current_topic_id" != "$previous_topic_id" ]]; then
      echo "Topic $topic was recreated while waiting for deletion; cleanup will let apply-topics validate/recreate it."
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for deleted topic to disappear: $topic" >&2
  describe_topic "$topic"
  return 1
}

if [[ "${KAFKA_DELETE_UNWANTED_TOPICS:-false}" == "true" ]]; then
  kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | while read -r topic; do
    if [[ "$topic" =~ $PROTECTED_TOPIC_REGEX ]]; then
      echo "Keeping protected topic: $topic"
    elif grep -qx "$topic" "$declared_file"; then
      echo "Keeping approved topic: $topic"
    else
      echo "Deleting unwanted topic: $topic"
      previous_topic_id="$(topic_id_from_description "$(describe_topic "$topic")")"
      kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic"
      wait_for_topic_absent "$topic" "$previous_topic_id"
    fi
  done
fi
if [[ "${KAFKA_CLEANUP_MODE:-retention}" == "delete-recreate" ]]; then
  declare -A previous_topic_ids
  while read -r topic; do
    previous_topic_ids["$topic"]="$(topic_id_from_description "$(describe_topic "$topic")")"
  done < "$approved_file"
  while read -r topic; do
    if [[ -z "${previous_topic_ids[$topic]:-}" ]]; then
      echo "Approved app topic absent before cleanup: $topic"
      continue
    fi
    if is_durable "$topic"; then
      echo "Keeping DURABLE approved topic (retention.ms=-1): $topic"
      continue
    fi
    echo "Deleting approved app topic: $topic"
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$topic" || true
  done < "$approved_file"
  while read -r topic; do
    if is_durable "$topic"; then
      continue
    fi
    echo "Waiting for deleted approved app topic to disappear: $topic"
    wait_for_topic_absent "$topic" "${previous_topic_ids[$topic]:-}"
  done < "$approved_file"
else
  while read -r topic; do
    if is_durable "$topic"; then
      echo "Keeping DURABLE approved topic (retention.ms=-1), not shrinking: $topic"
      continue
    fi
    echo "Temporarily shrinking retention for: $topic"
    kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=1000 || true
  done < "$approved_file"
fi
