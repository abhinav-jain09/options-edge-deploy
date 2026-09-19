#!/usr/bin/env bash
# ensure-partition-only-topics.sh — create every OPTIONS_EDGE_PARTITION_ONLY_TOPICS entry that is missing at
# its declared partition count (NO configs: the owning service stamps policy/retention), grow a smaller
# existing copy, and name any larger one (Kafka cannot shrink a topic). Runs BEFORE any service starts so no
# client can choose these topics' size. Same rule dev-cleanup.sh applies on dev; this is the standalone,
# environment-aware copy the prod clean-slate uses after a full topic wipe.
#
# ENVIRONMENT=production: entries whose name carries a dev application id (`-dev-`) are SKIPPED and counted.
# They are dev's copies of app-private topics; on prod the owning app creates its `-prod-` copy itself at its
# own count, and prod's broker has auto.create.topics.enable=false, so nothing else can create it first.
#
# Env: KAFKA_BOOTSTRAP_SERVERS (required), TOPICS_ENV (default: sibling topics.env), KAFKA_TOPICS_CMD
# (default: kafka-topics on PATH — the same binary apply-topics.sh uses), PARALLELISM (default 8),
# KAFKA_TOPIC_REPLICATION_FACTOR (default 1).
set -uo pipefail
BS="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be set}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPICS_ENV="${TOPICS_ENV:-$HERE/topics.env}"
KT="${KAFKA_TOPICS_CMD:-kafka-topics}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
# shellcheck source=/dev/null
. "$TOPICS_ENV"
[ -n "${OPTIONS_EDGE_PARTITION_ONLY_TOPICS:-}" ] || { echo "Partition-only topics: none declared in $TOPICS_ENV"; exit 0; }

desc="$("$KT" --bootstrap-server "$BS" --describe 2>/dev/null)" \
  || { echo "ERROR: could not describe topics at $BS — partition-only topics NOT ensured (nothing created blind)" >&2; exit 1; }
have="$(printf '%s\n' "$desc" | awk -F'\t' '$1 ~ /^Topic: / {n=$1; sub(/^Topic: /, "", n); for (i = 2; i <= NF; i++) if ($i ~ /^PartitionCount: /) {c=$i; sub(/^PartitionCount: /, "", c); print n, c}}')"

to_create=""; wanted=0; grown=0; larger=""; skipped_dev=""
for spec in $OPTIONS_EDGE_PARTITION_ONLY_TOPICS; do
  name="${spec%%:*}"; want="${spec##*:}"
  if [ "${ENVIRONMENT:-}" = "production" ]; then
    case "$name" in *-dev-*) skipped_dev="$skipped_dev $name"; continue ;; esac
  fi
  cur="$(printf '%s\n' "$have" | awk -v t="$name" '$1 == t {print $2; exit}')"
  if [ -z "$cur" ]; then
    to_create="$to_create--topic $name --partitions $want --replication-factor $RF
"
    wanted=$((wanted+1))
  elif [ "$cur" -lt "$want" ]; then
    # --topic is a regular expression to --alter: escape the dots so only this topic can match.
    "$KT" --bootstrap-server "$BS" --alter --topic "$(printf '%s' "$name" | sed 's/\./\\./g')" --partitions "$want" >/dev/null 2>&1 \
      && grown=$((grown+1)) || echo "  WARNING: could not grow $name $cur -> $want"
  elif [ "$cur" -gt "$want" ]; then
    larger="$larger $name($cur>$want)"
  fi
done
created=0
if [ "$wanted" -gt 0 ]; then
  # Trailing blanks stripped and blank lines dropped: `xargs -L 1` treats a line ending in a blank as
  # continuing onto the next line (2026-09-16: 15 of 137 topics created on dev because of that).
  created=$(printf '%s' "$to_create" | sed -e 's/[[:space:]]*$//' -e '/^$/d' \
    | xargs -P "${PARALLELISM:-8}" -L 1 sh -c "\"$KT\" --bootstrap-server \"$BS\" --create --if-not-exists \"\$@\" >/dev/null 2>&1 && echo CREATED" sh \
    | grep -c '^CREATED$' || true)
  [ "$created" -eq "$wanted" ] || echo "  WARNING: created $created of $wanted missing partition-only topics"
fi
n_skip=$(printf '%s\n' $skipped_dev | grep -c . || true)
echo "Partition-only topics: created $created, grown $grown, skipped dev-named $n_skip (ENVIRONMENT=${ENVIRONMENT:-unset}; $TOPICS_ENV)"
[ -z "$larger" ] || echo "  WARNING: larger than declared (Kafka cannot shrink; a wipe recreates them):$larger"
[ "$created" -eq "$wanted" ]
