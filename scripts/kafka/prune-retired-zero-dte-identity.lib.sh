#!/usr/bin/env bash
# prune-retired-zero-dte-identity.lib.sh — SINGLE implementation of the zero-orphan prune
# for the retired zero-dte-intelligence identity (One Service One Identity Rule).
#
# The service formerly published {prefix}options.spx.0dte.intelligence.current under
# consumer group zero-dte-intelligence-service-v1[-profile]. That hyphen namespace is
# contractually retired: version-suffixed identities are banned by the One Service One
# Identity Rule, so nothing legitimate can ever be created under it again.
#
# Caller contract (both production callers source this file):
#   1. run under set -euo pipefail;
#   2. define prune_kt() / prune_kg() wrapping kafka-topics / kafka-consumer-groups for
#      the target broker (host CLIs for prod, docker-exec for es4);
#   3. call prune_retired_zero_dte_identity "<legacy topic name incl. any prefix>".
#
# Guarantees: exact-name topic match; exact-prefix group match; FAIL-CLOSED discovery (a
# listing failure aborts, never reads as "no orphans"); fail-loud deletes; bounded
# asynchronous topic-delete verification; terminal group re-list verification. Matching
# captures grep WITHOUT -q (grep -q early-exit can SIGPIPE the producer under pipefail
# and falsely report absence on large listings).

prune_retired_zero_dte_identity() {
  local legacy_topic="$1"
  local prefix="zero-dte-intelligence-service-v1"
  local wait_s="${KAFKA_TOPIC_DELETE_WAIT_SECONDS:-90}"
  local topics_now present deleted still groups_now legacy_groups remaining g

  topics_now="$(prune_kt --list)" \
    || { echo "FATAL: could not list topics — refusing to prune (fail-closed)" >&2; return 1; }
  present="$(printf '%s\n' "$topics_now" | grep -Fx "$legacy_topic" || true)"
  if [ -n "$present" ]; then
    prune_kt --delete --topic "$legacy_topic" >/dev/null \
      || { echo "FATAL: failed to delete retired topic $legacy_topic" >&2; return 1; }
    # Topic deletion is asynchronous — verify terminal absence within a bounded wait.
    deleted=false
    for _ in $(seq 1 "$wait_s"); do
      topics_now="$(prune_kt --list)" \
        || { echo "FATAL: could not re-list topics during delete verification" >&2; return 1; }
      still="$(printf '%s\n' "$topics_now" | grep -Fx "$legacy_topic" || true)"
      if [ -z "$still" ]; then deleted=true; break; fi
      sleep 1
    done
    [ "$deleted" = "true" ] \
      || { echo "FATAL: $legacy_topic still present ${wait_s}s after delete" >&2; return 1; }
    echo "$legacy_topic deleted and verified absent (retired identity)"
  else
    echo "$legacy_topic absent (nothing to prune)"
  fi

  groups_now="$(prune_kg --list)" \
    || { echo "FATAL: could not list consumer groups — refusing to prune (fail-closed)" >&2; return 1; }
  legacy_groups="$(printf '%s\n' "$groups_now" | grep -E "^${prefix}(-|$)" || true)"
  if [ -z "$legacy_groups" ]; then
    echo "no ${prefix}* consumer groups remain (nothing to prune)"
    return 0
  fi
  # Line-safe iteration: group ids are not guaranteed whitespace-free, and word splitting
  # would turn one matching group into deletion requests against unrelated names.
  while IFS= read -r g; do
    [ -n "$g" ] || continue
    # --delete refuses a group with live members; that would mean the retired identity is
    # still running somewhere, which must fail the deploy loudly, never be skipped.
    prune_kg --delete --group "$g" >/dev/null \
      || { echo "FATAL: failed to delete retired consumer group $g (still active?)" >&2; return 1; }
    echo "consumer group $g deleted (retired identity)"
  done <<EOF
$legacy_groups
EOF
  # Terminal zero-orphan proof: re-list and require that no retired group remains.
  groups_now="$(prune_kg --list)" \
    || { echo "FATAL: could not re-list consumer groups during verification" >&2; return 1; }
  remaining="$(printf '%s\n' "$groups_now" | grep -E "^${prefix}(-|$)" || true)"
  [ -z "$remaining" ] \
    || { echo "FATAL: retired groups still present after delete: $remaining" >&2; return 1; }
  echo "zero-orphan verified: no ${prefix}* consumer groups remain"
}
