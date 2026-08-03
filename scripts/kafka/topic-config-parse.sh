#!/usr/bin/env bash
# ONE implementation of "read a value out of kafka-configs --describe output", sourced by both
# ensure-oi-anchor-topic.sh and its test. It lived in two copies, which is a test that can pass
# while the shipped parser is wrong -- the exact failure this parser already had once.

# extract <key-regex> <config-text> -> the whitespace-delimited value, or empty.
#
# grep -oE, not sed: BSD sed (the dev Macs) has no \| alternation in BRE, so a boundary written
# that way silently matches NOTHING there while working on the Linux agent. A check that passes for
# the wrong reason on one platform is worse than no check.
#
# The boundary matters. A bare "retention.ms" substring also occurs inside "delete.retention.ms" --
# a different knob, routinely set on compacted topics -- so an unanchored match reads the tombstone
# retention and reports it as the topic's retention.
#
# No match is a legitimate answer ("the key is absent"), not an error: without the `|| true`,
# grep's exit 1 trips the caller's set -e before it can report WHICH check failed.
extract() {
  { printf '%s' "$2" | grep -oE "(^|[[:space:],])$1=[^ ]*" || true; } | head -1 | sed -E "s/.*$1=//"
}
