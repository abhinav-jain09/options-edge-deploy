#!/usr/bin/env bash
# validate-heap-leaves-container-headroom.sh — CI invariant.
#
# A container whose -Xmx equals its memory limit is OOMKilled before the JVM ever reaches its own
# ceiling. The heap is only part of a JVM's footprint: RocksDB (every Kafka Streams service here
# holds state off-heap), metaspace, thread stacks, code cache and direct buffers all live outside
# it. The kernel kills the cgroup first, so there is NO OutOfMemoryError in the log and no heap
# dump — only exit 137, which reads like a resource shortage rather than a misconfiguration.
#
# 2026-10-19: prod option-pressure-state-service (limit 1Gi, -Xmx1g) OOMKilled on a loop. Two more
# services carried the identical 100% setting and had simply not been pushed hard enough yet:
# greek-move-authenticity-service and databento-maxpain-service.
#
# The bar is this fleet's own practice, not a number invented here. The 41 prod workloads that pin
# a heap sat at 12%-100%, everything healthy at or below 80%, modal value 50%. So 80% is the
# ceiling: it rejects the 100% mistake and anything approaching it, while leaving the deliberate
# 80% (databento-gex-service) alone.
#
# READ THE RENDER, NOT THE BASE: an overlay can change either the limit or JAVA_TOOL_OPTIONS, so a
# healthy-looking base can still render a 100% container. This walks the rendered overlays, and it
# parses them as YAML — a container block and an `env` entry both begin `- name:`, so a regex
# sweep misreads every JAVA_TOOL_OPTIONS entry as a limitless container.
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 scripts/ci/heap_headroom.py "${HEAP_MAX_PERCENT_OF_LIMIT:-80}"
echo "=== validate-heap-leaves-container-headroom: OK ==="
