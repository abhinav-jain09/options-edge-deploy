#!/usr/bin/env bash
# The ARCHIVE UNCOMPACTION FLAG must reach prod and es4, and must never reach dev.
#
# WHY THIS EXISTS. Compaction on the archive environments came back over and over because the
# repair was only ever made on ONE of the two writers. processing-common
# app.kafka.KafkaTopics.ensureTopic() calls incrementalAlterConfigs on EVERY service boot, so
# whatever the deploy-time topic scripts write is overwritten by the next restart unless the
# boot-time stamp itself is told to stamp delete. That instruction is a single environment
# variable, OPTIONS_EDGE_UNCOMPACTED_SERVED_TOPICS, read by KafkaTopics.ensureServedTopic():
#
#   absent / anything but "true"  -> stamp cleanup.policy=compact   (dev's behaviour, unchanged)
#   exactly "true"                -> stamp cleanup.policy=delete    (prod + es4: pure archive)
#
# So the flag is not a tuning knob. It is the boot-time half of the fix, and a deploy that
# renders without it silently re-shreds the archive on the next pod restart with no error
# anywhere. This test fails the build instead.
#
# DEV IS DELIBERATELY EXCLUDED. Dev has no archival duty and a small disk; compaction is what
# keeps it bounded. A flag that leaked into dev would be just as wrong as one missing from prod,
# so both directions are asserted.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

FLAG="OPTIONS_EDGE_UNCOMPACTED_SERVED_TOPICS"
fail=0

note() { printf '  %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; fail=1; }

render() { kubectl kustomize "$1" 2>/dev/null; }

# ── 1. production overlay renders the flag, as "true", on the shared configmap ────────────────
echo "[archive-uncompaction] production overlay"
prod="$(render k8s/overlays/production)"
if [ -z "$prod" ]; then
  bad "k8s/overlays/production did not render"
else
  got="$(printf '%s' "$prod" | python3 -c '
import sys, yaml
flag = "'"$FLAG"'"
holders = []
for d in yaml.safe_load_all(sys.stdin):
    if not d or d.get("kind") != "ConfigMap":
        continue
    data = d.get("data") or {}
    if flag in data:
        holders.append((d["metadata"]["name"], data[flag]))
for name, value in holders:
    print(f"{name}={value}")
')"
  case "$got" in
    "options-edge-config=true") note "options-edge-config carries $FLAG=true" ;;
    "")  bad "$FLAG absent from the production render — the boot-time stamp stays COMPACT and prod re-compacts on every restart" ;;
    *)   bad "$FLAG rendered unexpectedly as: $got (want exactly options-edge-config=true)" ;;
  esac

  # The flag is worthless unless the workloads actually read that configmap.
  users="$(printf '%s' "$prod" | python3 -c '
import sys, yaml
n = 0
for d in yaml.safe_load_all(sys.stdin):
    if not d or d.get("kind") not in ("Deployment", "StatefulSet", "Job", "CronJob"):
        continue
    tpl = d["spec"].get("template")
    if tpl is None:
        tpl = d["spec"].get("jobTemplate", {}).get("spec", {}).get("template")
    if not tpl:
        continue
    for c in tpl["spec"].get("containers", []):
        for e in c.get("envFrom", []) or []:
            if (e.get("configMapRef") or {}).get("name") == "options-edge-config":
                n += 1
print(n)
')"
  if [ "${users:-0}" -lt 20 ]; then
    bad "only ${users:-0} prod containers envFrom options-edge-config — the flag would reach almost nothing"
  else
    note "$users prod containers envFrom options-edge-config"
  fi
fi

# ── 2. dev overlay must NOT render the flag ───────────────────────────────────────────────────
echo "[archive-uncompaction] dev overlay"
dev="$(render k8s/overlays/dev)"
if [ -z "$dev" ]; then
  bad "k8s/overlays/dev did not render"
# NOTE: no `| grep -q` here. grep -q exits at the first match, printf then dies of SIGPIPE,
# and `set -o pipefail` turns the whole pipeline into 141 — so the leak test would report
# "clean" precisely when the flag IS present. Bash substring matching has no pipeline at all.
elif [ "${dev#*"$FLAG"}" != "$dev" ]; then
  bad "$FLAG leaked into the dev render — dev must stay compacted (bounded disk, no archival duty)"
else
  note "dev render is free of $FLAG"
fi

# ── 3. es4 shares the same duty, via its own env configmap ────────────────────────────────────
echo "[archive-uncompaction] es4 common env"
es4="k8s/es4/es4-common-env.yaml"
if ! grep -qE "^[[:space:]]+${FLAG}:[[:space:]]*\"true\"[[:space:]]*$" "$es4"; then
  bad "$es4 does not set $FLAG: \"true\" — es4 re-compacts its archive on the next restart"
else
  note "$es4 sets $FLAG=true"
fi

if [ "$fail" -ne 0 ]; then
  echo "[archive-uncompaction] FAILED"
  exit 1
fi
echo "[archive-uncompaction] OK"
