#!/usr/bin/env bash
# U16 (CL-R10 L4/O3): promtool check + test over the declared paging rules. promtool lives on the
# production host (where prometheus runs); CI ships the files over and validates there.
set -euo pipefail
cd "$(dirname "$0")/../.."
HOST="${PROMTOOL_HOST:-abhinav@192.168.100.252}"
TMP="/tmp/cvd-levels-alerts.$$"
trap 'ssh "$HOST" "rm -rf $TMP" 2>/dev/null || true' EXIT
ssh "$HOST" "mkdir -p $TMP"
scp -q monitoring/cvd-spx-levels-alerts.yml monitoring/cvd-spx-levels-alerts-test.yml "$HOST:$TMP/"
ssh "$HOST" "cd $TMP && promtool check rules cvd-spx-levels-alerts.yml && promtool test rules cvd-spx-levels-alerts-test.yml"
echo "=== validate-cvd-levels-alerts: OK ==="
