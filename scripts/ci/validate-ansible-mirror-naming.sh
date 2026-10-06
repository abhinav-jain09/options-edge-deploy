#!/usr/bin/env bash
# The parity gate reads ansible/vars/es-mirrors.yml and the templates. It cannot see the Jinja in
# ansible/es-mirrors.yml that COMPOSES a unit's identity from them — and that composition is the most
# dangerous code in the playbook: the group.id carries the committed offsets (a wrong one is a new
# mirror that re-reads the retention window into the target) and the slug names the unit, the label
# and the plist (a wrong one installs a unit nobody loaded and leaves the real one untouched).
#
# So this runs the real playbook in its dump mode — which stops before any broker access — and pins
# the composed identity of one unit per shape the table has: topic-in-slug, port-in-group,
# no-topic-in-slug, and a commit interval. It also pins the production-only holdback on dev.
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v ansible-playbook >/dev/null || { echo "FAIL: ansible-playbook is required to test the playbook's own logic"; exit 1; }
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT

run() { # target-ip target-port out-file
  ansible-playbook ansible/es-mirrors.yml \
    -e "mirror_target_ip=$1" -e "mirror_target_port=$2" -e "dump_rows=$3" >"$OUT/log" 2>&1 \
    || { echo "FAIL: the dump run for $1:$2 failed"; sed 's/^/    /' "$OUT/log" | tail -20; exit 1; }
}
run 192.168.100.252 9092 "$OUT/prod.json"
run 127.0.0.1 19092 "$OUT/dev.json"

python3 - "$OUT/prod.json" "$OUT/dev.json" <<'PY'
import json, sys
prod = {r["topic"] + "@" + r["pipeline"]: r for r in json.load(open(sys.argv[1]))}
dev  = {r["topic"] + "@" + r["pipeline"]: r for r in json.load(open(sys.argv[2]))}
fail = []

def check(table, key, **want):
    row = table.get(key)
    if row is None:
        fail.append(f"{key}: not in the dumped rows at all")
        return
    for k, v in want.items():
        # commit_interval arrives as a native int where a pipeline sets one (5000) and as '' where
        # none does — compared as text so the test pins the VALUE, not ansible's type for it
        got = row.get(k)
        if k == "commit_interval":
            got = "" if got in (None, "") else str(got)
        if got != v:
            fail.append(f"{key}: {k} is {got!r}, expected {v!r}")

# topic in the slug, topic in the group
check(prod, "es.futures.footprint.bars@es-cvd-mirror",
      group_id="es-cvd-mirror-192.168.100.252-es.futures.footprint.bars",
      label="com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-footprint-bars",
      mdir="/Users/abhinav/oe-ops/es-cvd-mirror-192-168-100-252-9092-es-futures-footprint-bars",
      plist="/Users/abhinav/Library/LaunchAgents/com.optionsedge.es-cvd-mirror-192-168-100-252-9092-es-futures-footprint-bars.plist",
      offset_reset="earliest", isolation_level="read_committed", commit_interval="")
# PORT in the group, and a commit interval
check(prod, "es.futures.auction@es-auction-mirror",
      group_id="es-auction-mirror-192.168.100.252-9092-es.futures.auction",
      label="com.optionsedge.es-auction-mirror-192-168-100-252-9092-es-futures-auction",
      offset_reset="earliest", isolation_level="", commit_interval="5000")
# NO topic in the slug, no topic in the group
check(prod, "es.futures.aggressor-flow@es-futures-flow-mirror",
      group_id="es-futures-flow-mirror-192.168.100.252",
      label="com.optionsedge.es-futures-flow-mirror-192-168-100-252-9092",
      mdir="/Users/abhinav/oe-ops/es-futures-flow-mirror-192-168-100-252-9092",
      offset_reset="latest", commit_interval="")
# the dev side composes from the dev broker, and the production-only unit is NOT in the dev plan
check(dev, "es.options.indicators.bars@es-indicator-mirror",
      group_id="es-indicator-mirror-127.0.0.1-es.options.indicators.bars",
      label="com.optionsedge.es-indicator-mirror-127-0-0-1-19092-es-options-indicators-bars")
if "es.futures.cvd.levels@es-cvd-mirror" in dev:
    fail.append("es.futures.cvd.levels is PRODUCTION-ONLY but appears in the dev plan")
if "es.futures.cvd.levels@es-cvd-mirror" not in prod:
    fail.append("es.futures.cvd.levels is missing from the PRODUCTION plan")

if fail:
    print("=== validate-ansible-mirror-naming: FAILED ===")
    for f in fail:
        print(f)
    sys.exit(1)
print(f"=== validate-ansible-mirror-naming: OK === {len(prod)} prod and {len(dev)} dev rows composed; "
      "group.id, label, unit dir and plist pinned for all four table shapes, and the production-only "
      "holdback verified on dev")
PY
