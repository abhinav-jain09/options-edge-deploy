#!/usr/bin/env bash
# deploy-snapshot.sh - a READ-ONLY before/after record of every Deployment and StatefulSet in a namespace, and the comparison that proves a roll touched ONE workload.
#
# Written for the liquidity-validator-service roll (options-edge-documents design/geometry-trade-rules/LIQUIDITY-VALIDATOR-SERVICE-REQUIREMENT.md section 7.3): the EAV overnight segment is live
# on dev, and a new-service roll must be shown not to have restarted, re-rolled or re-pointed anything else.
#
#   capture [OUTFILE]                one `kubectl get deploy,sts,pods -n $NAMESPACE -o json` (the ONLY cluster call, a `get`), summarised to one sorted line per workload
#   summarize                        the same summary from a `kubectl get ... -o json` document on stdin (no cluster call; what the tests use)
#   compare BEFORE AFTER [ALLOWED]   exit 0 only when every workload other than ALLOWED (default liquidity-validator-service) is byte-identical in both files and none appeared or vanished
#
# Line: <Kind>/<name> TAB generation=<n> TAB observedGeneration=<n> TAB images=<spec images, sorted, comma> TAB pods=<uid:restartCountSum:imageIDs, sorted, semicolon>
# A pod belongs to a workload when every key=value of the workload's spec.selector.matchLabels is on the pod. Environment: NAMESPACE (default options-edge), KUBECTL (default kubectl);
# the kubeconfig is whatever the caller exported - this script never chooses a cluster.
set -euo pipefail

NAMESPACE="${NAMESPACE:-options-edge}"
KUBECTL="${KUBECTL:-kubectl}"
MODE="${1:-}"

summarize() {
  python3 -c '
import json, sys
doc = json.load(sys.stdin)
items = doc.get("items", [doc] if doc.get("kind") not in (None, "List") else [])
pods = [i for i in items if i.get("kind") == "Pod"]
lines = {}
for w in items:
    kind = w.get("kind")
    if kind not in ("Deployment", "StatefulSet"):
        continue
    name = w["metadata"]["name"]
    sel = (w.get("spec", {}).get("selector") or {}).get("matchLabels") or {}
    images = sorted(c.get("image", "") for c in w["spec"]["template"]["spec"].get("containers", []))
    mine = []
    for p in pods:
        labels = p.get("metadata", {}).get("labels") or {}
        if sel and all(labels.get(k) == v for k, v in sel.items()):
            st = p.get("status", {}).get("containerStatuses") or []
            restarts = sum(int(c.get("restartCount", 0)) for c in st)
            ids = ",".join(sorted(c.get("imageID", "") for c in st))
            mine.append("%s:%d:%s" % (p["metadata"]["uid"], restarts, ids))
    key = "%s/%s" % (kind, name)
    lines[key] = "\t".join([key, "generation=%s" % w["metadata"].get("generation", ""), "observedGeneration=%s" % w.get("status", {}).get("observedGeneration", ""),
                            "images=" + ",".join(images), "pods=" + ";".join(sorted(mine))])
for k in sorted(lines):
    print(lines[k])
'
}

compare() {
  local before="$1" after="$2" allowed="${3:-liquidity-validator-service}"
  [ -r "$before" ] && [ -r "$after" ] || { echo "FAIL: both snapshot files must be readable ($before, $after)" >&2; return 2; }
  python3 - "$before" "$after" "$allowed" <<'PY'
import sys
def load(path):
    d = {}
    for line in open(path):
        line = line.rstrip("\n")
        if line:
            d[line.split("\t", 1)[0]] = line
    return d
b, a, allowed = load(sys.argv[1]), load(sys.argv[2]), sys.argv[3]
if not b or not a:
    print("FAIL: an empty snapshot proves nothing (before=%d workloads, after=%d)" % (len(b), len(a)))
    sys.exit(1)
bad = []
for key in sorted(set(b) | set(a)):
    if key.split("/", 1)[1] == allowed:
        continue
    if key not in b:
        bad.append("%s appeared" % key)
    elif key not in a:
        bad.append("%s vanished" % key)
    elif b[key] != a[key]:
        bad.append("%s changed:\n    before %s\n    after  %s" % (key, b[key].split("\t", 1)[1], a[key].split("\t", 1)[1]))
if bad:
    print("FAIL: workloads other than %s differ between the snapshots:" % allowed)
    for x in bad:
        print("  " + x)
    sys.exit(1)
print("OK: %d workloads compared, only %s may differ (%s before, %s after)" % (len(set(b) | set(a)) - (1 if any(k.endswith("/" + allowed) for k in set(b) | set(a)) else 0), allowed,
      "present" if any(k.endswith("/" + allowed) for k in b) else "absent", "present" if any(k.endswith("/" + allowed) for k in a) else "absent"))
PY
}

case "$MODE" in
  capture)
    out="${2:-}"
    raw="$("$KUBECTL" get deploy,sts,pods -n "$NAMESPACE" -o json)"
    if [ -n "$out" ]; then printf '%s' "$raw" | summarize >"$out"; echo "snapshot written: $out ($(wc -l <"$out" | tr -d ' ') workloads)"; else printf '%s' "$raw" | summarize; fi
    ;;
  summarize) summarize ;;
  compare) shift; compare "$@" ;;
  *) echo "usage: $0 capture [OUTFILE] | summarize | compare BEFORE AFTER [ALLOWED_WORKLOAD]" >&2; exit 2 ;;
esac
