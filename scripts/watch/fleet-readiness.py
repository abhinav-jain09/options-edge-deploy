#!/usr/bin/env python3
"""Fleet readiness check (docs/schema-governance R18(b)): a Deployment or StatefulSet that WANTS replicas but has had fewer than that
available for longer than the threshold is a failure. Workloads held at 0 replicas (owner holds, shutdowns) are never reported.

  fleet-readiness.py --input kubectl-get-json [--now ISO] [--minutes 30]     (kubectl get deploy,statefulset -o json)

Fail closed: unparsable input, an object without a status, or no workloads at all is an error, not 'all healthy'.
Exit 0 = healthy, 1 = at least one stuck workload, 2 = the check itself could not run.
"""
import argparse, datetime as dt, json, sys

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--now", default=None)
ap.add_argument("--minutes", type=int, default=30)
a = ap.parse_args()


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


try:
    data = json.load(open(a.input))
    items = data["items"]
except Exception as e:  # noqa: BLE001
    print("ERROR: cannot read kubectl output: %s" % e); sys.exit(2)
if not items:
    print("ERROR: no Deployments or StatefulSets in the input - refusing to report 'healthy' on an empty answer"); sys.exit(2)
now = ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)
stuck, held, ok = [], 0, 0
for it in items:
    kind, name = it.get("kind"), it.get("metadata", {}).get("name", "?")
    want = it.get("spec", {}).get("replicas")
    st = it.get("status")
    if want is None or st is None:
        print("ERROR: %s/%s has no spec.replicas or status - cannot judge it" % (kind, name)); sys.exit(2)
    if want == 0:
        held += 1; continue
    have = st.get("availableReplicas", st.get("readyReplicas", 0)) or 0
    if have >= want:
        ok += 1; continue
    # how long has it been short? the Available condition's transition time (Deployments), else the newest condition, else creation
    conds = st.get("conditions") or []
    since = None
    for c in conds:
        if c.get("type") == "Available" and c.get("status") == "False" and c.get("lastTransitionTime"):
            since = ts(c["lastTransitionTime"])
    if since is None and conds:
        since = max(ts(c["lastTransitionTime"]) for c in conds if c.get("lastTransitionTime")) if any(c.get("lastTransitionTime") for c in conds) else None
    if since is None:
        since = ts(it["metadata"]["creationTimestamp"])
    mins = (now - since).total_seconds() / 60
    if mins > a.minutes:
        stuck.append((kind, name, want, have, int(mins)))
for kind, name, want, have, mins in sorted(stuck):
    print("STUCK %s/%s wants %d, %d available, short for %d min" % (kind, name, want, have, mins))
print("fleet-readiness: %d healthy, %d held at 0, %d stuck (> %d min)" % (ok, held, len(stuck), a.minutes))
sys.exit(1 if stuck else 0)
