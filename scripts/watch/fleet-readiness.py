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
    # A ROLLOUT that is not completing hides behind the old pods: the old ReplicaSet keeps `available` at the desired count while the new one crash-loops.
    # So a Deployment is also stuck when its Progressing condition says the deadline passed, or when the new generation has not been observed / is not fully
    # updated for longer than the threshold (counted from the Progressing condition's last update).
    if kind == "Deployment" and have >= want:
        conds = st.get("conditions") or []
        prog = next((c for c in conds if c.get("type") == "Progressing"), None)
        lag = (st.get("updatedReplicas", 0) or 0) < want or (st.get("observedGeneration", 0) or 0) < it.get("metadata", {}).get("generation", 0)
        if prog and prog.get("status") == "False" and prog.get("reason") == "ProgressDeadlineExceeded":
            stuck.append((kind, name, want, have, -1)); continue
        if lag:
            when = (prog.get("lastUpdateTime") or prog.get("lastTransitionTime")) if prog else None
            if when is None:
                # a rollout that has not caught up and carries NO progress timestamp cannot be aged, so it cannot be called healthy (fail closed)
                stuck.append((kind, name, want, have, -2)); continue
            mins_lag = (now - ts(when)).total_seconds() / 60
            if mins_lag > a.minutes:
                stuck.append((kind, name, want, have, int(mins_lag))); continue
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
    if mins == -2: print("STUCK %s/%s: the new generation is not fully rolled out and there is no Progressing timestamp to age it (%d old pods serving)" % (kind, name, have))
    elif mins < 0: print("STUCK %s/%s: the rollout failed (ProgressDeadlineExceeded) while %d of %d old pods keep serving" % (kind, name, have, want))
    elif have >= want: print("STUCK %s/%s: rollout not completing for %d min (new generation not fully updated) while %d old pods keep serving" % (kind, name, mins, have))
    else: print("STUCK %s/%s wants %d, %d available, short for %d min" % (kind, name, want, have, mins))
print("fleet-readiness: %d healthy, %d held at 0, %d stuck (> %d min)" % (ok, held, len(stuck), a.minutes))
sys.exit(1 if stuck else 0)
