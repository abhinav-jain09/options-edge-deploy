#!/usr/bin/env python3
"""Fleet readiness check (docs/schema-governance R18(b)): a Deployment or StatefulSet that WANTS replicas but has had fewer than that
available for longer than the threshold is a failure. Workloads held at 0 replicas (owner holds, shutdowns) are never reported.

  fleet-readiness.py --input kubectl-get-json [--now ISO] [--minutes 30]     (kubectl get deploy,statefulset -o json)

Fail closed: unparsable input, an object without a status, or no workloads at all is an error, not 'all healthy'.
Exit 0 = healthy, 1 = at least one stuck workload, 2 = the check itself could not run.
"""
import argparse, datetime as dt, json, os, sys

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--now", default=None)
ap.add_argument("--minutes", type=int, default=30)
ap.add_argument("--expected-unready", default=None, help="JSON list of {kind,name,reason}: workloads whose readiness is false BY DESIGN at times. They are exempt only while every pod "
                "of the workload (resolved through controller ownership) is Running with every container running and few restarts - a crash loop or an image pull failure is still reported")
ap.add_argument("--state", default=None, help="file remembering when an un-timestamped rollout (a StatefulSet's) was first seen incomplete; without it such a rollout fails closed")
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
seen = {}
if a.state and os.path.lexists(a.state):      # lexists: a dangling symlink EXISTS and is unreadable, it is not "no state yet"
    try:
        if not os.path.isfile(a.state): raise ValueError("not a readable regular file")
        seen = json.load(open(a.state))
        if not isinstance(seen, dict): raise ValueError("not an object")
    except Exception as e:  # noqa: BLE001
        # A corrupt clock must not be read as "first sight": that would hide a rollout that has already been incomplete. Fail closed (exit 2).
        print("ERROR: the state file %s is unreadable (%s) - refusing to restart every rollout clock; fix or remove it deliberately" % (a.state, e)); sys.exit(2)
new_seen = {}
stuck, held, ok, exempt = [], 0, 0, []
expected = {}
if a.expected_unready:
    try:
        for e in json.load(open(a.expected_unready)): expected[(e["kind"], e["name"])] = e["reason"]
    except Exception as e:  # noqa: BLE001
        print("ERROR: cannot read the expected-unready list %s: %s" % (a.expected_unready, e)); sys.exit(2)
pods = [it for it in items if it.get("kind") == "Pod"]
if not [it for it in items if it.get("kind") in ("Deployment", "StatefulSet")]:
    print("ERROR: the input holds no Deployment or StatefulSet (only %d other object(s)) - refusing to report 'healthy'" % len(items)); sys.exit(2)


rsets = [it for it in items if it.get("kind") == "ReplicaSet"]


def owned_by(obj, uid):
    """True only for the CONTROLLING owner reference (controller: true) with this uid: a bare reference does not make a pod this workload's."""
    return any(o.get("uid") == uid and o.get("controller") is True for o in obj.get("metadata", {}).get("ownerReferences") or [])


def pods_healthy(workload, want):
    """The workload's OWN pods - resolved through controller ownership (Deployment -> its CURRENT ReplicaSet -> Pods; StatefulSet -> Pods), not by labels
    alone, and only when its selector is plain matchLabels that the pod also satisfies - account for every desired replica and are all really running."""
    uid = workload.get("metadata", {}).get("uid")
    sel = workload.get("spec", {}).get("selector") or {}
    if not uid or not sel.get("matchLabels") or sel.get("matchExpressions"): return False     # fail closed on anything this cannot resolve exactly
    if workload.get("kind") == "Deployment":
        rev = (workload.get("metadata", {}).get("annotations") or {}).get("deployment.kubernetes.io/revision")
        if not rev: return False
        # the CURRENT ReplicaSet only (its revision annotation equals the Deployment's): an old one kept during a rollout must not stand in for it
        cur = [rs for rs in rsets if owned_by(rs, uid) and (rs.get("metadata", {}).get("annotations") or {}).get("deployment.kubernetes.io/revision") == rev and rs.get("metadata", {}).get("uid")]
        if len(cur) != 1 or (cur[0].get("spec", {}).get("replicas", 0) or 0) != want: return False
        mine = [p for p in pods if owned_by(p, cur[0]["metadata"]["uid"])]
    else:
        mine = [p for p in pods if owned_by(p, uid)]
    mine = [p for p in mine if all(p.get("metadata", {}).get("labels", {}).get(k) == v for k, v in sel["matchLabels"].items())]
    if len(mine) != want: return False
    for p in mine:
        if p.get("status", {}).get("phase") != "Running": return False
        cs = p.get("status", {}).get("containerStatuses") or []
        if not cs: return False
        for c in cs:
            if "running" not in (c.get("state") or {}) or (c.get("restartCount", 0) or 0) >= 3: return False
    return True


for it in items:
    if it.get("kind") in ("Pod", "ReplicaSet"): continue
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
    if kind == "Deployment":
        # a terminal rollout failure declared by Kubernetes itself is never "temporarily healthy", whatever the availability is right now
        _p = next((c for c in (st.get("conditions") or []) if c.get("type") == "Progressing"), None)
        if _p and _p.get("status") == "False" and _p.get("reason") == "ProgressDeadlineExceeded":
            stuck.append((kind, name, want, have, -1)); continue
    if kind == "Deployment" and have >= want:
        conds = st.get("conditions") or []
        prog = next((c for c in conds if c.get("type") == "Progressing"), None)
        updated_all = (st.get("updatedReplicas", 0) or 0) >= want
        observed = (st.get("observedGeneration", 0) or 0) >= it.get("metadata", {}).get("generation", 0)
        # A rollout is COMPLETE only when Kubernetes says so: Progressing=True with reason NewReplicaSetAvailable, every replica updated, the generation observed.
        # Anything else is a rollout in flight, and it is aged from the Progressing condition: old pods keeping `available` at the desired count must not
        # make a half-done (or stuck, e.g. new pods updated but never Ready) rollout look healthy for as long as the progress deadline allows.
        complete = bool(prog) and prog.get("status") == "True" and prog.get("reason") == "NewReplicaSetAvailable" and updated_all and observed
        if prog and prog.get("status") == "False" and prog.get("reason") == "ProgressDeadlineExceeded":
            stuck.append((kind, name, want, have, -1)); continue
        if not complete:
            when = (prog.get("lastUpdateTime") or prog.get("lastTransitionTime")) if prog else None
            if when is None:
                stuck.append((kind, name, want, have, -2)); continue      # cannot be aged, so cannot be called healthy (fail closed)
            mins_lag = (now - ts(when)).total_seconds() / 60
            if mins_lag > a.minutes:
                stuck.append((kind, name, want, have, int(mins_lag))); continue
    if kind == "StatefulSet" and have >= want:
        # A StatefulSet carries no Progressing condition, so a rollout cannot be aged from the object: it is complete when its generation is observed and every
        # pod is at the update revision (or the update strategy is OnDelete / a partition, where a partial state is the design). An incomplete one is aged
        # from the first run that saw it incomplete (--state); with no state it cannot be aged and fails closed.
        spec = it.get("spec", {})
        strat = spec.get("updateStrategy", {})
        partial_by_design = strat.get("type") == "OnDelete" or ((strat.get("rollingUpdate") or {}).get("partition", 0) or 0) > 0
        observed = (st.get("observedGeneration", 0) or 0) >= it.get("metadata", {}).get("generation", 0)
        at_update = (st.get("updatedReplicas", 0) or 0) >= want or (st.get("currentRevision") and st.get("currentRevision") == st.get("updateRevision"))
        if not (observed and (at_update or partial_by_design)):
            if not a.state:
                stuck.append((kind, name, want, have, -2)); continue
            key = "%s/%s@%s" % (kind, name, it.get("metadata", {}).get("generation", 0))
            first = ts(seen[key]) if key in seen else now
            new_seen[key] = first.strftime("%Y-%m-%dT%H:%M:%SZ")
            mins_lag = (now - first).total_seconds() / 60
            if mins_lag > a.minutes:
                stuck.append((kind, name, want, have, int(mins_lag))); continue
            ok += 1; continue
    if have >= want:
        ok += 1; continue
    if (kind, name) in expected and pods_healthy(it, want):
        exempt.append((kind, name, expected[(kind, name)])); continue
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
if a.state:
    os.makedirs(os.path.dirname(os.path.abspath(a.state)), exist_ok=True)
    tmp = a.state + ".tmp"
    with open(tmp, "w") as fh: json.dump(new_seen, fh)
    os.replace(tmp, a.state)   # atomic: an interrupted write can never leave a truncated clock; only the still-incomplete rollouts survive
for kind, name, want, have, mins in sorted(stuck):
    if mins == -2: print("STUCK %s/%s: the new generation is not fully rolled out and there is no Progressing timestamp to age it (%d old pods serving)" % (kind, name, have))
    elif mins < 0: print("STUCK %s/%s: the rollout failed (ProgressDeadlineExceeded) while %d of %d old pods keep serving" % (kind, name, have, want))
    elif have >= want: print("STUCK %s/%s: rollout not completing for %d min (new generation not fully updated) while %d old pods keep serving" % (kind, name, mins, have))
    else: print("STUCK %s/%s wants %d, %d available, short for %d min" % (kind, name, want, have, mins))
for kind, name, why in sorted(exempt):
    print("EXPECTED_UNREADY %s/%s (pods Running, not crash-looping): %s" % (kind, name, why[:140]))
print("fleet-readiness: %d healthy, %d held at 0, %d expected-unready, %d stuck (> %d min)" % (ok, held, len(exempt), len(stuck), a.minutes))
sys.exit(1 if stuck else 0)
