#!/usr/bin/env python3
"""Negative controls for fleet-readiness.py: every verdict is produced from a synthetic kubectl document."""
import json, os, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__)); CHECK = os.path.join(HERE, "fleet-readiness.py")
NOW = "2026-11-03T12:00:00Z"


def dep(name, want, have, since="2026-11-03T10:00:00Z", kind="Deployment", conds=True):
    st = {"availableReplicas": have, "updatedReplicas": want, "observedGeneration": 1} if have is not None else {}
    if conds:
        st["conditions"] = [{"type": "Available", "status": "True" if have and have >= want else "False", "lastTransitionTime": since}]
        if kind == "Deployment" and have and have >= want:
            # what a real, finished rollout carries
            st["conditions"].append({"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable", "lastUpdateTime": since, "lastTransitionTime": since})
    return {"kind": kind, "metadata": {"name": name, "generation": 1, "creationTimestamp": "2026-10-01T00:00:00Z"}, "spec": {"replicas": want}, "status": st}


def run(items, raw=None, minutes=30, state=None, now=NOW, expected=None):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write(raw if raw is not None else json.dumps({"items": items}))
    r = subprocess.run([sys.executable, CHECK, "--input", f.name, "--now", now, "--minutes", str(minutes)] + (["--state", state] if state else []) + (["--expected-unready", expected] if expected else []), capture_output=True, text=True)
    os.unlink(f.name); return r.returncode, r.stdout


fail = 0
def expect(name, rc_want, out_has, items=None, raw=None):
    global fail
    rc, out = run(items, raw)
    good = rc == rc_want and (out_has in out)
    print(("ok     " if good else "NOT-OK ") + name + ("" if good else " (rc=%d, out=%r)" % (rc, out)))
    fail |= 0 if good else 1


expect("all workloads available", 0, "0 stuck", [dep("a", 1, 1), dep("b", 2, 2)])
expect("held at 0 replicas is never reported, however old", 0, "1 held at 0", [dep("a", 1, 1), dep("held", 0, 0, since="2026-01-01T00:00:00Z")])
expect("the 9-hour crash-loop: wants 1, 0 available since 03:00", 1, "STUCK Deployment/eav", [dep("eav", 1, 0, since="2026-11-03T03:00:00Z")])
expect("short for under the threshold is tolerated (rollout in progress)", 0, "0 stuck", [dep("roll", 1, 0, since="2026-11-03T11:45:00Z")])
expect("exactly at the threshold is tolerated, one minute over is not", 1, "STUCK", [dep("edge", 1, 0, since="2026-11-03T11:29:00Z")])
expect("a StatefulSet counts too", 1, "StatefulSet/db", [dep("db", 1, 0, kind="StatefulSet", since="2026-11-03T05:00:00Z")])
expect("partly available (1 of 3) for hours is stuck", 1, "wants 3, 1 available", [dep("p", 3, 1, since="2026-11-03T01:00:00Z")])
expect("no conditions: falls back to creation time, long ago -> stuck", 1, "STUCK", [dep("n", 1, 0, conds=False)])
expect("an empty answer is an error, not 'all healthy'", 2, "ERROR", [])
expect("garbage input is an error", 2, "ERROR", raw="not json")
expect("an object with no status is an error", 2, "ERROR", [{"kind": "Deployment", "metadata": {"name": "x"}, "spec": {"replicas": 1}}])
def rollout(name, want, updated, prog_status, prog_reason, last="2026-11-03T05:00:00Z", gen=3, observed=3):
    d = dep(name, want, want, since="2026-11-03T01:00:00Z")
    d["metadata"]["generation"] = gen
    d["status"].update({"updatedReplicas": updated, "observedGeneration": observed})
    d["status"]["conditions"] = [c for c in d["status"]["conditions"] if c["type"] != "Progressing"]   # this helper supplies its own
    d["status"]["conditions"].append({"type": "Progressing", "status": prog_status, "reason": prog_reason, "lastUpdateTime": last, "lastTransitionTime": last})
    return d


expect("old pods still serving, rollout failed (ProgressDeadlineExceeded): STUCK", 1, "rollout failed", [rollout("bad", 1, 0, "False", "ProgressDeadlineExceeded")])
expect("old pods serving, new generation 0 of 1 updated for hours: STUCK", 1, "rollout not completing", [rollout("slow", 1, 0, "True", "ReplicaSetUpdated")])
expect("a rollout in progress for under the threshold is tolerated", 0, "0 stuck", [rollout("fresh", 1, 0, "True", "ReplicaSetUpdated", last="2026-11-03T11:50:00Z")])
expect("the new generation unobserved for hours is stuck", 1, "rollout not completing", [rollout("unobs", 1, 1, "True", "NewReplicaSetAvailable", gen=4, observed=3)])
expect("a completed rollout (updated == desired, generation observed) is healthy", 0, "1 healthy", [rollout("done", 2, 2, "True", "NewReplicaSetAvailable")])
expect("new pods updated == desired but never Ready, old pods still serving, ReplicaSetUpdated for hours: STUCK", 1, "rollout not completing", [rollout("surge", 2, 2, "True", "ReplicaSetUpdated")])
expect("the same state for under the threshold is a rollout in flight", 0, "0 stuck", [rollout("surge2", 2, 2, "True", "ReplicaSetUpdated", last="2026-11-03T11:55:00Z")])
nop = rollout("noprog", 1, 0, "True", "x"); nop["status"]["conditions"] = [c for c in nop["status"]["conditions"] if c["type"] != "Progressing"]
expect("lagging rollout with no Progressing condition fails closed", 1, "no Progressing timestamp", [nop])
def sts(name, want, ready, updated, gen=2, observed=2, cur="a", upd="b", strat=None):
    s = {"kind": "StatefulSet", "metadata": {"name": name, "generation": gen, "creationTimestamp": "2026-10-01T00:00:00Z"},
         "spec": {"replicas": want, "updateStrategy": strat or {"type": "RollingUpdate"}},
         "status": {"readyReplicas": ready, "availableReplicas": ready, "updatedReplicas": updated, "observedGeneration": observed, "currentRevision": cur, "updateRevision": upd}}
    return s


st_file = os.path.join(tempfile.mkdtemp(), "state.json")
def expect_run(name, rc_want, has, items, **kw):
    global fail
    rc, out = run(items, **kw); good = rc == rc_want and has in out
    print(("ok     " if good else "NOT-OK ") + name + ("" if good else " (rc=%d out=%r)" % (rc, out))); fail |= 0 if good else 1


expect("a StatefulSet with old pods Ready and the new generation unrolled, with no state to age it, fails closed", 1, "no Progressing timestamp", [sts("db", 1, 1, 0)])
expect_run("first sight of an incomplete StatefulSet rollout is tolerated (clock starts)", 0, "0 stuck", [sts("db", 1, 1, 0)], state=st_file, now="2026-11-03T12:00:00Z")
expect_run("...still tolerated 20 minutes later", 0, "0 stuck", [sts("db", 1, 1, 0)], state=st_file, now="2026-11-03T12:20:00Z")
expect_run("...stuck once it has been incomplete for more than the threshold", 1, "StatefulSet/db", [sts("db", 1, 1, 0)], state=st_file, now="2026-11-03T12:45:00Z")
expect_run("a completed StatefulSet rollout is healthy and forgets its clock", 0, "1 healthy", [sts("db", 1, 1, 1, cur="b", upd="b")], state=st_file, now="2026-11-03T13:00:00Z")
expect("a StatefulSet on OnDelete is partial by design, not stuck", 0, "1 healthy", [sts("od", 1, 1, 0, strat={"type": "OnDelete"})])
bad_state = os.path.join(tempfile.mkdtemp(), "bad.json"); open(bad_state, "w").write('{"StatefulSet/db@2": "2026-11-03T1')   # truncated write
expect_run("a corrupt state file fails closed instead of restarting every clock", 2, "unreadable", [sts("db", 1, 1, 0)], state=bad_state)
dang = os.path.join(tempfile.mkdtemp(), "dangling.json"); os.symlink("/nonexistent/target.json", dang)
expect_run("a dangling-symlink state path fails closed instead of reading as 'no state'", 2, "unreadable", [sts("db", 1, 1, 0)], state=dang)
lowav = dep("lowav", 2, 1, since="2026-11-03T11:50:00Z")
lowav["status"]["conditions"].append({"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded", "lastUpdateTime": "2026-11-03T11:50:00Z", "lastTransitionTime": "2026-11-03T11:50:00Z"})
expect("ProgressDeadlineExceeded is STUCK at once even when availability has already fallen below desired", 1, "rollout failed", [lowav])
def pod(app, phase="Running", running=True, restarts=0, n=1, owner="rs-vix-uid", labels=None):
    return {"kind": "Pod", "metadata": {"name": "%s-%d" % (app, n), "labels": labels or {"app.kubernetes.io/name": app}, "ownerReferences": [{"uid": owner, "controller": True}]},
            "status": {"phase": phase, "containerStatuses": [{"restartCount": restarts, "state": {"running": {}} if running else {"waiting": {"reason": "CrashLoopBackOff"}}}]}}


def rset(uid="rs-vix-uid", owner="dep-vix-uid", replicas=1, rev="3"):
    return {"kind": "ReplicaSet", "metadata": {"name": "rs", "uid": uid, "annotations": {"deployment.kubernetes.io/revision": rev}, "ownerReferences": [{"uid": owner, "controller": True}]}, "spec": {"replicas": replicas}}


exp = os.path.join(tempfile.mkdtemp(), "exp.json"); json.dump([{"kind": "Deployment", "name": "vix", "reason": "session window"}], open(exp, "w"))
def withsel(d, app):
    d["spec"]["selector"] = {"matchLabels": {"app.kubernetes.io/name": app}}; d["metadata"]["uid"] = "dep-%s-uid" % app
    d["metadata"].setdefault("annotations", {})["deployment.kubernetes.io/revision"] = "3"; return d


unready = withsel(dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), "vix")
expect_run("a by-design unready workload whose pod is Running is EXPECTED_UNREADY, not stuck", 0, "EXPECTED_UNREADY Deployment/vix", [unready, rset(), pod("vix")], expected=exp)
expect_run("...but a CrashLoopBackOff pod is still STUCK", 1, "STUCK Deployment/vix", [unready, rset(), pod("vix", running=False)], expected=exp)
expect_run("...and a restarting pod (3 restarts) is still STUCK", 1, "STUCK Deployment/vix", [unready, rset(), pod("vix", restarts=3)], expected=exp)
expect_run("...and a workload with no pods at all is still STUCK", 1, "STUCK Deployment/vix", [unready], expected=exp)
expect_run("a workload NOT on the list gets no exemption even with healthy pods", 1, "STUCK Deployment/other", [withsel(dep("other", 1, 0, since="2026-11-03T05:00:00Z"), "other"), rset("rs-other-uid", "dep-other-uid"), pod("other", owner="rs-other-uid")], expected=exp)
bad_exp = os.path.join(tempfile.mkdtemp(), "bad.json"); open(bad_exp, "w").write("[{not json")
expect_run("an unreadable expected-unready list fails closed", 2, "cannot read", [unready, rset(), pod("vix")], expected=bad_exp)
two = withsel(dep("vix", 2, 0, since="2026-11-03T05:00:00Z"), "vix")
expect_run("2 replicas wanted but only 1 healthy pod exists: the missing replica is not hidden by the exemption", 1, "STUCK Deployment/vix", [two, rset(replicas=2), pod("vix", n=1)], expected=exp)
expect_run("2 replicas wanted and 2 healthy pods: expected-unready", 0, "EXPECTED_UNREADY", [two, rset(replicas=2), pod("vix", n=1), pod("vix", n=2)], expected=exp)
other_wl = withsel(dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), "vix"); other_wl["spec"]["selector"]["matchLabels"]["component"] = "feed"
expect_run("a healthy pod that only shares the app label does not satisfy the workload's own selector", 1, "STUCK Deployment/vix", [other_wl, rset(), pod("vix")], expected=exp)
expect_run("no selector on the workload: nothing to bind pods to, so no exemption", 1, "STUCK Deployment/vix", [dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), pod("vix")], expected=exp)
expect_run("an input of only Pods is an error, not 'healthy'", 2, "no Deployment or StatefulSet", [pod("vix")], expected=exp)
expect_run("a healthy pod with the right labels but owned by a Job/manual controller is not the Deployment's pod", 1, "STUCK Deployment/vix", [unready, rset(), pod("vix", owner="some-job-uid")], expected=exp)
expr = withsel(dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), "vix"); expr["spec"]["selector"]["matchExpressions"] = [{"key": "track", "operator": "In", "values": ["stable"]}]
expect_run("a selector with matchExpressions cannot be resolved exactly: no exemption (fail closed)", 1, "STUCK Deployment/vix", [expr, rset(), pod("vix")], expected=exp)
expect_run("only an EMPTY (replicas 0) ReplicaSet owns the pod: not the current rollout", 1, "STUCK Deployment/vix", [unready, rset(replicas=0), pod("vix")], expected=exp)
p_ref = pod("vix"); p_ref["metadata"]["ownerReferences"] = [{"uid": "rs-vix-uid"}]    # a bare reference, not the controller
expect_run("a pod that merely REFERENCES the ReplicaSet (controller not true) is not its pod", 1, "STUCK Deployment/vix", [unready, rset(), p_ref], expected=exp)
expect_run("an OLD ReplicaSet kept during a rollout (other revision) cannot stand in for the current one", 1, "STUCK Deployment/vix", [unready, rset(rev="2"), pod("vix")], expected=exp)
expect_run("two ReplicaSets claiming the current revision: ambiguous, no exemption", 1, "STUCK Deployment/vix", [unready, rset(), rset(uid="rs2"), pod("vix")], expected=exp)
expect_run("the current ReplicaSet scaled differently from the Deployment: no exemption", 1, "STUCK Deployment/vix", [unready, rset(replicas=2), pod("vix")], expected=exp)
term = pod("vix"); term["metadata"]["deletionTimestamp"] = "2026-11-03T11:59:00Z"
expect_run("a TERMINATING pod (still phase Running) does not satisfy the exemption", 1, "STUCK Deployment/vix", [unready, rset(), term], expected=exp)
drift = pod("vix", n=2, running=False); drift["metadata"]["labels"] = {"app.kubernetes.io/name": "vix", "component": "drifted"}
sel2 = withsel(dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), "vix"); sel2["spec"]["selector"]["matchLabels"]["component"] = "feed"
ok_pod = pod("vix", n=1); ok_pod["metadata"]["labels"] = {"app.kubernetes.io/name": "vix", "component": "feed"}
expect_run("a controller-owned crash-looping pod whose labels drifted is NOT dropped from the count (it makes the exemption fail)", 1, "STUCK Deployment/vix", [sel2, rset(), ok_pod, drift], expected=exp)
expect_run("the same workload with only its correctly-labelled healthy pod is expected-unready", 0, "EXPECTED_UNREADY", [sel2, rset(), ok_pod], expected=exp)
gone = withsel(dep("vix", 1, 0, since="2026-11-03T05:00:00Z"), "vix"); gone["metadata"]["deletionTimestamp"] = "2026-11-03T11:00:00Z"
expect_run("a Deployment being deleted is not 'unready by design'", 1, "STUCK Deployment/vix", [gone, rset(), pod("vix")], expected=exp)
rs_gone = rset(); rs_gone["metadata"]["deletionTimestamp"] = "2026-11-03T11:00:00Z"
expect_run("a ReplicaSet being deleted is not the current ReplicaSet", 1, "STUCK Deployment/vix", [unready, rs_gone, pod("vix")], expected=exp)
sts_gone = sts("db", 1, 0, 0); sts_gone["metadata"].update({"uid": "sts-db-uid", "deletionTimestamp": "2026-11-03T11:00:00Z"}); sts_gone["spec"]["selector"] = {"matchLabels": {"app": "db"}}
json.dump([{"kind": "StatefulSet", "name": "db", "reason": "x"}], open(exp, "w"))
expect_run("a StatefulSet being deleted is not 'unready by design' either", 1, "STUCK StatefulSet/db", [sts_gone], expected=exp)
sys.exit(fail)
