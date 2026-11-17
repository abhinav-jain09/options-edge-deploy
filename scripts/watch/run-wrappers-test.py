#!/usr/bin/env python3
"""Controls for readiness-run.sh, oom-run.sh and host-load-run.sh, driven by a FAKE kubectl on PATH.

The point of each wrapper is that a failure to fetch is an ordinary exit 2 INSIDE the wrapped command, so fleet-alert-deliver.sh can say
"could not check" in Discord. The last cases run the whole path (wrapper -> check -> composer -> alert()) with a stub delivery function.
"""
import json
import os
import stat
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
fails = []
BIN = tempfile.mkdtemp(prefix="oe-fakebin-")
STATE = tempfile.mkdtemp(prefix="oe-state-")
SENT = os.path.join(STATE, "sent.txt")

KUBECTL = r'''#!/usr/bin/env bash
# fake kubectl: behaviour chosen by FAKE_* env vars; fixtures by FAKE_DIR
args="$*"
case "$args" in
  *"get deploy,statefulset,rs,pods"*) [ -n "${FAKE_FAIL_WORKLOADS:-}" ] && { echo "error: connection refused" >&2; exit 1; }; cat "$FAKE_DIR/workloads.json" ;;
  *"get pods"*) [ -n "${FAKE_FAIL_PODS:-}" ] && { echo "error: connection refused" >&2; exit 1; }; cat "$FAKE_DIR/pods.json" ;;
  *"get --raw /apis/metrics.k8s.io"*) [ -f "$FAKE_DIR/metrics.json" ] && cat "$FAKE_DIR/metrics.json" || { echo "not found" >&2; exit 1; } ;;
  *"get nodes -o json"*) [ -n "${FAKE_FORBID_NODES:-}" ] && { echo 'Error from server (Forbidden): nodes is forbidden: User "system:serviceaccount:options-edge:jenkins-deployer" cannot list resource "nodes"' >&2; exit 1; }; cat "$FAKE_DIR/nodes.json" ;;
  *) echo "fake kubectl: unexpected $args" >&2; exit 9 ;;
esac
'''
open(os.path.join(BIN, "kubectl"), "w").write(KUBECTL)
os.chmod(os.path.join(BIN, "kubectl"), 0o755)
STUB = os.path.join(STATE, "alert-stub.sh")
open(STUB, "w").write('alert() { printf "%s\\n---\\n" "$1" >> "' + SENT + '"; return 0; }\n')


def fx(**docs):
    d = tempfile.mkdtemp(prefix="oe-fx-")
    for n, doc in docs.items():
        json.dump(doc, open(os.path.join(d, n + ".json"), "w"))
    return d


def run(cmd, d, **env):
    e = dict(os.environ, PATH=BIN + os.pathsep + os.environ["PATH"], FAKE_DIR=d, FLEET_STATE_DIR=STATE, **env)
    r = subprocess.run(cmd, capture_output=True, text=True, env=e, timeout=60)
    return r.returncode, r.stdout + r.stderr


def check(name, rc, out, want_rc, expect=None, forbid=None):
    why = []
    if rc != want_rc:
        why.append("exit %d, wanted %d" % (rc, want_rc))
    if expect and expect not in out:
        why.append("missing %r" % expect)
    if forbid and forbid in out:
        why.append("contains %r" % forbid)
    if why:
        fails.append(name)
        print("  FAIL %-66s %s\n       %s" % (name, "; ".join(why), out.strip()[:400].replace("\n", "\n       ")))
    else:
        print("  ok   %-66s" % name)


W = lambda f: os.path.join(HERE, f)
STUCK_DEPLOY = {"kind": "Deployment", "metadata": {"name": "level-absorption", "generation": 1, "uid": "u1", "creationTimestamp": "2026-11-01T00:00:00Z"},
                "spec": {"replicas": 1}, "status": {"availableReplicas": 0, "conditions": [
                    {"type": "Available", "status": "False", "lastTransitionTime": "2026-11-01T00:00:00Z"}]}}
HEALTHY_DEPLOY = {"kind": "Deployment", "metadata": {"name": "ok", "generation": 1, "uid": "u2", "creationTimestamp": "2026-11-01T00:00:00Z"},
                  "spec": {"replicas": 1}, "status": {"availableReplicas": 1, "observedGeneration": 1, "updatedReplicas": 1, "conditions": [
                      {"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable", "lastUpdateTime": "2026-11-01T00:00:00Z"}]}}

print("wrapper controls")
print(" readiness-run.sh")
d = fx(workloads={"items": [STUCK_DEPLOY, HEALTHY_DEPLOY]})
rc, out = run(["bash", W("readiness-run.sh")], d, FLEET_ENV="dev")
check("a stuck workload is exit 1 and named", rc, out, 1, expect="STUCK Deployment/level-absorption")
rc, out = run(["bash", W("readiness-run.sh")], fx(workloads={"items": [HEALTHY_DEPLOY]}), FLEET_ENV="dev")
check("a healthy fleet is exit 0", rc, out, 0, expect="0 stuck")
rc, out = run(["bash", W("readiness-run.sh")], d, FLEET_ENV="dev", FAKE_FAIL_WORKLOADS="1")
check("a kubectl failure is exit 2 with an ERROR: reason", rc, out, 2, expect="ERROR: kubectl -n options-edge get deploy,statefulset,rs,pods failed: error: connection refused")
rc, out = run(["bash", W("readiness-run.sh")], d, FLEET_ENV="dev", STUCK_MINUTES="soon")
check("a non-numeric STUCK_MINUTES is exit 2", rc, out, 2, expect="ERROR: STUCK_MINUTES")
e = dict(os.environ, PATH=BIN + os.pathsep + os.environ["PATH"], FAKE_DIR=d, FLEET_ENV="dev")
e.pop("FLEET_STATE_DIR", None); e.pop("WORKSPACE", None)
r = subprocess.run(["bash", W("readiness-run.sh")], capture_output=True, text=True, env=e)
check("no state directory and no WORKSPACE is exit 2, not a silent default", r.returncode, r.stdout + r.stderr, 2, expect="cannot locate")

print(" oom-run.sh")
OOMPOD = {"metadata": {"name": "gex-0", "creationTimestamp": "2026-11-17T00:00:00Z"}, "spec": {"containers": [{"name": "gex", "resources": {"limits": {"memory": "6Gi"}}}]},
          "status": {"phase": "Running", "containerStatuses": [{"name": "gex", "restartCount": 2, "state": {"running": {"startedAt": "2026-11-17T11:00:00Z"}},
                                                               "lastState": {"terminated": {"reason": "OOMKilled", "finishedAt": "2026-11-17T11:55:00Z"}}}]}}
rc, out = run(["bash", W("oom-run.sh"), "--now", "2026-11-17T12:00:00Z"], fx(pods={"items": [OOMPOD]}))
check("a fresh OOMKilled is exit 1 and named", rc, out, 1, expect="OOM_KILLED gex-0")
rc, out = run(["bash", W("oom-run.sh")], fx(pods={"items": [OOMPOD]}), FAKE_FAIL_PODS="1")
check("a kubectl failure is exit 2 with an ERROR: reason", rc, out, 2, expect="ERROR: kubectl -n options-edge get pods failed")

print(" host-load-run.sh")
NODE = {"metadata": {"name": "n1"}, "status": {"conditions": [{"type": "Ready", "status": "True"}], "allocatable": {"cpu": "16", "memory": "80000000Ki"}}}
MET = {"items": [{"metadata": {"name": "n1"}, "usage": {"cpu": "1", "memory": "75000000Ki"}}]}
rc, out = run(["bash", W("host-load-run.sh")], fx(nodes={"items": [NODE]}, metrics=MET))
check("memory over the threshold through the metrics API is exit 1", rc, out, 1, expect="HOST_MEMORY n1")
rc, out = run(["bash", W("host-load-run.sh")], fx(nodes={"items": [NODE]}))
check("no metrics API: exit 0, and it SAYS memory/cpu were not measured", rc, out, 0, expect="CONDITIONS ONLY")
rc, out = run(["bash", W("host-load-run.sh")], fx(nodes={"items": [NODE]}), FAKE_FORBID_NODES="1")
check("Forbidden nodes is exit 2 naming the missing permission", rc, out, 2, expect="ERROR: kubectl get nodes failed: Error from server (Forbidden)")

print(" whole path: wrapper -> check -> composer -> alert()")
DELIVER = W("fleet-alert-deliver.sh")
deliver_env = dict(FLEET_ENV="dev", FLEET_MODE="paging", ALERT_SH=STUB)
open(SENT, "w").close()
rc, out = run(["bash", DELIVER, "bash", W("readiness-run.sh")], d, FLEET_CHECK="readiness", **deliver_env)
sent = open(SENT).read()
check("a stuck workload reaches alert() as a STUCK finding", rc, sent, 1, expect="`STUCK` Deployment/level-absorption")
open(SENT, "w").close()
rc, out = run(["bash", DELIVER, "bash", W("readiness-run.sh")], d, FLEET_CHECK="readiness", **deliver_env)
check("the second run of the same incident sends nothing", rc, open(SENT).read(), 1, forbid="STUCK")
open(SENT, "w").close()
rc, out = run(["bash", DELIVER, "bash", W("host-load-run.sh")], fx(nodes={"items": [NODE]}), FLEET_CHECK="host-load", FAKE_FORBID_NODES="1", **deliver_env)
check("a Forbidden nodes read is DELIVERED as 'could not check'", rc, open(SENT).read(), 2, expect="could not check")
open(SENT, "w").close()
rc, out = run(["bash", DELIVER, "bash", W("oom-run.sh"), "--now", "2026-11-17T12:00:00Z"], fx(pods={"items": [OOMPOD]}), FLEET_CHECK="oom", **deliver_env)
check("an OOM kill reaches alert()", rc, open(SENT).read(), 1, expect="`OOM_KILLED` gex-0")

print("\n%d case(s) FAILED" % len(fails) if fails else "\nall cases ok")
sys.exit(1 if fails else 0)
