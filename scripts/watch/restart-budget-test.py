#!/usr/bin/env python3
"""Negative controls for restart-budget.py, then REAL mutations of it (each one must make this suite fail).

  python3 scripts/watch/restart-budget-test.py            # controls, then the mutation campaign
  RB_CHECK=/path/to/mutant.py python3 ... --controls-only # what the campaign runs: only the controls, against another copy

The fixtures are shaped like `kubectl get pods -o json` / `get deployments -o json`. kubectl itself is replaced by a stub that records its arguments and
can be told to fail, so the cluster writes (annotate, scale) are asserted on exactly: which calls, in which order, with which replicas.
"""
import datetime as dt
import json
import os
import stat
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", ".."))
CHECK = os.environ.get("RB_CHECK") or os.path.join(HERE, "restart-budget.py")
MANDATORY = os.path.join(REPO, "config", "prod-mandatory-services.txt")
NOW = dt.datetime(2026, 11, 19, 12, 0, 0, tzinfo=dt.timezone.utc)
fails = []


def iso(minutes_ago, now=NOW):
    return (now - dt.timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def pod(name, dep="svc-a", restarts=0, age=600, term_ago=None, reason="OOMKilled", uid=None, kind="ReplicaSet", owner=None,
        phase="Running", status_reason=None, init_restarts=None, start_ago=None, now=NOW):
    owner = owner or (dep + "-6d7f9c" if kind == "ReplicaSet" else dep)
    cs = {"name": "main", "restartCount": restarts, "state": {"running": {"startedAt": iso(5, now)}}}
    if term_ago is not None:
        cs["lastState"] = {"terminated": {"reason": reason, "exitCode": 137, "finishedAt": iso(term_ago, now)}}
    status = {"phase": phase, "containerStatuses": [cs]}
    if init_restarts is not None:
        status["initContainerStatuses"] = [{"name": "init", "restartCount": init_restarts}]
    if status_reason:
        status["reason"] = status_reason
    if start_ago is not None:
        status["startTime"] = iso(start_ago, now)
    return {"metadata": {"name": name, "uid": uid or name, "creationTimestamp": iso(age, now),
                         "ownerReferences": [{"kind": kind, "name": owner}]}, "status": status}


def deploy(name, replicas=1, annotations=None):
    return {"metadata": {"name": name, "annotations": annotations or {}}, "spec": {"replicas": replicas}}


def stub_kubectl(d):
    p = os.path.join(d, "kubectl-stub")
    open(p, "w").write('#!/usr/bin/env bash\necho "$*" >> "$STUB_CALLS"\n'
                       'case "$*" in\n'
                       '  *" scale "*) [ "${STUB_FAIL:-}" = scale ] && { echo "scale denied" >&2; exit 1; } ;;\n'
                       '  *" annotate "*--overwrite*) [ "${STUB_FAIL:-}" = annotate ] && { echo "annotate denied" >&2; exit 1; } ;;\n'
                       'esac\nexit 0\n')
    os.chmod(p, os.stat(p).st_mode | stat.S_IEXEC)
    return p


class Env:
    """One state directory shared by the runs of a scenario, so deltas between runs are real."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        self.state = os.path.join(self.d, "state")
        self.kubectl = stub_kubectl(self.d)
        self.calls = os.path.join(self.d, "calls.txt")
        self.n = 0

    def run(self, pods, deploys, now=NOW, mode="enforce", env="dev", extra=(), fail="", state=None, mandatory=MANDATORY, raw_pods=None):
        self.n += 1
        pf, df = os.path.join(self.d, "pods%d.json" % self.n), os.path.join(self.d, "deploys%d.json" % self.n)
        open(pf, "w").write(raw_pods if raw_pods is not None else json.dumps({"items": pods}))
        open(df, "w").write(json.dumps({"items": deploys}))
        open(self.calls, "w").close()
        e = dict(os.environ, STUB_CALLS=self.calls, STUB_FAIL=fail)
        r = subprocess.run([sys.executable, CHECK, "--env", env, "--pods", pf, "--deploys", df, "--state", state or self.state,
                            "--mode", mode, "--kubectl", self.kubectl, "--mandatory", mandatory,
                            "--now", now.strftime("%Y-%m-%dT%H:%M:%SZ"), *extra],
                           capture_output=True, text=True, timeout=60, env=e)
        calls = [l.strip() for l in open(self.calls).read().splitlines() if l.strip()]
        return r.returncode, r.stdout + r.stderr, calls

    def state_json(self, env="dev"):
        return json.load(open(os.path.join(self.state, "restart-budget-%s.json" % env)))


def check(name, ok, detail=""):
    print(("  ok   " if ok else "  FAIL ") + name + ("" if ok else "  " + detail[:500]))
    if not ok:
        fails.append(name)


def later(minutes):
    return NOW + dt.timedelta(minutes=minutes)


def controls():
    print("restart-budget.py negative controls (%s)" % os.path.basename(CHECK))

    # ---- the 5-restart boundary ----
    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=0, age=600)], [deploy("svc-a")])
    check("a quiet fleet is clean (exit 0, no writes)", rc == 0 and not calls and "0 over budget" in out, out)

    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=5, age=30, term_ago=2)], [deploy("svc-a")])
    check("exactly 5 restarts in the hour is allowed", rc == 0 and not calls and "RESTART_BUDGET" not in out, out)

    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=6, age=30, term_ago=2)], [deploy("svc-a")])
    check("the 6th restart stops the deployment: RESTART_BUDGET ... STOPPED just now, exit 1", rc == 1 and
          "RESTART_BUDGET svc-a: 6 restarts in 60 min (limit 5), last OOMKilled (exit 137); STOPPED just now" in out, out)
    check("... and the writes are annotate THEN scale --replicas=0, nothing else", len(calls) == 2 and " annotate deploy/svc-a --overwrite " in " " + calls[0]
          and "options-edge.io/quarantined-at=" in calls[0] and "options-edge.io/quarantine-prev-replicas=1" in calls[0]
          and calls[1] == "-n options-edge scale deploy/svc-a --replicas=0", "\n".join(calls))
    check("... the page names the one command that clears it", "scripts/ops/quarantine-clear.sh dev svc-a" in out, out)
    check("... and the stop clears the deployment's restart history (clearing it must not re-stop on the old restarts)",
          "svc-a" not in e.state_json()["events"], json.dumps(e.state_json()))

    # ---- cumulative counts are history, not a rate ----
    e = Env()
    rc, out, calls = e.run([pod("db-0", dep="svc-b", restarts=35, age=60 * 24 * 93, term_ago=60 * 24 * 3)], [deploy("svc-b")])
    check("a 93-day-old pod with 35 old restarts is NOT stopped at first sight (the bo-app-postgres case)", rc == 0 and not calls, out)

    e = Env()
    rc, out, calls = e.run([pod("la-1", dep="level-absorption-service", restarts=220, age=60 * 109, term_ago=20)], [deploy("level-absorption-service")])
    check("level-absorption (220 restarts in 4.5 days, last 20 min ago) is stopped on the 24 h budget, and the page says 'about'", rc == 1 and
          "about 48 restarts in 24 h (limit 5)" in out and "STOPPED just now" in out, out)
    check("... an hour budget alone would not have caught it (that is why there are two windows)", "restarts in 60 min" not in out, out)

    e = Env()
    rc, out, calls = e.run([pod("feed-1", dep="es-feed", restarts=149, age=60 * 422, term_ago=60 * 18 + 30)], [deploy("es-feed")])
    check("es4 es-feed (149 restarts in 17.6 days, the LAST 18.5 h ago) is NOT stopped: the burst is over, the pod is not looping now", rc == 0 and not calls, out)
    e = Env()
    rc, out, calls = e.run([pod("mir-1", dep="rithmic-depth-mirror-production", restarts=1433, age=60 * 390, term_ago=60 * 16)], [deploy("rithmic-depth-mirror-production")])
    check("... nor a pod with 1,433 old restarts whose last one was 16 h ago (an average interval of 16 minutes, silent since)", rc == 0 and not calls, out)

    # ---- counting across runs ----
    e = Env()
    p0 = pod("c-1", dep="svc-c", restarts=2, age=600, term_ago=500)
    rc, out, calls = e.run([p0], [deploy("svc-c")], now=NOW)
    check("run 1: an old pod with 2 old restarts only sets the baseline", rc == 0 and not calls, out)
    rc, out, calls = e.run([pod("c-1", dep="svc-c", restarts=5, age=615, term_ago=2, now=later(15))], [deploy("svc-c")], now=later(15))
    check("run 2 (+15 min): 3 new restarts is within budget", rc == 0 and not calls, out)
    rc, out, calls = e.run([pod("c-1", dep="svc-c", restarts=8, age=630, term_ago=2, now=later(30))], [deploy("svc-c")], now=later(30))
    check("run 3 (+30 min): 3 more makes 6 inside the hour -> stopped, from DELTAS not the cumulative 8", rc == 1 and "6 restarts in 60 min" in out and len(calls) == 2, out + str(calls))

    e = Env()
    e.run([pod("d-1", dep="svc-d", restarts=0, age=600)], [deploy("svc-d")], now=NOW)
    e.run([pod("d-1", dep="svc-d", restarts=4, age=610, term_ago=2, now=later(10))], [deploy("svc-d")], now=later(10), extra=["--day-limit", "100"])
    rc, out, calls = e.run([pod("d-1", dep="svc-d", restarts=8, age=680, term_ago=2, now=later(80))], [deploy("svc-d")], now=later(80), extra=["--day-limit", "100"])
    check("restarts older than the hour fall out of the hour budget (4 + 4 over 80 min is not 8 in an hour)", rc == 0 and not calls, out)
    rc, out, calls = e.run([pod("d-1", dep="svc-d", restarts=8, age=690, term_ago=2, now=later(90))], [deploy("svc-d")], now=later(90))
    check("... but the same 8 in the day window (limit 5) stops it", rc == 1 and "8 restarts in 24 h" in out and "in 60 min" not in out, out)

    e = Env()
    e.run([pod("s-1", dep="svc-s", restarts=0, age=600)], [deploy("svc-s")], now=NOW)
    rcs = []
    for i, (rest, mins) in enumerate([(2, 180), (4, 360), (6, 540)]):
        rcs.append(e.run([pod("s-1", dep="svc-s", restarts=rest, age=600 + mins, term_ago=5, now=later(mins))], [deploy("svc-s")], now=later(mins)))
    check("a slow loop (2 restarts every 3 h, never 5 in an hour) is stopped by the 24 h budget on the 6th restart",
          [r[0] for r in rcs] == [0, 0, 1] and "6 restarts in 24 h" in rcs[2][1], str(rcs))

    # ---- what is not a restart ----
    e = Env()
    rc, out, calls = e.run([pod("n-2", dep="svc-n", restarts=0, age=3, uid="new")], [deploy("svc-n")])
    check("a rollout (new pod, count 0) is not a restart", rc == 0 and not calls, out)
    e = Env()
    rc, out, calls = e.run([pod("j-1", kind="Job", owner="backfill", restarts=50, age=10, term_ago=1)], [deploy("svc-a")])
    check("a Job's pod is ignored (its own backoffLimit governs it)", rc == 0 and not calls, out)
    e = Env()
    rc, out, calls = e.run([pod("x-1", dep="gone-svc", restarts=9, age=10, term_ago=1)], [deploy("svc-a")])
    check("a pod whose Deployment is not in the list is ignored", rc == 0 and not calls, out)
    e = Env()
    e.run([pod("r-1", restarts=5, age=600)], [deploy("svc-a")], now=NOW)
    rc, out, calls = e.run([pod("r-1", restarts=3, age=615, now=later(15))], [deploy("svc-a")], now=later(15))
    check("a restart count that goes DOWN (a replaced container) is never a negative delta", rc == 0 and not calls, out)

    # ---- containers and pods that count ----
    e = Env()
    rc, out, calls = e.run([pod("i-1", dep="svc-i", restarts=2, init_restarts=5, age=20, term_ago=1)], [deploy("svc-i")])
    check("an INIT container's restarts count with the main container's (2 + 5 = 7)", rc == 1 and "7 restarts in 60 min" in out, out)
    e = Env()
    rc, out, calls = e.run([pod("m-1", dep="svc-m", restarts=3, age=20, term_ago=1, uid="m1"), pod("m-2", dep="svc-m", restarts=3, age=20, term_ago=1, uid="m2")], [deploy("svc-m", 2)])
    check("two replicas of one deployment add up (3 + 3 = 6)", rc == 1 and "6 restarts in 60 min" in out and "prev-replicas=2" in calls[0], out + str(calls))
    e = Env()
    evicted = [pod("ev-%d" % i, dep="svc-e", restarts=0, age=30, phase="Failed", status_reason="Evicted", start_ago=20, uid="ev%d" % i) for i in range(6)]
    rc, out, calls = e.run(evicted, [deploy("svc-e")])
    check("6 evicted pods in the hour stop the deployment (an eviction is a restart of the workload)", rc == 1 and "last Evicted" in out, out)
    e = Env()
    e.run(evicted[:3], [deploy("svc-e")], now=NOW)
    rc, out, calls = e.run(evicted[:3], [deploy("svc-e")], now=later(15))
    check("evicted pods are counted ONCE: the same 3 seen again add nothing", rc == 0 and not calls, out)

    # ---- never stopped ----
    e = Env()
    rc, out, calls = e.run([pod("w-1", dep="options-edge-web", restarts=9, age=20, term_ago=1)], [deploy("options-edge-web")], env="production")
    check("production MANDATORY service over budget: RESTART_BUDGET_HELD, exit 1, NO write at all", rc == 1 and not calls and
          "RESTART_BUDGET_HELD options-edge-web: 9 restarts" in out and "NOT stopped: production mandatory" in out, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("w-1", dep="stock-gex-service", restarts=9, age=20, term_ago=1)], [deploy("stock-gex-service")], env="prod")
    check("the stock-gex services the owner added are in the protected set (env 'prod' is production)", rc == 1 and not calls and "RESTART_BUDGET_HELD stock-gex-service" in out, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("w-1", dep="options-edge-web", restarts=9, age=20, term_ago=1)], [deploy("options-edge-web")], env="dev")
    check("the SAME name on dev is stopped (the mandatory set is a production set)", rc == 1 and len(calls) == 2 and "STOPPED just now" in out, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("w-1", dep="level-absorption-service", restarts=9, age=20, term_ago=1)], [deploy("level-absorption-service")], env="production")
    check("a non-mandatory production deployment is stopped", rc == 1 and len(calls) == 2, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("pg-0", dep="oe-keycloak-postgres", kind="StatefulSet", restarts=9, age=20, term_ago=1)], [deploy("svc-a")])
    check("a StatefulSet is paged (RESTART_BUDGET_HELD) and never scaled", rc == 1 and not calls and "RESTART_BUDGET_HELD oe-keycloak-postgres" in out and "StatefulSet" in out, out + str(calls))

    # ---- it stays stopped, and says so ----
    ann = {"options-edge.io/quarantined-at": "2026-11-19T10:00:00Z", "options-edge.io/quarantine-reason": "7 restarts in 60 min (limit 5)", "options-edge.io/quarantine-prev-replicas": "1"}
    e = Env()
    rc, out, calls = e.run([pod("o-1", restarts=0, age=600)], [deploy("svc-q", 0, ann)])
    check("a quarantined deployment at 0 is reported every run ('still stopped since ...'), exit 1, no new write", rc == 1 and not calls and
          "RESTART_BUDGET svc-q: still stopped since 2026-11-19T10:00:00Z" in out and "quarantine-clear.sh dev svc-q" in out, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("o-1", restarts=0, age=600)], [deploy("svc-q", 1, ann)])
    check("annotated but running again (a human scaled it up) and healthy: left alone", rc == 0 and not calls, out)

    # ---- dry run ----
    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=9, age=20, term_ago=1)], [deploy("svc-a")], mode="dry-run")
    check("dry-run: prints DRYRUN ... would stop, exits 0, writes NOTHING", rc == 0 and not calls and "DRYRUN svc-a:" in out and "would stop" in out and "STOPPED" not in out, out + str(calls))
    check("... and dry-run lines are not paged lines (they do not start with a RANK class)", not any(l.startswith("RESTART_BUDGET") for l in out.splitlines()), out)

    # ---- enforcement that fails must say so ----
    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=9, age=20, term_ago=1)], [deploy("svc-a")], fail="scale")
    check("scale refused: RESTART_BUDGET ... NOT STOPPED, exit 1", rc == 1 and "NOT STOPPED: scale to 0 failed: scale denied" in out, out)
    check("... and the annotation is taken off again (a marker on a deployment that kept running would lie)", len(calls) == 3 and calls[2].endswith(
          "annotate deploy/svc-a options-edge.io/quarantined-at- options-edge.io/quarantine-reason- options-edge.io/quarantine-prev-replicas-"), str(calls))
    rc, out, calls = e.run([pod("a-1", restarts=9, age=25, term_ago=1, now=later(15))], [deploy("svc-a")], now=later(15))
    check("... and the next run tries again (the restart history was kept) and stops it", rc == 1 and "STOPPED just now" in out and len(calls) == 2, out + str(calls))
    e = Env()
    rc, out, calls = e.run([pod("a-1", restarts=9, age=20, term_ago=1)], [deploy("svc-a")], fail="annotate")
    check("annotate refused: NOT STOPPED and NO scale is attempted", rc == 1 and "could not annotate" in out and len(calls) == 1, out + str(calls))

    # ---- could not check is exit 2, never clean ----
    e = Env()
    rc, out, _ = e.run([], [deploy("svc-a")])
    check("no pods at all is exit 2, not 'clean'", rc == 2 and out.startswith("ERROR: no pods"), out)
    e = Env()
    rc, out, _ = e.run(None, [deploy("svc-a")], raw_pods="not json")
    check("unparsable kubectl output is exit 2", rc == 2 and "cannot read kubectl pods output" in out, out)
    e = Env()
    rc, out, _ = e.run([pod("a-1")], [deploy("svc-a")], env="production", mandatory=os.path.join(e.d, "missing.txt"))
    check("production with no mandatory file is exit 2 (it must never run without the protected set)", rc == 2 and "mandatory-services file" in out, out)
    e = Env()
    open(os.path.join(e.d, "empty.txt"), "w").write("# nothing\n")
    rc, out, _ = e.run([pod("a-1")], [deploy("svc-a")], env="production", mandatory=os.path.join(e.d, "empty.txt"))
    check("... and an EMPTY mandatory file is exit 2 too", rc == 2 and "lists nothing" in out, out)
    e = Env()
    rc, out, _ = e.run([pod("a-1")], [deploy("svc-a")], extra=["--limit", "0"])
    check("--limit 0 is exit 2 (it would stop on the first restart)", rc == 2 and "--limit must be at least 1" in out, out)
    e = Env()
    blocker = os.path.join(e.d, "afile")
    open(blocker, "w").write("x")
    rc, out, _ = e.run([pod("a-1")], [deploy("svc-a")], state=os.path.join(blocker, "sub"))
    check("a baseline directory that cannot be made is exit 2", rc == 2 and "baseline directory" in out, out)
    e = Env()
    os.makedirs(e.state)
    open(os.path.join(e.state, "restart-budget-dev.json"), "w").write("{broken")
    rc, out, _ = e.run([pod("a-1", restarts=0, age=600)], [deploy("svc-a")])
    check("a corrupt baseline starts a new one, says so (NOTE:) and still runs", rc == 0 and "NOTE: the baseline" in out and "unreadable" in out, out)

    # ---- the wrapper ----
    w = os.path.join(HERE, "restart-budget-run.sh")
    r = subprocess.run(["bash", w], capture_output=True, text=True, timeout=30, env=dict(os.environ, FLEET_ENV="dev", RESTART_BUDGET_MODE="enfroce", FLEET_STATE_DIR="/tmp/x"))
    check("the wrapper refuses a mistyped mode (exit 2): a typo must not silently turn enforcement off or on", r.returncode == 2 and "must be enforce or dry-run" in r.stdout, r.stdout + r.stderr)
    r = subprocess.run(["bash", w], capture_output=True, text=True, timeout=30, env={k: v for k, v in os.environ.items() if k not in ("WORKSPACE", "FLEET_STATE_DIR")} | {"FLEET_ENV": "dev"})
    check("the wrapper with no baseline directory is exit 2", r.returncode == 2 and "baseline directory" in r.stdout, r.stdout + r.stderr)
    with tempfile.TemporaryDirectory() as d:
        fake = os.path.join(d, "bin")
        os.makedirs(fake)
        k = os.path.join(fake, "kubectl")
        open(k, "w").write('#!/usr/bin/env bash\ncase "$*" in *"config view"*) exit 0 ;; esac\necho "connection refused" >&2\nexit 1\n')
        os.chmod(k, 0o755)
        r = subprocess.run(["bash", w], capture_output=True, text=True, timeout=30,
                           env=dict(os.environ, PATH=fake + ":" + os.environ["PATH"], FLEET_ENV="dev", FLEET_STATE_DIR=os.path.join(d, "s"), RESTART_BUDGET_MODE="enforce"))
        check("a failing `kubectl get` inside the wrapper is exit 2 carrying kubectl's own words (so it reaches Discord)", r.returncode == 2 and r.stdout.startswith("ERROR: kubectl -n options-edge get pods failed: connection refused"), r.stdout + r.stderr)


MUTATIONS = [
    ("the budget is 'at least 5' instead of 'more than 5' (>= for >)", "hour > a.limit or day > a.day_limit", "hour >= a.limit or day > a.day_limit"),
    ("the long window is switched off", "hour > a.limit or day > a.day_limit", "hour > a.limit"),
    ("the production mandatory protection is removed", "    if dep in mandatory:\n", "    if False:\n"),
    ("StatefulSets are no longer held", "    if dep in held_owner:\n", "    if False:\n"),
    ("the restart history is not cleared after a stop", "    events.pop(dep, None)", "    pass"),
    ("the cumulative restart count is used instead of the delta", 'hour = day = max(0, total - int(prev.get("count", 0)))', "hour = day = total"),
    ("a failed scale leaves the annotation in place (no rollback)", '        cmd("annotate", "deploy/%s" % dep, "%s-" % QUARANTINED_AT,', '        (lambda *x: None)("annotate", "deploy/%s" % dep, "%s-" % QUARANTINED_AT,'),
    ("dry-run writes anyway", '    if a.mode == "dry-run":\n        dry.append("DRYRUN %s:', '    if False:\n        dry.append("DRYRUN %s:'),
    ("it scales UP instead of to 0", 'cmd("scale", "deploy/%s" % dep, "--replicas=0")', 'cmd("scale", "deploy/%s" % dep, "--replicas=1")'),
    ("init containers are not counted", '    for key in ("initContainerStatuses", "containerStatuses"):', '    for key in ("containerStatuses",):'),
    ("Job pods are counted as a deployment's restarts", '    else:\n        continue                      # Job pods, bare pods', '    else:\n        subject = oname'),
    ("the standing stop is no longer reported", '    if QUARANTINED_AT in ann and ((d.get("spec") or {}).get("replicas")) == 0 and dep not in stopped_now:', '    if False:'),
    ("an old pod's burst is assumed to be still going (the 3-interval recency test is dropped)", "                if term_age <= 3 * (age_s / total):", "                if True:"),
    ("the first-sight estimate for an old pod is dropped", "day, est = max(1, min(total, int(total * day_s / age_s))), True", "day, est = 0, True"),
]


def campaign():
    print("\nmutation campaign: every mutant must make the controls fail")
    src = open(os.path.join(HERE, "restart-budget.py")).read()
    survivors = []
    with tempfile.TemporaryDirectory() as d:
        for i, (label, old, new) in enumerate(MUTATIONS):
            if src.count(old) != 1:
                print("  FAIL %-70s  anchor matches %d time(s), a mutation that changes nothing proves nothing" % (label, src.count(old)))
                survivors.append(label + " (anchor)")
                continue
            m = os.path.join(d, "mutant%d.py" % i)
            open(m, "w").write(src.replace(old, new))
            r = subprocess.run([sys.executable, os.path.abspath(__file__), "--controls-only"], capture_output=True, text=True, timeout=600, env=dict(os.environ, RB_CHECK=m))
            killed = r.returncode != 0
            print(("  killed   " if killed else "  SURVIVED ") + label)
            if not killed:
                survivors.append(label)
    return survivors


if __name__ == "__main__":
    controls()
    if fails:
        print("FAILED controls: %s" % fails)
        sys.exit(1)
    if "--controls-only" in sys.argv:
        print("all restart-budget controls passed")
        sys.exit(0)
    survivors = campaign()
    if survivors:
        print("SURVIVING MUTANTS: %s" % survivors)
        sys.exit(1)
    print("all restart-budget controls passed and %d/%d mutants killed" % (len(MUTATIONS), len(MUTATIONS)))
