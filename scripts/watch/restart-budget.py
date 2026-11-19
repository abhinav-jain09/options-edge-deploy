#!/usr/bin/env python3
"""Restart budget (owner rule 2026-11-19: "a crash loop or any kind of restart only 5 times, not more than that").

  restart-budget.py --env dev --pods pods.json --deploys deploys.json --state DIR [--mode enforce|dry-run]
                    [--limit 5] [--window 60] [--day-limit 5] [--day-window 1440] [--namespace options-edge] [--kubectl kubectl] [--now ISO]

A Deployment whose pods restarted MORE than --limit times inside the rolling --window (minutes), OR more than --day-limit times inside the
rolling --day-window (24 h), is STOPPED. Two windows because one is not enough: the 11-19 dev loops restart every ~30 minutes (level-absorption 220
restarts in 4.5 days), which a 5-per-hour budget never reaches but a 5-per-day budget does. Stopped means: annotated and scaled to 0, so a
crash loop burns no CPU (11-19: two OOM loops, level-absorption 220 restarts and journal-dispatcher 31, took a third of the dev host's pod CPU,
load average 160 on 16 cores) and cannot flap for days unseen. One finding per deployment, so the fleet-alert composer pages ONCE per incident.

WHAT COUNTS. Restarts of any container or init container (restartCount, which already includes OOMKilled and Error terminations) plus pods
EVICTED. Counted as DELTAS between runs, never as the cumulative count: a long-lived pod carries restarts from weeks ago (bo-app-postgres-0 has
35, level-absorption 220) and must not be stopped for them. A pod first seen is dated by its creation time (all its restarts are inside the window
if it is younger than the window); an older pod seen for the first time contributes one restart only if its last termination is inside the window.
A rollout (new pod, count 0) is not a restart. The 6th restart inside the window stops the deployment: 5 are allowed.

WHAT IS NEVER STOPPED. Production deployments in config/prod-mandatory-services.txt (PR 1677): paged as RESTART_BUDGET_HELD, never scaled, because
stopping them is the outage (scaling stock-gex-service to 0 blanked Gamma Lab on 11-19). StatefulSets and DaemonSets (a database is not scaled to
0 by a watcher): paged as RESTART_BUDGET_HELD. Job pods are ignored (the Job's own backoffLimit governs them).

STAYS STOPPED. The deployment is annotated options-edge.io/quarantined-at; the autostart and bring-up scripts skip annotated deployments and say
so, and this check keeps reporting it (RESTART_BUDGET, 'still stopped') until a human runs scripts/ops/quarantine-clear.sh, which removes the
annotations and restores the previous replica count. After a stop the deployment's restart history is cleared, so clearing it does not re-stop
it on the old restarts.

--mode dry-run prints what it WOULD do (lines start DRYRUN, which the composer ignores), changes nothing in the cluster and exits 0.
Exit 0 = nothing over budget and nothing stopped, 1 = at least one finding, 2 = the check itself could not run.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys

QUARANTINED_AT = "options-edge.io/quarantined-at"
QUARANTINE_REASON = "options-edge.io/quarantine-reason"
QUARANTINE_PREV = "options-edge.io/quarantine-prev-replicas"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MANDATORY = os.path.normpath(os.path.join(HERE, "..", "..", "config", "prod-mandatory-services.txt"))

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True, help="dev | es4 | production | prod")
ap.add_argument("--pods", required=True, help="kubectl -n <ns> get pods -o json")
ap.add_argument("--deploys", required=True, help="kubectl -n <ns> get deployments -o json")
ap.add_argument("--state", required=True, help="baseline directory (.oe-watch)")
ap.add_argument("--mode", choices=("enforce", "dry-run"), default="dry-run")
ap.add_argument("--limit", type=int, default=5, help="restarts allowed inside the window; one more stops the deployment")
ap.add_argument("--window", type=int, default=60, help="rolling window in minutes")
ap.add_argument("--day-limit", type=int, default=5, help="restarts allowed inside --day-window")
ap.add_argument("--day-window", type=int, default=1440, help="rolling long window in minutes (24 h)")
ap.add_argument("--namespace", default="options-edge")
ap.add_argument("--kubectl", default="kubectl")
ap.add_argument("--mandatory", default=DEFAULT_MANDATORY, help="file of production mandatory deployments")
ap.add_argument("--now", default=None, help="ISO instant, for the tests")
a = ap.parse_args()


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


if a.limit < 1:
    die("--limit must be at least 1 (got %d): 0 would stop a deployment on its first restart" % a.limit)
if a.window < 1:
    die("--window must be at least 1 minute (got %d)" % a.window)
if a.day_limit < 1:
    die("--day-limit must be at least 1 (got %d)" % a.day_limit)
if a.day_window < a.window:
    die("--day-window (%d) must not be shorter than --window (%d)" % (a.day_window, a.window))
env = "production" if a.env in ("prod", "production") else a.env
now = ts(a.now) if a.now else dt.datetime.now(dt.timezone.utc)
now_s = now.timestamp()
win_s = a.window * 60
day_s = a.day_window * 60


def load_items(path, what):
    try:
        data = json.load(open(path))
        items = data["items"]
    except Exception as e:  # noqa: BLE001
        die("cannot read kubectl %s output: %s" % (what, e))
    if not isinstance(items, list):
        die("`items` of %s is not a list - refusing to report 'clean' on an answer this shape" % what)
    if not items:
        die("no %s in the input - refusing to report 'clean' on an empty answer" % what)
    return items


pods = load_items(a.pods, "pods")
deploys = {}
for d in load_items(a.deploys, "deployments"):
    n = (d.get("metadata") or {}).get("name")
    if not n:
        die("a deployment without a name in the input")
    deploys[n] = d

mandatory = set()
if env == "production":
    try:
        for line in open(a.mandatory):
            name = line.split("#", 1)[0].strip()
            if name:
                mandatory.add(name.split()[0])
    except OSError as e:
        die("cannot read the mandatory-services file %s: %s - without it a mandatory service could be stopped" % (a.mandatory, e))
    if not mandatory:
        die("the mandatory-services file %s lists nothing - refusing to run without the protected set" % a.mandatory)

# ---- baseline -------------------------------------------------------------------------------------------------------------------------
state_path = os.path.join(a.state, "restart-budget-%s.json" % env)
state, note = {"pods": {}, "events": {}}, None
try:
    os.makedirs(a.state, exist_ok=True)
except OSError as e:
    die("cannot create the baseline directory %s: %s" % (a.state, e))
if not os.access(a.state, os.W_OK):
    die("the baseline directory %s is not writable - restarts could not be counted between runs" % a.state)
if os.path.exists(state_path):
    try:
        state = json.load(open(state_path))
        if not isinstance(state.get("pods"), dict) or not isinstance(state.get("events"), dict):
            raise ValueError("unexpected shape")
    except Exception as e:  # noqa: BLE001
        state, note = {"pods": {}, "events": {}}, "the baseline %s was unreadable (%s) - starting a new one" % (state_path, e)

# ---- count this run's restarts per deployment ----------------------------------------------------------------------------------------------
new_pods = {}
last_reason = {}          # deployment -> "OOMKilled (exit 137)" of the most recent termination seen
held_owner = {}           # statefulset/daemonset name -> kind
fresh_events = {}         # deployment -> [(epoch, n)]


def last_terminated(statuses):
    best = None
    for c in statuses:
        t = (c.get("lastState") or {}).get("terminated") or {}
        fin = t.get("finishedAt")
        if fin:
            try:
                when = ts(fin)
            except Exception as e:  # noqa: BLE001
                die("a container has an unparsable terminated.finishedAt %r (%s)" % (fin, e))
            if best is None or when > best[0]:
                best = (when, "%s (exit %s)" % (t.get("reason") or "terminated", t.get("exitCode")))
    return best


for pod in pods:
    meta = pod.get("metadata") or {}
    name = meta.get("name") or "<unnamed>"
    uid = meta.get("uid") or name
    status = pod.get("status")
    if not isinstance(status, dict):
        die("pod %s has no status - a pod whose state cannot be read is not a healthy pod" % name)
    owners = meta.get("ownerReferences") or []
    owner = owners[0] if owners else {}
    kind, oname = owner.get("kind"), owner.get("name") or ""
    if kind == "ReplicaSet":
        subject = oname.rsplit("-", 1)[0]
        if subject not in deploys:
            continue                 # a ReplicaSet whose Deployment is gone, or not a Deployment's: nothing to stop
    elif kind in ("StatefulSet", "DaemonSet"):
        subject = oname
        held_owner[subject] = kind
    else:
        continue                      # Job pods, bare pods
    statuses = []
    for key in ("initContainerStatuses", "containerStatuses"):
        v = status.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            die("pod %s has a %s that is not a list" % (name, key))
        statuses += v
    total = 0
    for c in statuses:
        rc = c.get("restartCount")
        if rc is None:
            continue
        if not isinstance(rc, int) or isinstance(rc, bool):
            die("pod %s container %s has a non-integer restartCount %r" % (name, c.get("name"), rc))
        total += rc
    created_raw = meta.get("creationTimestamp")
    try:
        created = ts(created_raw)
    except Exception as e:  # noqa: BLE001
        die("pod %s has an unparsable creationTimestamp %r (%s)" % (name, created_raw, e))
    age_s = now_s - created.timestamp()
    lt = last_terminated(statuses)
    prev = state["pods"].get(uid)
    evicted = status.get("phase") == "Failed" and status.get("reason") == "Evicted"
    term_age = (now_s - lt[0].timestamp()) if lt else None
    est = False
    if evicted:
        if prev:
            hour = day = 0
        else:
            st = status.get("startTime") or created_raw
            try:
                age_ev = now_s - ts(st).timestamp()
            except Exception:  # noqa: BLE001
                age_ev = None
            hour = 1 if age_ev is not None and age_ev <= win_s else 0
            day = 1 if age_ev is not None and age_ev <= day_s else 0
        new_pods[uid] = {"count": 1, "dep": subject}
        last_reason[subject] = "Evicted"
    else:
        if prev:
            hour = day = max(0, total - int(prev.get("count", 0)))
        elif age_s <= win_s:
            hour = day = total                       # every restart of a pod younger than the window is inside it
        else:
            hour = 1 if total > 0 and term_age is not None and term_age <= win_s else 0
            if age_s <= day_s:
                day = total
            elif total > 0 and term_age is not None and term_age <= day_s:
                # first sight of a pod older than the long window: its restarts are undated, so assume a steady rate (and say so)
                day, est = max(1, min(total, int(total * day_s / age_s))), True
            else:
                day = 0
        new_pods[uid] = {"count": total, "dep": subject}
        if (hour or day) and lt:
            last_reason[subject] = lt[1]
    if hour or day:
        fresh_events.setdefault(subject, []).append((now_s, hour, day, est))

# merge into the rolling history, drop everything older than the long window
events = {}
for dep in set(state["events"]) | set(fresh_events):
    kept = [list(e) for e in state["events"].get(dep, []) if len(e) == 4 and now_s - e[0] <= day_s]
    kept += [list(e) for e in fresh_events.get(dep, [])]
    if kept:
        events[dep] = kept


def counts(dep):
    """(restarts in the short window, restarts in the long window, the long window figure is partly estimated)"""
    ev = events.get(dep, [])
    hour = sum(e[1] for e in ev if now_s - e[0] <= win_s)
    day = sum(e[2] for e in ev)
    return hour, day, any(e[3] for e in ev)


def over(dep):
    hour, day, _ = counts(dep)
    return hour > a.limit or day > a.day_limit


# ---- decide, then enforce -----------------------------------------------------------------------------------------------------------------
findings, dry = [], []
stopped_now = []


def cmd(*args):
    r = subprocess.run([a.kubectl, "-n", a.namespace, *args], capture_output=True, text=True, timeout=60)
    return r.returncode, (r.stdout + r.stderr).strip().replace("\n", " ")[:200]


for dep in sorted(events):
    hour, day, estimated = counts(dep)
    if a.mode == "dry-run" and not over(dep):
        dry.append("DRYRUN watch %s: %d restart(s) in %d min, %s%d in %d h (limits %d / %d)"
                   % (dep, hour, a.window, "about " if estimated else "", day, a.day_window // 60, a.limit, a.day_limit))
    if not over(dep):
        continue
    # the short window is the sharper statement; the long one is named only when it is the one that was crossed
    if hour > a.limit:
        why = "%d restarts in %d min (limit %d)" % (hour, a.window, a.limit)
    else:
        why = "%s%d restarts in %d h (limit %d)" % ("about " if estimated else "", day, a.day_window // 60, a.day_limit)
    if dep in last_reason:
        why += ", last %s" % last_reason[dep]
    d = deploys.get(dep)
    if dep in held_owner:
        findings.append(("RESTART_BUDGET_HELD", dep, "%s; NOT stopped: a %s is never scaled by this check, look at it" % (why, held_owner[dep])))
        continue
    if dep in mandatory:
        findings.append(("RESTART_BUDGET_HELD", dep, "%s; NOT stopped: production mandatory service (config/prod-mandatory-services.txt), fix it, do not scale it" % why))
        continue
    replicas = ((d or {}).get("spec") or {}).get("replicas")
    already = ((d or {}).get("metadata") or {}).get("annotations", {}) or {}
    if replicas == 0 and QUARANTINED_AT in already:
        continue                      # reported below as a standing stop
    prev_replicas = replicas if isinstance(replicas, int) and replicas > 0 else 1
    if a.mode == "dry-run":
        dry.append("DRYRUN %s: %s; would stop: annotate %s and scale %d -> 0" % (dep, why, QUARANTINED_AT, prev_replicas))
        continue
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    rc, out = cmd("annotate", "deploy/%s" % dep, "--overwrite", "%s=%s" % (QUARANTINED_AT, stamp),
                  "%s=%s" % (QUARANTINE_REASON, why[:180]), "%s=%d" % (QUARANTINE_PREV, prev_replicas))
    if rc != 0:
        findings.append(("RESTART_BUDGET", dep, "%s; NOT STOPPED: could not annotate: %s" % (why, out)))
        continue
    rc, out = cmd("scale", "deploy/%s" % dep, "--replicas=0")
    if rc != 0:
        cmd("annotate", "deploy/%s" % dep, "%s-" % QUARANTINED_AT, "%s-" % QUARANTINE_REASON, "%s-" % QUARANTINE_PREV)
        findings.append(("RESTART_BUDGET", dep, "%s; NOT STOPPED: scale to 0 failed: %s" % (why, out)))
        continue
    stopped_now.append(dep)
    findings.append(("RESTART_BUDGET", dep, "%s; STOPPED just now: scaled %d -> 0 and annotated. Clear with: scripts/ops/quarantine-clear.sh %s %s"
                     % (why, prev_replicas, a.env, dep)))

for dep in stopped_now:
    events.pop(dep, None)             # a cleared deployment must not be re-stopped on the restarts that stopped it

# standing stops: a deployment still at 0 with the annotation stays reported until a human clears it
for dep in sorted(deploys):
    d = deploys[dep]
    ann = ((d.get("metadata") or {}).get("annotations")) or {}
    if QUARANTINED_AT in ann and ((d.get("spec") or {}).get("replicas")) == 0 and dep not in stopped_now:
        findings.append(("RESTART_BUDGET", dep, "still stopped since %s (%s); previous replicas %s. Clear with: scripts/ops/quarantine-clear.sh %s %s"
                         % (ann.get(QUARANTINED_AT), ann.get(QUARANTINE_REASON, "no reason recorded"), ann.get(QUARANTINE_PREV, "?"), a.env, dep)))

# ---- keep the baseline (only after a complete pass) ----------------------------------------------------------------------------------------
state_out = {"pods": new_pods, "events": events}
tmp = state_path + ".tmp"
try:
    with open(tmp, "w") as f:
        json.dump(state_out, f)
    os.replace(tmp, state_path)
except OSError as e:
    die("cannot write the baseline %s: %s" % (state_path, e))

if note:
    print("NOTE: %s" % note)
for line in dry:
    print(line)
for cls, dep, text in sorted(findings, key=lambda f: (f[1], f[0])):
    print("%s %s: %s" % (cls, dep, text))
n_over = len([1 for dep in events if over(dep)])
print("restart-budget %s (%s): %d deployment(s) with restarts in the last %d h, %d over budget (%d per %d min or %d per %d h), %d stopped now, %d finding(s)"
      % (a.env, a.mode, len(events), a.day_window // 60, n_over, a.limit, a.window, a.day_limit, a.day_window // 60, len(stopped_now), len(findings)))
sys.exit(1 if findings else 0)
