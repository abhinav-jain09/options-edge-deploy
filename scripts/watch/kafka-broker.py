#!/usr/bin/env python3
"""Kafka broker check (dev): the broker that fenced itself, and the host it runs on. Nothing else pages a broker that is "up".

  kafka-broker.py --env dev [--state DIR] [--bootstrap H:P] [--since-minutes 30] [--now "YYYY-mm-dd HH:MM:SS"] [--fixture FILE] [--kcat PATH] [--ssh-host user@host]

WHY (2026-11-18): at 06:19:18 and 06:19:39 CET the dev broker's controller logged `Exceptionally slow controller event maybeFenceStaleBroker took 10774 ms`, then
`handleBrokerFenced: changing 6894 partition(s)` and, two seconds later, `handleBrokerUnfenced`. The broker had lost its session with its own controller and
re-elected every partition leader. Most clients re-synced; the Databento feed's producer did not and published nothing for 4 h 53 min. The broker answered
metadata requests the whole time, the process was up, and it runs on .102 as a launchd job - outside Kubernetes, so no pod/readiness/OOM check can see it.

What is checked (dev only: the broker lives on the dev Mac):
  1. METADATA ROUND TRIP over the bootstrap address (kcat -L): fails -> exit 2 (could not check); slower than --slow-ms -> BROKER_SLOW.
  2. THE FENCE SIGNATURE in the broker host's controller log inside --since-minutes: any `handleBrokerFenced` -> BROKER_FENCED (names the partition count,
     the worst slow-controller-event time and when); a slow controller event of >= --slow-event-ms without a fence -> BROKER_SLOW (the same stall, one step earlier).
  3. THE HOST: 15-minute load average per core >= --load-per-core -> BROKER_HOST_LOAD; swap in use >= --swap-gib -> BROKER_HOST_SWAP. (The 11-18 host ran 5.8-7.8 GiB
     of swap and 1.4 load per core: neither is proof of cause, so the thresholds sit well above them and these pages say "pressure", not "cause".)
The log and the host figures are read over ssh with BatchMode; an ssh that fails is exit 2, never a silent pass.
Exit 0 = nothing to report. Exit 1 = findings. Exit 2 = could not check. Paging, de-duplication and recovery are fleet-alert.py's (subject kafka-<env>-broker / -host).
"""
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

HOSTS = {"dev": {"ssh": "abhinav@192.168.100.102", "logs": "/Users/abhinav/development/kafka-options-edge/logs"}}

REMOTE = (
    "echo \"NOW $(date '+%%Y-%%m-%%d %%H:%%M:%%S')\"; echo \"SWAP $(sysctl -n vm.swapusage)\"; echo \"NCPU $(sysctl -n hw.ncpu)\"; "
    "echo \"LOAD $(sysctl -n vm.loadavg)\"; cd '%(logs)s' || { echo 'NOLOGDIR'; exit 0; }; "
    "for f in controller.log $(ls -t controller.log.* 2>/dev/null | head -%(nfiles)d); do "
    "grep -hE 'handleBrokerFenced|Exceptionally slow controller event' \"$f\" 2>/dev/null; done; echo END"
)

STAMP = r"^\[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+\]"
FENCED = re.compile(STAMP + r".*handleBrokerFenced: changing (?P<parts>\d+) partition")
SLOW = re.compile(STAMP + r".*Exceptionally slow controller event (?P<ev>\w+)\(\d+\) took (?P<ms>\d+) ms")


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def parse_host(text):
    """-> dict(now, swap_gib, ncpu, load15, fences[(ts, parts)], slows[(ts, event, ms)]). Anything missing is an error, not a zero."""
    out = {"fences": [], "slows": []}
    seen_end = False
    for line in text.splitlines():
        line = line.rstrip()
        if line.startswith("NOW "):
            out["now"] = dt.datetime.strptime(line[4:], "%Y-%m-%d %H:%M:%S")
        elif line.startswith("SWAP "):
            m = re.search(r"used = ([\d.]+)([MG])", line)
            if m:
                out["swap_gib"] = float(m.group(1)) / (1024.0 if m.group(2) == "M" else 1.0)
        elif line.startswith("NCPU "):
            out["ncpu"] = int(line[5:])
        elif line.startswith("LOAD "):
            nums = re.findall(r"[\d.]+", line[5:])
            if len(nums) >= 3:
                out["load15"] = float(nums[2])
        elif line == "NOLOGDIR":
            die("the broker log directory does not exist on the broker host")
        elif line == "END":
            seen_end = True
        else:
            m = FENCED.match(line)
            if m:
                out["fences"].append((dt.datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S"), int(m.group("parts"))))
                continue
            m = SLOW.match(line)
            if m:
                out["slows"].append((dt.datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S"), m.group("ev"), int(m.group("ms"))))
    missing = [k for k in ("now", "swap_gib", "ncpu", "load15") if k not in out]
    if not seen_end or missing:
        die("the broker host's answer was incomplete (missing %s%s)" % (", ".join(missing) or "nothing", "" if seen_end else "; no END marker"))
    return out


def read_host(ssh_host, logs, since_minutes):
    # controller.log rotates HOURLY (controller.log.YYYY-MM-DD-HH): read as many rotated files as the window reaches back, plus one for the boundary.
    nfiles = since_minutes // 60 + 2
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "LogLevel=ERROR", ssh_host, REMOTE % {"logs": logs, "nfiles": nfiles}]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    except (OSError, subprocess.TimeoutExpired) as e:
        die("ssh to the broker host %s failed: %s" % (ssh_host, e))
    if r.returncode != 0:
        die("ssh to the broker host %s failed (rc=%d): %s" % (ssh_host, r.returncode, " ".join(r.stderr.split())[-300:]))
    return r.stdout


def metadata_ms(kcat, bootstrap):
    t0 = time.monotonic()
    try:
        r = subprocess.run([kcat, "-b", bootstrap, "-L", "-J", "-t", "__consumer_offsets", "-m", "10"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        die("kcat metadata round trip to %s failed: %s" % (bootstrap, e))
    ms = (time.monotonic() - t0) * 1000.0
    if r.returncode != 0:
        die("kcat metadata round trip to %s failed: %s" % (bootstrap, " ".join(r.stderr.split())[-300:]))
    try:
        if not json.loads(r.stdout).get("brokers"):
            die("the broker at %s answered metadata with no brokers" % bootstrap)
    except ValueError:
        die("the broker at %s answered metadata that is not JSON" % bootstrap)
    return ms


def judge(host, meta_ms, a, subject_b, subject_h):
    findings = []
    since = host["now"] - dt.timedelta(minutes=a.since_minutes)
    fences = [f for f in host["fences"] if since <= f[0] <= host["now"] + dt.timedelta(minutes=1)]
    slows = [s for s in host["slows"] if since <= s[0] <= host["now"] + dt.timedelta(minutes=1) and s[2] >= a.slow_event_ms]
    worst = max([s[2] for s in slows], default=0)
    if fences:
        last = max(f[0] for f in fences)
        findings.append("BROKER_FENCED %s: the broker fenced itself %d time(s) in the last %d min (last %s, %d partition(s) re-elected%s); producers that did not re-sync stay silent "
                        "while their pods look healthy - check each producer log for 'delivery failed' and watch the TOPIC_STALE lines"
                        % (subject_b, len(fences), a.since_minutes, last.strftime("%H:%M:%S"), max(f[1] for f in fences),
                           ", worst controller event %d ms" % worst if worst else ""))
    elif slows:
        findings.append("BROKER_SLOW %s: %d controller event(s) took >= %d ms in the last %d min (worst %d ms, %s) - the stall that precedes a fence"
                        % (subject_b, len(slows), a.slow_event_ms, a.since_minutes, worst, max(slows)[1]))
    if meta_ms is not None and meta_ms >= a.slow_ms:
        findings.append("BROKER_SLOW %s: a metadata round trip took %d ms (limit %d)" % (subject_b, meta_ms, a.slow_ms))
    per_core = host["load15"] / max(host["ncpu"], 1)
    if per_core >= a.load_per_core:
        findings.append("BROKER_HOST_LOAD %s: 15-minute load %.1f on %d cores (%.1f per core, limit %.1f)" % (subject_h, host["load15"], host["ncpu"], per_core, a.load_per_core))
    if host["swap_gib"] >= a.swap_gib:
        findings.append("BROKER_HOST_SWAP %s: %.1f GiB of swap in use (limit %.0f GiB)" % (subject_h, host["swap_gib"], a.swap_gib))
    return findings, len(fences), len(slows), per_core


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=sorted(HOSTS))
    ap.add_argument("--state", default=None)  # accepted for stateful-check-run.sh; paging state lives in fleet-alert.py
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--since-minutes", type=int, default=30)
    ap.add_argument("--slow-event-ms", type=int, default=5000)
    ap.add_argument("--slow-ms", type=int, default=5000)
    ap.add_argument("--load-per-core", type=float, default=4.0)
    ap.add_argument("--swap-gib", type=float, default=24.0)
    ap.add_argument("--fixture", default=None, help="JSON {host_text, metadata_ms}, for the tests")
    ap.add_argument("--kcat", default=os.environ.get("KCAT", "/opt/homebrew/bin/kcat"))
    ap.add_argument("--ssh-host", default=None)
    a = ap.parse_args()
    cfg = HOSTS[a.env]
    if a.fixture:
        fx = json.load(open(a.fixture))
        host_text, meta_ms = fx["host_text"], fx.get("metadata_ms")
    else:
        bootstrap = a.bootstrap
        if not bootstrap:
            try:
                bootstrap = json.load(open(os.path.join(HERE, "..", "..", "schema-manifest", "kafka.json")))[a.env]
            except (OSError, ValueError, KeyError) as e:
                die("no bootstrap server for %s: %s" % (a.env, e))
        meta_ms = metadata_ms(a.kcat, bootstrap)
        host_text = read_host(a.ssh_host or cfg["ssh"], cfg["logs"], a.since_minutes)
    host = parse_host(host_text)
    findings, nf, ns, per_core = judge(host, meta_ms, a, "kafka-%s-broker" % a.env, "kafka-%s-host" % a.env)
    print("kafka-broker %s: metadata %s, %d fence(s) and %d slow controller event(s) in %d min, load %.1f/core, swap %.1f GiB, %d finding(s)"
          % (a.env, "n/a" if meta_ms is None else "%d ms" % meta_ms, nf, ns, a.since_minutes, per_core, host["swap_gib"], len(findings)))
    for f in findings:
        print(f)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
