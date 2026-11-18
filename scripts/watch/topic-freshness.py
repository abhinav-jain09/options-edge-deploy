#!/usr/bin/env python3
"""Topic-freshness check: a producer that is Running/Ready but publishes NOTHING is an outage nobody else pages.

  topic-freshness.py --env dev [--state DIR] [--bootstrap H:P] [--now ISO] [--fixture FILE] [--kcat PATH]

WHY (2026-11-18): the dev Databento feed's Kafka producer wedged (78,356 MessageTimedOut) while the pod stayed Running 1/1. underlying.spx.price,
underlying.es.price and underlying.es.trades were silent for 4 h 53 min and the estimate bars / market structure built from them had a hole.
Fleet-watch looks at pods and consumer lag, not at "this topic produced nothing for N minutes", so nobody was paged.

For each watched topic the newest record's publish time (max over its partitions) must be younger than its limit while its window is open:
  GLOBEX   = the CME ES session (Sunday 18:00 ET - Friday 17:00 ET minus the daily 17:00-18:00 break, minus the first 5 minutes after an open)
  OVERNIGHT = the part of that session before the 09:30 ET cash open (the estimate bars exist only there).
Exit 0 = every open-window topic is fresh (or no window is open). Exit 1 = TOPIC_STALE lines. Exit 2 = could not check (never healthy).
Reuses the session calendar of bridge-flow.py; paging, de-duplication and recovery messages are fleet-alert.py's (one incident per topic).
"""
import argparse
import datetime as dt
import importlib.util
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("bridge_flow", os.path.join(HERE, "bridge-flow.py"))
_bf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bf)

# (topic, max age in minutes, window, what stops downstream when it is silent)
WATCHED = {
    "dev": [
        ("underlying.spx.price", 10, "GLOBEX", "the SPX spot every bar, structure and pattern is built from"),
        ("underlying.es.price", 10, "GLOBEX", "the ES price the spot is derived from"),
        ("underlying.es.trades", 10, "GLOBEX", "the ES tape (CVD, footprint, liquidity, spot)"),
        ("options.indicators.estimate.bars", 10, "OVERNIGHT", "the overnight estimate candles and market structure"),
    ],
}


def die(msg):
    print("ERROR: %s" % msg)
    sys.exit(2)


def window_open(window, now):
    is_open, _ = _bf.session_state(now)
    if not is_open:
        return False
    if window == "GLOBEX":
        return True
    t = now.astimezone(_bf.ET)
    return t.hour * 60 + t.minute < 9 * 60 + 30 or t.hour * 60 + t.minute >= 18 * 60   # before the cash open, or after the Globex open


def newest_ms(kcat, bootstrap, topic):
    """Newest publish time (ms) over every partition of the topic; None = the topic holds no record."""
    try:
        meta = subprocess.run([kcat, "-b", bootstrap, "-L", "-J", "-t", topic], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        die("kcat metadata for %s failed: %s" % (topic, e))
    if meta.returncode != 0:
        die("kcat metadata for %s failed: %s" % (topic, " ".join(meta.stderr.split())[-300:]))
    try:
        topics = [t for t in json.loads(meta.stdout)["topics"] if t["topic"] == topic and "error" not in t]
        parts = [p["partition"] for p in topics[0]["partitions"]]
    except (ValueError, KeyError, IndexError):
        die("topic %s is not on %s" % (topic, bootstrap))
    best = None
    for p in parts:
        try:
            r = subprocess.run([kcat, "-b", bootstrap, "-t", topic, "-p", str(p), "-C", "-o", "-1", "-c", "1", "-e", "-q", "-f", "%T\n"],
                               capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as e:
            die("kcat read of %s[%d] failed: %s" % (topic, p, e))
        line = r.stdout.strip().splitlines()[-1:] or [""]
        if line[0].lstrip("-").isdigit() and int(line[0]) > 0:
            best = max(best or 0, int(line[0]))
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=sorted(WATCHED))
    ap.add_argument("--state", default=None)  # accepted for stateful-check-run.sh; paging state lives in fleet-alert.py
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--now", default=None)
    ap.add_argument("--fixture", default=None, help="JSON {topic: newest publish ms | null}, for the tests")
    ap.add_argument("--kcat", default=os.environ.get("KCAT", "/opt/homebrew/bin/kcat"))
    a = ap.parse_args()
    now = dt.datetime.fromisoformat(a.now.replace("Z", "+00:00")) if a.now else dt.datetime.now(dt.timezone.utc)
    bootstrap = a.bootstrap
    if not bootstrap and not a.fixture:
        try:
            bootstrap = json.load(open(os.path.join(HERE, "..", "..", "schema-manifest", "kafka.json")))[a.env]
        except (OSError, ValueError, KeyError) as e:
            die("no bootstrap server for %s: %s" % (a.env, e))
    fixture = json.load(open(a.fixture)) if a.fixture else None
    findings, checked = [], 0
    now_ms = int(now.timestamp() * 1000)
    for topic, limit, window, why in WATCHED[a.env]:
        if not window_open(window, now):
            continue
        newest = fixture.get(topic) if fixture is not None else newest_ms(a.kcat, bootstrap, topic)
        checked += 1
        if newest is None:
            findings.append("TOPIC_STALE %s: no record at all inside the %s window; %s is down" % (topic, window, why))
            continue
        age = (now_ms - newest) / 60000.0
        if age > limit:
            findings.append("TOPIC_STALE %s: newest record %d min old (limit %d) inside the %s window; %s is stalled (a Running pod can still be wedged: check the producer log for 'delivery failed')"
                            % (topic, age, limit, window, why))
    print("topic-freshness %s: %d topic(s) checked, %d stale" % (a.env, checked, len(findings)))
    for f in findings:
        print(f)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
