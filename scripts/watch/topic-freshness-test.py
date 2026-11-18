#!/usr/bin/env python3
"""Negative controls for topic-freshness.py: the topics are a JSON fixture, the clock is --now."""
import json, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = os.path.join(HERE, "topic-freshness.py")
NOW = "2026-11-18T08:00:00Z"            # Wed 03:00 ET: inside the session and the overnight window
NOW_MS = 1794988800000
fails = []


def run(fixture, now=NOW, env="dev"):
    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "f.json")
        json.dump(fixture, open(f, "w"))
        r = subprocess.run([sys.executable, CHECK, "--env", env, "--now", now, "--fixture", f], capture_output=True, text=True, timeout=30)
        return r.returncode, r.stdout + r.stderr


def fresh(**over):
    base = {"underlying.spx.price": NOW_MS - 30000, "underlying.es.price": NOW_MS - 30000, "underlying.es.trades": NOW_MS - 1000,
            "options.indicators.estimate.bars": NOW_MS - 60000}
    base.update(over)
    return base


def case(name, want_rc, fixture, expect=None, forbid=None, now=NOW, env="dev"):
    rc, out = run(fixture, now, env)
    why = []
    if rc != want_rc: why.append("exit %d wanted %d" % (rc, want_rc))
    if expect and expect not in out: why.append("lacks %r" % expect)
    if forbid and forbid in out: why.append("contains %r" % forbid)
    print(("  ok   " if not why else "  FAIL ") + name + ("" if not why else "  " + "; ".join(why) + "\n" + out[:300]))
    if why: fails.append(name)


print("topic-freshness.py negative controls")
case("all fresh is clean", 0, fresh(), expect="0 stale")
case("spot silent for 4h53m (the 11-18 incident) pages", 1, fresh(**{"underlying.spx.price": NOW_MS - 293 * 60000}), expect="TOPIC_STALE underlying.spx.price: newest record 293 min old")
case("only the stale topic is named", 1, fresh(**{"underlying.es.trades": NOW_MS - 40 * 60000}), forbid="TOPIC_STALE underlying.spx.price")
case("a topic with no record at all pages", 1, fresh(**{"underlying.es.price": None}), expect="no record at all")
case("exactly at the limit is fresh", 0, fresh(**{"underlying.spx.price": NOW_MS - 10 * 60000}))
case("Saturday is not checked", 0, fresh(**{"underlying.spx.price": 1}), now="2026-11-14T15:00:00Z", expect="0 topic(s) checked")
case("the daily break is not checked", 0, fresh(**{"underlying.spx.price": 1}), now="2026-11-18T22:30:00Z", expect="0 topic(s) checked")
LATER_MS = NOW_MS + 8 * 3600 * 1000      # 16:00Z = 11:00 ET, after the cash open
case("estimate bars are not required after the cash open", 0,
     {"underlying.spx.price": LATER_MS - 1000, "underlying.es.price": LATER_MS - 1000, "underlying.es.trades": LATER_MS - 1000,
      "options.indicators.estimate.bars": 1}, now="2026-11-18T16:00:00Z", expect="3 topic(s) checked")
case("estimate bars ARE required overnight", 1, fresh(**{"options.indicators.estimate.bars": NOW_MS - 90 * 60000}), expect="TOPIC_STALE options.indicators.estimate.bars")

def es4(**over):
    base = {"es.underlying.es.trades": NOW_MS - 1000, "es.underlying.es.price.basis-reference": NOW_MS - 20000}
    base.update(over)
    return base


print("es4 profile")
case("es4 all fresh is clean and checks both topics", 0, es4(), env="es4", expect="2 topic(s) checked, 0 stale")
case("es4 basis reference silent 4h51m (the 11-18 mirror starvation) pages", 1, es4(**{"es.underlying.es.price.basis-reference": NOW_MS - 291 * 60000}), env="es4",
     expect="TOPIC_STALE es.underlying.es.price.basis-reference: newest record 291 min old")
case("es4 stale tape is named, the fresh basis reference is not", 1, es4(**{"es.underlying.es.trades": NOW_MS - 40 * 60000}), env="es4",
     expect="TOPIC_STALE es.underlying.es.trades", forbid="TOPIC_STALE es.underlying.es.price.basis-reference")
case("es4 is not checked on a Saturday", 0, es4(**{"es.underlying.es.trades": 1}), env="es4", now="2026-11-14T15:00:00Z", expect="0 topic(s) checked")
case("dev topics are not demanded of es4 (an extra dev topic in the fixture is ignored)", 0, dict(es4(), **{"underlying.spx.price": 1}), env="es4", forbid="underlying.spx.price")
sys.exit(1 if fails else 0)
