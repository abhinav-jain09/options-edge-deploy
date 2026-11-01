#!/usr/bin/env python3
"""T-18 (LEVEL-ABSORPTION-GEOMETRY-SITES-REQUIREMENT): the watermark-bounded reader job that ARCHIVES the Candle Direction export.

context-tape.direction.current is COMPACTED (one latest record per key) and context-tape.direction.commissioning is in no archive set, so the joined export
(context-tape-service CandleDirectionExport: barId join of the decision and the closed candle `candle|<barId>`) is archived by this job, not by a mirror.

Watermark-bounded: for every partition of both topics the job first reads the LAST record offset (kcat -o -1 -c 1) = the high watermark minus one at start; it then reads
from the beginning and stops at that offset. Records appended after the start are not in the archive and the manifest says so (the offsets are written). A partition with no
record has watermark -1. The job never writes a topic, never commits a group (kcat without -G), and refuses a read that does not reach its watermark.

Join rules are CandleDirectionExport.join, ported 1:1 (the conformance test feeds the Java test's own fixtures). A pair that fails the join is REFUSED into the
`refused` count with its barId and reason, never dropped silently. Output: <out>/candle-direction-export.tsv and <out>/manifest.json (sha256 of the tsv, per-topic watermarks).
Usage: candle-direction-export-archive.py --bootstrap HOST:PORT --out DIR [--kcat PATH] [--current TOPIC] [--candles TOPIC] [--timeout-ms N]
"""
import argparse, hashlib, json, os, subprocess, sys

COLS = ["barId", "tradeDate", "candleStartMs", "candleEndMs", "intendedSide", "posture", "available", "unavailableReason", "ruleFired", "veto", "quality"]


def _text(n, f):
    v = n.get(f)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f + " is required")
    return v


def _bool(n, f):
    v = n.get(f)
    if not isinstance(v, bool):
        raise ValueError(f + " must be a boolean")
    return v


def _nullable(n, f):
    v = n.get(f)
    if v is None:
        return None
    if not isinstance(v, str):
        raise ValueError(f + " must be text or null")
    return v


def _int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def quality(rule_fired, veto, unavailable):
    if unavailable:
        return "UNREADABLE"
    if veto is not None:
        return "VETOED:" + veto
    return "RULE_FIRED" if rule_fired else "NO_RULE"


def join(cur, can):
    """CandleDirectionExport.join; ValueError when the two records are not the same bar or are not readable."""
    if cur is None or can is None:
        raise ValueError("both the current decision and the candle are required")
    bar = _text(cur, "barId")
    if bar != _text(can, "barId"):
        raise ValueError("the candle is bar %s, the decision is for %s" % (can.get("barId"), bar))
    if not (_int(cur.get("barEndMs")) and _int(can.get("startMs")) and _int(can.get("endMs"))):
        raise ValueError("barEndMs, startMs and endMs must be integers")
    start, end = can["startMs"], can["endMs"]
    if end != cur["barEndMs"]:
        raise ValueError("the decision's barEndMs is not the candle's endMs")
    if start < 0 or start >= end:
        raise ValueError("the candle's start is not before its end")
    side = cur.get("intendedSide")
    if side == "NONE":
        side = None
    if side is not None and side not in ("LONG_DELTA", "SHORT_DELTA"):
        raise ValueError("intendedSide " + str(side))
    why = cur.get("unreadableReason")
    if why is not None and not isinstance(why, str):
        raise ValueError("unreadableReason must be text or null")
    unavailable = why is not None
    fired, veto = _bool(cur, "ruleFired"), _nullable(cur, "veto")
    return {"barId": bar, "tradeDate": _text(cur, "tradeDate"), "candleStartMs": start, "candleEndMs": end, "intendedSide": side, "posture": _nullable(cur, "posture"),
            "available": not unavailable, "unavailableReason": why, "ruleFired": fired, "veto": veto, "quality": quality(fired, veto, unavailable)}


def kcat(args, bootstrap, kc, timeout_ms):
    cmd = [kc, "-b", bootstrap, "-X", "socket.timeout.ms=%d" % timeout_ms] + args
    return subprocess.run(cmd, capture_output=True, text=True, timeout=max(30, timeout_ms // 1000 * 3))


def partitions(bootstrap, kc, topic, timeout_ms):
    r = kcat(["-L", "-J", "-t", topic], bootstrap, kc, timeout_ms)
    if r.returncode != 0:
        raise SystemExit("metadata read failed for %s: %s" % (topic, r.stderr.strip()))
    for t in json.loads(r.stdout).get("topics", []):
        if t.get("topic") == topic and not t.get("error"):
            return sorted(p["partition"] for p in t["partitions"])
    raise SystemExit("topic %s is absent: nothing is archived and nothing is guessed" % topic)


def snapshot(bootstrap, kc, topic, timeout_ms):
    """{partition: last record offset at this instant, -1 when empty}"""
    marks = {}
    for p in partitions(bootstrap, kc, topic, timeout_ms):
        last = kcat(["-C", "-t", topic, "-p", str(p), "-o", "-1", "-c", "1", "-e", "-f", "%o\n"], bootstrap, kc, timeout_ms)
        txt = last.stdout.strip()
        if last.returncode != 0 and not txt:
            raise SystemExit("watermark read failed for %s/%d: %s" % (topic, p, last.stderr.strip()))
        marks[p] = int(txt) if txt else -1
    return marks


def read_topic(bootstrap, kc, topic, marks, timeout_ms):
    """records [(partition, offset, key, value)] up to the snapshot watermarks"""
    recs = []
    for p in sorted(marks):
        if marks[p] < 0:
            continue
        r = kcat(["-C", "-t", topic, "-p", str(p), "-o", "beginning", "-e", "-J"], bootstrap, kc, timeout_ms)
        got = -1
        for line in r.stdout.splitlines():
            if not line.strip():
                continue
            m = json.loads(line)
            if m["offset"] > marks[p]:
                break
            got = m["offset"]
            recs.append((p, m["offset"], m.get("key"), m.get("payload")))
        if got < marks[p]:
            # compaction leaves holes, so the LAST offset must be reached exactly
            raise SystemExit("%s/%d: read stopped at %d before the watermark %d; refusing a partial archive" % (topic, p, got, marks[p]))
    return recs


def build(cur_recs, can_recs):
    """latest decision per barId (higher offset wins within a partition order), candle|barId records -> (rows, refused)"""
    cur, can, bad = {}, {}, []
    for _p, _o, k, v in cur_recs:
        if not v:   # a tombstone deletes the bar (the key is the bar id or ends with it); a deleted decision is never exported
            for b in [b for b in cur if k and (b == k or k.endswith("|" + b))]:
                del cur[b]
            continue
        try:
            d = json.loads(v)
        except ValueError:
            d = None
        if isinstance(d, dict) and isinstance(d.get("barId"), str):
            cur[d["barId"]] = d
        else:
            bad.append(("current:" + str(k), "unreadable decision record"))
    for _p, _o, k, v in can_recs:
        if not k or not k.startswith("candle|"):
            continue
        if not v:
            can.pop(k[len("candle|"):], None)
            continue
        try:
            d = json.loads(v)
        except ValueError:
            d = None
        if isinstance(d, dict):
            can[k[len("candle|"):]] = d
        else:
            bad.append((k, "unreadable candle record"))
    rows, refused = [], list(bad)
    for bar in sorted(cur):
        if bar not in can:
            refused.append((bar, "no candle record for the bar"))
            continue
        try:
            rows.append(join(cur[bar], can[bar]))
        except ValueError as e:
            refused.append((bar, str(e)))
    return rows, refused


def write(out, rows, refused, marks_cur, marks_can, topics):
    os.makedirs(out, exist_ok=True)
    tsv = os.path.join(out, "candle-direction-export.tsv")
    with open(tsv, "w") as f:
        f.write("\t".join(COLS) + "\n")
        for r in rows:
            f.write("\t".join("" if r[c] is None else str(r[c]).lower() if isinstance(r[c], bool) else str(r[c]) for c in COLS) + "\n")
    digest = hashlib.sha256(open(tsv, "rb").read()).hexdigest()
    man = {"topics": topics, "watermarksAtStart": {topics[0]: marks_cur, topics[1]: marks_can}, "rows": len(rows), "refused": [{"barId": b, "reason": r} for b, r in refused], "tsvSha256": digest}
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(man, f, indent=1, sort_keys=True)
    return man


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--kcat", default="kcat")
    ap.add_argument("--current", default="context-tape.direction.current")
    ap.add_argument("--candles", default="context-tape.direction.commissioning")
    ap.add_argument("--timeout-ms", type=int, default=15000)
    a = ap.parse_args(argv)
    mc = snapshot(a.bootstrap, a.kcat, a.current, a.timeout_ms)   # BOTH snapshots before either read: one start instant
    mk = snapshot(a.bootstrap, a.kcat, a.candles, a.timeout_ms)
    cur = read_topic(a.bootstrap, a.kcat, a.current, mc, a.timeout_ms)
    can = read_topic(a.bootstrap, a.kcat, a.candles, mk, a.timeout_ms)
    rows, refused = build(cur, can)
    man = write(a.out, rows, refused, {str(k): v for k, v in mc.items()}, {str(k): v for k, v in mk.items()}, [a.current, a.candles])
    print(json.dumps({k: man[k] for k in ("rows", "tsvSha256")}), "refused=%d" % len(refused))
    return 0


if __name__ == "__main__":
    sys.exit(main())
