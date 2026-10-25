"""T-18 reader job: join conformance with the Java CandleDirectionExportTest fixtures, and the watermark bound against a fake kcat."""
import importlib.util, json, os, pathlib, stat, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("cdea", ROOT / "scripts/evidence/candle-direction-export-archive.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

CUR = {"barId": "b1", "tradeDate": "2026-10-26", "barEndMs": 1000060, "intendedSide": "LONG_DELTA", "posture": "TREND", "unreadableReason": None, "ruleFired": True, "veto": None}
CAN = {"barId": "b1", "startMs": 1000000, "endMs": 1000060}


class Join(unittest.TestCase):
    def test_fields_and_quality(self):
        e = m.join(CUR, CAN)
        self.assertEqual((1000000, 1000060, "LONG_DELTA", "TREND", True, "RULE_FIRED"), (e["candleStartMs"], e["candleEndMs"], e["intendedSide"], e["posture"], e["available"], e["quality"]))
        self.assertEqual("VETOED:BIAS", m.join(dict(CUR, veto="BIAS"), CAN)["quality"])
        u = m.join(dict(CUR, intendedSide=None, unreadableReason="NO_ES"), CAN)
        self.assertEqual((False, "NO_ES", None, "UNREADABLE"), (u["available"], u["unavailableReason"], u["intendedSide"], u["quality"]))
        self.assertIsNone(m.join(dict(CUR, intendedSide="NONE"), CAN)["intendedSide"])

    def test_bad_pairs_are_refused(self):
        bad = [(CUR, dict(CAN, barId="b2")), (CUR, dict(CAN, endMs=1000061)), (CUR, dict(CAN, startMs=1000060)), (dict(CUR, intendedSide="LONG"), CAN),
               (dict(CUR, barEndMs="x"), CAN), (dict(CUR, posture={"a": 1}), CAN), (dict(CUR, ruleFired="yes"), CAN), (dict(CUR, veto=[]), CAN),
               (dict(CUR, unreadableReason={}), CAN), (CUR, dict(CAN, startMs=1.5)), ({k: v for k, v in CUR.items() if k != "barId"}, CAN)]
        for c, k in bad:
            with self.assertRaises(ValueError, msg=str((c, k))):
                m.join(c, k)
        with self.assertRaises(ValueError):
            m.join(None, CAN)


FAKE = r'''#!/usr/bin/env python3
import json, sys
a = sys.argv[1:]
data = json.load(open(%r))
def opt(f): return a[a.index(f) + 1]
if "-L" in a:
    t = opt("-t"); print(json.dumps({"topics": [{"topic": t, "partitions": [{"partition": p} for p in sorted(data[t])]}]})); sys.exit(0)
t, p = opt("-t"), opt("-p"); recs = data[t][p] if isinstance(list(data[t])[0], str) else data[t][int(p)]
if opt("-o") == "-1":
    if recs: print(recs[-1]["offset"])
    sys.exit(0)
for r in recs:
    print(json.dumps({"offset": r["offset"], "key": r["key"], "payload": r["value"]}))
'''


class Job(unittest.TestCase):
    def run_job(self, data):
        d = tempfile.mkdtemp()
        fixture = os.path.join(d, "data.json"); json.dump(data, open(fixture, "w"))
        k = os.path.join(d, "kcat"); open(k, "w").write(FAKE % fixture); os.chmod(k, 0o755)
        out = os.path.join(d, "out")
        m.main(["--bootstrap", "x:1", "--out", out, "--kcat", k])
        return out

    def test_archive_joins_refuses_and_records_the_watermarks(self):
        c2 = dict(CUR, barId="b2"); k2 = dict(CAN, barId="b2", endMs=999)
        data = {"context-tape.direction.current": {"0": [{"offset": 0, "key": "b1", "value": json.dumps(CUR)}, {"offset": 3, "key": "b2", "value": json.dumps(c2)}], "1": []},
                "context-tape.direction.commissioning": {"0": [{"offset": 0, "key": "candle|b1", "value": json.dumps(CAN)}, {"offset": 1, "key": "candle|b2", "value": json.dumps(k2)}]}}
        out = self.run_job(data)
        man = json.load(open(os.path.join(out, "manifest.json")))
        self.assertEqual(1, man["rows"])
        self.assertEqual(["b2"], [r["barId"] for r in man["refused"]])
        self.assertEqual({"0": 3, "1": -1}, man["watermarksAtStart"]["context-tape.direction.current"])
        lines = open(os.path.join(out, "candle-direction-export.tsv")).read().splitlines()
        self.assertEqual(2, len(lines)); self.assertTrue(lines[1].startswith("b1\t2026-10-26\t1000000\t1000060\tLONG_DELTA\tTREND\ttrue"))

    def test_a_missing_topic_is_not_guessed(self):
        with self.assertRaises(Exception):
            self.run_job({})


if __name__ == "__main__":
    unittest.main()
