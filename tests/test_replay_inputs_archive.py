# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""Hermetic tests for scripts/ops/archive/replay-inputs/oe-replay-inputs-archive.py (no broker, no registry).

They drive the real writer/verify/check code with stub Kafka messages and a hand-made archived day, and
prove the failure modes the archive exists to catch: a tampered file, an orphan from a crashed run, a
discontinuity between committed runs, a record whose writer schema was never stored."""
from __future__ import annotations

import base64
import gzip
import importlib.util
import io
import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "ria", ROOT / "scripts/ops/archive/replay-inputs/oe-replay-inputs-archive.py")
ria = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ria)

try:
    import fastavro
except ImportError:  # the dev Mac venv has it; the decode tests are skipped elsewhere
    fastavro = None

SCHEMA = {"type": "record", "name": "T", "namespace": "t", "fields": [{"name": "x", "type": "long"}]}


class Msg:
    def __init__(self, p, o, ts, value, key=b"k"):
        self._p, self._o, self._ts, self._v, self._k = p, o, ts, value, key

    def partition(self): return self._p
    def offset(self): return self._o
    def timestamp(self): return (1, self._ts)
    def value(self): return self._v
    def key(self): return self._k
    def headers(self): return [("h", b"1")]


def avro(sid, x):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(SCHEMA), {"x": x})
    return b"\x00" + sid.to_bytes(4, "big") + buf.getvalue()


def write_day(root: Path, day="2026-11-02", topic="t.topic", n=3, sid=7, store_schema=True, base=0):
    """Hand-build exactly what capture() commits: chunk, day manifest line, schema file, _runs line."""
    ddir = root / f"dt={day}"
    (ddir / topic).mkdir(parents=True, exist_ok=True)
    (ddir / "_schemas").mkdir(exist_ok=True)
    msgs = [Msg(0, base + i, 1793600000000 + i, avro(sid, i) if fastavro else b"\x00" + sid.to_bytes(4, "big") + b"\x00")
            for i in range(n)]
    w = ria.DayWriter(root / "w.tmp")
    for m in msgs:
        w.add(m)
    w.close()
    name = f"{topic}.p0.e0.{w.first}-{w.last}.20261102T230000Z.rec.jsonl.gz"
    w.tmp.replace(ddir / topic / name)
    sha, md5, nb = ria.file_digests(ddir / topic / name)
    ent = {"path": f"{topic}/{name}", "topic": topic, "dt": day, "partition": 0, "epoch": 0, "offset_first": w.first,
           "offset_last": w.last, "records": w.records, "bytes": nb, "sha256": sha, "md5": md5,
           "ts_min_ms": w.ts_min, "ts_max_ms": w.ts_max, "schema_ids": sorted(w.sids),
           "schema_ids_missing": [] if store_schema else sorted(w.sids)}
    ria.append_line(ddir / "_manifest.jsonl", ent)
    if store_schema:
        (ddir / "_schemas" / f"{sid}.json").write_text(json.dumps(
            {"id": sid, "schemaType": "AVRO", "schema": json.dumps(SCHEMA), "references": [], "subjects": []}))
    ria.append_line(root / "_runs.jsonl", {"topic": topic, "partition": 0, "epoch": 0, "from": base,
                                           "to": base + n, "records": n,
                                           "files": [{"dt": day, "path": ent["path"], "sha256": sha}]})
    ria.write_day_manifests(ddir)
    return ddir, ent


def run(fn, **kw):
    out = io.StringIO()
    with redirect_stdout(out):
        rc = fn(Namespace(**kw))
    return rc, out.getvalue()


class Pure(unittest.TestCase):
    def test_schema_id(self):
        self.assertEqual(ria.schema_id_of(b"\x00\x00\x00\x07\xd1abc"), 2001)
        self.assertIsNone(ria.schema_id_of(b'{"a":1}'))
        self.assertIsNone(ria.schema_id_of(None))
        self.assertIsNone(ria.schema_id_of(b"\x00\x00"))

    def test_et_date_uses_record_time_not_run_time(self):
        # 2026-11-02 23:30 ET is 2026-11-03 04:30Z: the ET date must win.
        self.assertEqual(ria.et_date(1793680200000, "x"), "2026-11-02")
        self.assertEqual(ria.et_date(0, "fallback"), "fallback")

    def test_topics_file_rejects_duplicates_and_empty(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "t.conf"
            p.write_text("# c\na\nb # x\n")
            self.assertEqual(ria.read_topics(p), ["a", "b"])
            p.write_text("a\na\n")
            with self.assertRaises(SystemExit):
                ria.read_topics(p)
            p.write_text("# only comment\n")
            with self.assertRaises(SystemExit):
                ria.read_topics(p)

    def test_shipped_topic_list_has_the_replay_inputs(self):
        t = ria.read_topics(ROOT / "scripts/ops/archive/replay-inputs/replay-inputs-topics.conf")
        for need in ("options.spx.market-structure.tools", "options.spx.market-structure.geometry",
                     "options.spx.market-structure.break-events", "es.options.indicators.snapshot.current",
                     "options.spx.line-acceptance.journal", "options.spx.line-acceptance.checkpoint",
                     "es.futures.aggressor-flow"):
            self.assertIn(need, t)
        # already archived elsewhere: must not be duplicated here
        for dup in ("spx.basis.state", "underlying.es.price", "underlying.spx.price"):
            self.assertNotIn(dup, t)

    def test_envelope_is_byte_exact(self):
        m = Msg(2, 9, 5, b"\x00\x00\x00\x01\xff\x00")
        e = json.loads(ria.envelope(m))
        self.assertEqual(base64.b64decode(e["v"]), b"\x00\x00\x00\x01\xff\x00")
        self.assertEqual((e["p"], e["o"], e["ts"]), (2, 9, 5))


class Verify(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.root = Path(self.t.name)

    def tearDown(self):
        self.t.cleanup()

    def v(self):
        return run(ria.verify, root=str(self.root), dt="2026-11-02")

    def test_clean_day_verifies(self):
        write_day(self.root)
        rc, out = self.v()
        self.assertEqual(rc, 0, out)

    def test_tampered_file_is_caught(self):
        ddir, ent = write_day(self.root)
        with (ddir / ent["path"]).open("ab") as f:
            f.write(b"x")
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("digest/size mismatch", out)

    def test_orphan_from_a_crashed_run_is_reported(self):
        ddir, _ = write_day(self.root)
        (ddir / "t.topic" / "t.topic.p0.9-9.crashed.rec.jsonl.gz").write_bytes(b"")
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("orphan file", out)

    def test_discontinuity_between_runs_is_reported(self):
        write_day(self.root, n=3, base=0)
        ria.append_line(self.root / "_runs.jsonl", {"topic": "t.topic", "partition": 0, "epoch": 0, "from": 10,
                                                    "to": 12, "records": 2, "files": []})
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("discontinuity", out)

    def test_declared_gap_is_not_a_discontinuity(self):
        write_day(self.root, n=3, base=0)
        ria.append_line(self.root / "_runs.jsonl", {"topic": "t.topic", "partition": 0, "epoch": 0, "from": 10,
                                                    "to": 12, "records": 2, "files": [], "gap": {"from": 3, "to": 10}})
        rc, out = self.v()
        self.assertEqual(rc, 0, out)

    def test_missing_schema_is_a_problem(self):
        write_day(self.root, store_schema=False)
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("schema ids never fetched", out)

    def test_manifest_line_without_a_committed_run_is_a_crash_tail(self):
        write_day(self.root)
        (self.root / "_runs.jsonl").write_text("")          # the checkpoint never landed
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("not referenced by a committed run", out)

    def test_duplicate_reread_is_flagged(self):
        ddir, ent = write_day(self.root)
        dup = dict(ent, path=ent["path"].replace("20261102T230000Z", "20261102T231000Z"))
        (ddir / dup["path"]).write_bytes((ddir / ent["path"]).read_bytes())
        ria.append_line(ddir / "_manifest.jsonl", dup)
        ria.append_line(self.root / "_runs.jsonl", {"topic": "t.topic", "partition": 0, "epoch": 0, "from": 3, "to": 3,
                                                    "records": 0, "files": [{"dt": "2026-11-02", "path": dup["path"], "sha256": dup["sha256"]}]})
        ria.write_day_manifests(ddir)
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("overlapping/duplicate offsets", out)

    def test_recover_drops_the_tail_of_a_crashed_run_and_keeps_committed_data(self):
        ddir, ent = write_day(self.root)
        crashed = ent["path"].replace("20261102T230000Z", "20261102T235900Z")
        (ddir / crashed).write_bytes((ddir / ent["path"]).read_bytes())
        ria.append_line(ddir / "_manifest.jsonl", dict(ent, path=crashed))     # listed, never committed
        (self.root / "_state" / "tmp").mkdir(parents=True)
        ria.recover(self.root, ria.load_runs(self.root))
        self.assertFalse((ddir / crashed).exists())
        self.assertTrue((ddir / ent["path"]).exists())
        rc, out = self.v()
        self.assertEqual(rc, 0, out)

    def test_recover_never_deletes_committed_data_missing_from_the_manifest(self):
        ddir, ent = write_day(self.root)
        (ddir / "_manifest.jsonl").write_text("")
        (self.root / "_state" / "tmp").mkdir(parents=True)
        ria.recover(self.root, ria.load_runs(self.root))
        self.assertTrue((ddir / ent["path"]).exists())
        rc, out = self.v()
        self.assertEqual(rc, 2)        # no manifest entries: flagged, not destroyed

    def test_truncated_final_checkpoint_line_is_cut_and_earlier_corruption_is_fatal(self):
        write_day(self.root)
        p = self.root / "_runs.jsonl"
        good = p.read_text()
        p.write_text(good + '{"topic":"t.to')
        self.assertEqual(len(ria.load_runs(self.root)), 1)
        self.assertEqual(len(ria.load_runs(self.root, repair=True)), 1)
        self.assertEqual(p.read_text(), good)
        p.write_text('{bad}\n' + good)
        with self.assertRaises(SystemExit):
            ria.load_runs(self.root)

    def test_recover_rebuilds_sidecar_manifests_after_a_crash_past_the_commit(self):
        ddir, _ = write_day(self.root)
        (ddir / "MANIFEST.sha256").unlink()
        (ddir / "MANIFEST.md5").write_text("stale\n")
        (self.root / "_state" / "tmp").mkdir(parents=True)
        ria.recover(self.root, ria.load_runs(self.root))
        rc, out = self.v()
        self.assertEqual(rc, 0, out)

    def test_recover_survives_a_torn_day_manifest_line(self):
        ddir, _ = write_day(self.root)
        with (ddir / "_manifest.jsonl").open("a") as f:
            f.write('{"path":"t.top')
        (self.root / "_state" / "tmp").mkdir(parents=True)
        ria.recover(self.root, ria.load_runs(self.root))
        rc, out = self.v()
        self.assertEqual(rc, 0, out)

    def test_verify_fails_on_a_torn_manifest_tail(self):
        ddir, _ = write_day(self.root)
        with (ddir / "_manifest.jsonl").open("a") as f:
            f.write('{"path":"t.top')
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("torn line", out)

    def test_verify_reports_a_manifest_that_is_only_a_torn_line(self):
        ddir, _ = write_day(self.root)
        (ddir / "_manifest.jsonl").write_text('{"path":"t.top')
        rc, out = self.v()
        self.assertEqual(rc, 2)
        self.assertIn("torn line", out)

    def test_check_refuses_a_torn_or_empty_manifest(self):
        ddir, _ = write_day(self.root)
        with (ddir / "_manifest.jsonl").open("a") as f:
            f.write('{"path":"t.top')
        with self.assertRaises(SystemExit):
            ria.day_entries(self.root, "2026-11-02")
        (ddir / "_manifest.jsonl").write_text("")
        if fastavro:
            rc, out = run(ria.check, root=str(self.root), dt="2026-11-02", broker=False, bootstrap="x")
            self.assertEqual(rc, 2)

    def test_stale_manifest_file_is_caught(self):
        ddir, _ = write_day(self.root)
        (ddir / "MANIFEST.sha256").write_text("")
        rc, out = self.v()
        self.assertEqual(rc, 1)
        self.assertIn("MANIFEST.sha256 missing or stale", out)


@unittest.skipIf(fastavro is None, "fastavro not installed")
class Decode(unittest.TestCase):
    def test_check_decodes_with_archived_schema_and_flags_unknown_schema(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            ddir, _ = write_day(root, n=4, sid=7)
            rc, out = run(ria.check, root=str(root), dt="2026-11-02", broker=False, bootstrap="x")
            self.assertEqual(rc, 0, out)
            self.assertIn("records=4 decoded=4", out)
            (ddir / "_schemas" / "7.json").unlink()
            rc, out = run(ria.check, root=str(root), dt="2026-11-02", broker=False, bootstrap="x")
            self.assertEqual(rc, 1)
            self.assertIn("not archived", out)

    def test_tombstone_with_undecodable_framed_key_fails_check(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            ddir, ent = write_day(root, n=1, sid=7)
            doc = json.loads((ddir / "_schemas" / "7.json").read_text())
            p = ddir / ent["path"]
            bad_key = base64.b64encode(b"\x00\x00\x00\x00\x07").decode()   # framed with schema 7, empty body
            line = json.dumps({"p": 0, "o": 0, "ts": 1, "tt": 1, "k": bad_key, "v": None, "h": []}) + "\n"
            with gzip.open(p, "wt", encoding="ascii") as f:
                f.write(line)
            rc, out = run(ria.check, root=str(root), dt="2026-11-02", broker=False, bootstrap="x")
            self.assertEqual(rc, 1, out)

    def test_show_renders_tombstones_and_refuses_empty_day(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            ddir, ent = write_day(root, n=1)
            with gzip.open(ddir / ent["path"], "wt", encoding="ascii") as f:
                f.write(json.dumps({"p": 0, "o": 0, "ts": 1, "tt": 1, "k": None, "v": None, "h": []}) + "\n")
            rc, out = run(ria.show, root=str(root), dt="2026-11-02", topic="t.topic", n=5)
            self.assertEqual(rc, 0)
            self.assertIn('"value": null', out)
            (ddir / "_manifest.jsonl").write_text("")
            rc, _ = run(ria.show, root=str(root), dt="2026-11-02", topic="t.topic", n=5)
            self.assertEqual(rc, 2)

    def test_nested_references_load_dependency_first(self):
        inner = {"type": "record", "name": "Inner", "namespace": "t", "fields": [{"name": "a", "type": "long"}]}
        mid = {"type": "record", "name": "Mid", "namespace": "t", "fields": [{"name": "i", "type": "t.Inner"}]}
        top = {"type": "record", "name": "Top", "namespace": "t", "fields": [{"name": "m", "type": "t.Mid"}]}
        with tempfile.TemporaryDirectory() as d:
            ddir = Path(d) / "dt=2026-11-02"
            (ddir / "_schemas").mkdir(parents=True)
            (ddir / "_schemas" / "9.json").write_text(json.dumps({
                "id": 9, "schemaType": "AVRO", "schema": json.dumps(top), "subjects": [],
                "references": [{"name": "t.Inner", "schema": json.dumps(inner)}, {"name": "t.Mid", "schema": json.dumps(mid)}]}))
            parsed = ria.load_schemas(ddir)
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, parsed[9], {"m": {"i": {"a": 5}}})
            kind, val = ria.decode_value(parsed, b"\x00" + (9).to_bytes(4, "big") + buf.getvalue())
            self.assertEqual((kind, val), (9, {"m": {"i": {"a": 5}}}))


if __name__ == "__main__":
    unittest.main()
