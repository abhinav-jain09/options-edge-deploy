"""Tests of oe-archive-session-chain.py and of bucket_split() in oe-archive-kafka.sh (A-GAP-05).
Run: python3 test_archive_session_chain.py   (stdlib only; the shell side is run through bash)."""
import datetime, gzip, importlib.util, json, os, subprocess, sys, tempfile, unittest
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("chain", os.path.join(HERE, "oe-archive-session-chain.py"))
chain = importlib.util.module_from_spec(spec); spec.loader.exec_module(chain)
NY = ZoneInfo("America/New_York")


def ms(y, mo, d, h, mi=0, s=0):
    return int(datetime.datetime(y, mo, d, h, mi, s, tzinfo=NY).timestamp() * 1000)


VECTORS = [  # (NY local time, expected trade date)
    ((2026, 11, 5, 17, 59, 59), "2026-11-05"), ((2026, 11, 5, 18, 0, 0), "2026-11-06"),
    ((2026, 11, 6, 17, 59, 59), "2026-11-06"), ((2026, 11, 6, 18, 0, 0), "2026-11-09"),
    ((2026, 11, 7, 12, 0, 0), "2026-11-09"), ((2026, 11, 7, 23, 59, 59), "2026-11-09"),
    ((2026, 11, 8, 0, 0, 0), "2026-11-09"), ((2026, 11, 8, 17, 59, 59), "2026-11-09"),
    ((2026, 11, 8, 18, 0, 0), "2026-11-09"), ((2026, 11, 9, 17, 59, 59), "2026-11-09"),
    ((2026, 11, 9, 18, 0, 0), "2026-11-10"),
    ((2026, 11, 1, 17, 59, 0), "2026-11-02"),   # the Sunday the clocks fall back
    ((2026, 11, 2, 18, 0, 0), "2026-11-03"),
    ((2027, 3, 14, 18, 0, 0), "2027-03-15"),    # the Sunday the clocks spring forward
    ((2027, 3, 12, 18, 0, 0), "2027-03-15"),    # the Friday before it
    ((2026, 12, 31, 18, 0, 0), "2027-01-01"),
]


class TradeDate(unittest.TestCase):
    def test_python_vectors(self):
        for t, want in VECTORS:
            self.assertEqual(chain.trade_date(ms(*t)).isoformat(), want, t)

    def test_the_archivers_bucket_split_agrees_on_every_vector(self):
        """bucket_split() (shell + inline Python in oe-archive-kafka.sh) and trade_date() here are one rule."""
        lines = []
        for i, (t, _) in enumerate(VECTORS):
            lines.append("CreateTime:1\tPartition:0\tOffset:%d\tk\t{\"bucketEnd\":%d}" % (i, ms(*t)))
        with tempfile.TemporaryDirectory() as d:
            plain = os.path.join(d, "plain")
            with open(plain, "w") as fh: fh.write("\n".join(lines) + "\n")
            script = 'source <(sed -n "/^bucket_split() {/,/^}/p" "$1"); bucket_split "$2" "$3"'
            out = subprocess.run(["bash", "-c", script, "x", os.path.join(HERE, "oe-archive-kafka.sh"), plain, os.path.join(d, "seg")],
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            # reconstruct the per-record date from the segments
            got = []
            for row in out.stdout.strip().split("\n"):
                idx, day, first, n = row.split("\t")[:4]
                got += [day] * int(n)
            # adjacent equal dates merge into one segment, so compare the expanded list
            self.assertEqual(got, [w for _, w in VECTORS])

    def test_split_descriptors_and_files(self):
        recs = [(5, ms(2026, 11, 5, 17, 59, 58)), (6, ms(2026, 11, 5, 17, 59, 59)), (9, ms(2026, 11, 5, 18, 0, 0)),
                (10, ms(2026, 11, 5, 18, 0, 1)), (11, ms(2026, 11, 5, 17, 0, 0)),   # a stray earlier record
                (12, ms(2026, 11, 6, 18, 0, 0))]
        lines = ["CreateTime:1\tPartition:0\tOffset:%d\tk\t{\"bucketEnd\":%d}" % r for r in recs]
        lines.insert(3, "CreateTime:%d\tPartition:0\tOffset:99\tk\t{\"noBucket\":1}" % ms(2026, 11, 5, 18, 0, 0))
        with tempfile.TemporaryDirectory() as d:
            plain = os.path.join(d, "plain")
            with open(plain, "w") as fh: fh.write("\n".join(lines) + "\n")
            script = 'source <(sed -n "/^bucket_split() {/,/^}/p" "$1"); bucket_split "$2" "$3"'
            out = subprocess.run(["bash", "-c", script, "x", os.path.join(HERE, "oe-archive-kafka.sh"), plain, os.path.join(d, "seg")],
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            rows = [r.split("\t") for r in out.stdout.strip().split("\n")]
            self.assertEqual([(r[1], r[2], r[3]) for r in rows],
                             [("2026-11-05", "5", "2"), ("2026-11-06", "9", "3"), ("2026-11-05", "11", "1"), ("2026-11-09", "12", "1")])
            # the record without a bucketEnd stayed in the file it fell in and is counted
            self.assertEqual([r[7] for r in rows], ["0", "1", "0", "0"])
            total = 0
            for i in range(4):
                with open(os.path.join(d, "seg%d" % i)) as fh: total += len(fh.read().strip().split("\n"))
            self.assertEqual(total, len(lines))   # nothing dropped, nothing doubled

    def test_a_malformed_line_fails_the_cut(self):
        with tempfile.TemporaryDirectory() as d:
            plain = os.path.join(d, "plain")
            with open(plain, "w") as fh: fh.write("not five fields\n")
            script = 'source <(sed -n "/^bucket_split() {/,/^}/p" "$1"); bucket_split "$2" "$3"'
            out = subprocess.run(["bash", "-c", script, "x", os.path.join(HERE, "oe-archive-kafka.sh"), plain, os.path.join(d, "seg")],
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 3)


MON = datetime.date(2026, 11, 9)
START = ms(2026, 11, 8, 18)            # session start instant of Monday
TERM = ms(2026, 11, 10, 9, 30)         # next trade date 09:30
UTC = datetime.timezone.utc


def wall(ms_):
    return datetime.datetime.fromtimestamp(ms_ / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Archive:
    def __init__(self, d, env="dev"):
        self.root = d; self.env = env

    def add(self, topic, dt, frm, to, stamp, start=0, begin=0, ws=None, we=None, maxbe=None, legacy=False, partition=0, extra=None):
        p = os.path.join(self.root, "kafka", self.env, topic, "dt=" + dt)
        os.makedirs(p, exist_ok=True)
        e = {"topic": topic, "dt": dt, "partition": partition, "offset_from": frm, "offset_to": to, "records": to - frm,
             "file": "%s.p%d.%d-%d.%s.jsonl.gz" % (topic, partition, frm, to, stamp), "archived_at": stamp}
        if not legacy:
            e.update({"capture_start_checkpoint": start, "broker_beginning_offset": begin,
                      "capture_wall_start": wall(ws), "capture_wall_end": wall(we if we else ws + 60000)})
            if maxbe is not None:
                e.update({"first_bucket_end": maxbe - 1000, "last_bucket_end": maxbe, "max_bucket_end": maxbe})
        if extra:
            e.update(extra)
        with open(os.path.join(p, "_manifest.jsonl"), "a") as f:
            f.write(json.dumps(e) + "\n")
        with gzip.open(os.path.join(p, e["file"]), "wb") as g:
            g.write(("CreateTime:1\tPartition:0\tOffset:%d\tk\t{\"bucketEnd\":%d}\n" % (frm, maxbe or 1)).encode())
        return e

    def all_topics(self, fn):
        for t in chain.BOOK_TOPICS:
            fn(t)


def report(a, strict=False, scan=False):
    d = chain.session_report(a.root, a.env, chain.BOOK_TOPICS, MON, chain.Cal(), scan)
    return d


def healthy(a):
    """Three captures: one before the session start, one inside, one after the terminal."""
    def f(t):
        a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=START - 10_000)
        a.add(t, "2026-11-09", 1000, 5000, "S2", start=1000, begin=0, ws=START + 6 * 3600_000, maxbe=ms(2026, 11, 9, 12))
        a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 5000)
    a.all_topics(f)


class Chain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.a = Archive(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_clean_chain_is_complete(self):
        healthy(self.a)
        r = report(self.a)
        self.assertEqual(r["status"], "BOOK_COMPLETE", r)

    def test_duplicates_are_not_holes(self):
        healthy(self.a)
        self.a.add("es.mbo.health", "2026-11-09", 1000, 5000, "S2b", start=1000, begin=0, ws=START + 6 * 3600_000 + 1, maxbe=ms(2026, 11, 9, 12))
        self.assertEqual(report(self.a)["status"], "BOOK_COMPLETE")

    def test_missing_topic(self):
        healthy(self.a)
        import shutil; shutil.rmtree(os.path.join(self.a.root, "kafka", "dev", "es.mbo.closures"))
        r = report(self.a)
        self.assertEqual(r["status"], "BOOK_INCOMPLETE"); self.assertIn("NO_FILES", r["topics"]["es.mbo.closures"])

    def test_file_hole(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=START - 10_000)
            a.add(t, "2026-11-09", 1200, 5000, "S2", start=1200, begin=0, ws=START + 3600_000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 1)
        a.all_topics(f)
        r = report(a)
        self.assertEqual(r["status"], "BOOK_INCOMPLETE")
        self.assertTrue(any("FILE_HOLE[1000,1200)" in w for w in r["topics"]["es.mbo.health"]), r["topics"]["es.mbo.health"])
        self.assertTrue(any("CAPTURE_CHAIN_BREAK" in w for w in r["topics"]["es.mbo.health"]))

    def test_capture_chain_break_alone(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=START - 10_000)
            a.add(t, "2026-11-09", 1000, 5000, "S2", start=900, begin=0, ws=START + 3600_000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 1)
        a.all_topics(f)
        w = report(a)["topics"]["es.mbo.health"]
        self.assertTrue(any(x.startswith("p0:CAPTURE_CHAIN_BREAK:S2") for x in w), w)
        self.assertTrue(any("CAPTURE_CLAMPED:S2" in x for x in w), w)

    def test_no_capture_before_the_session_start(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-09", 0, 5000, "S2", start=0, begin=0, ws=START + 600_000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 1)
        a.all_topics(f)
        r = report(a)
        self.assertTrue(any("NO_CAPTURE_BEFORE_SESSION_START" in w for w in r["topics"]["es.mbo.health"]))

    def test_terminal_not_reached(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=START - 10_000)
            a.add(t, "2026-11-09", 1000, 5000, "S2", start=1000, begin=0, ws=START + 3600_000, maxbe=ms(2026, 11, 9, 12))
        a.all_topics(f)
        r = report(a)
        self.assertIn("p0:TERMINAL_NOT_REACHED", r["topics"]["es.mbo.level-flow"])

    def test_the_boundary_of_the_start_instant_is_inclusive(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 5000, "S1", start=0, begin=0, ws=START, we=START + 1000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM)
        a.all_topics(f)
        self.assertEqual(report(a)["status"], "BOOK_COMPLETE")   # wall start == start instant, max_bucket_end == terminal
        # one second later the first capture no longer started before the session
        import shutil; shutil.rmtree(os.path.join(a.root, "kafka"))
        def g(t):
            a.add(t, "2026-11-08", 0, 5000, "S1", start=0, begin=0, ws=START + 1000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM - 1)
        a.all_topics(g)
        r = report(a)
        self.assertEqual(r["status"], "BOOK_INCOMPLETE")
        self.assertTrue(any("NO_CAPTURE_BEFORE_SESSION_START" in w for w in r["topics"]["es.mbo.health"]))

    def test_legacy_lines_never_make_a_session_complete(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-07", 0, 5000, "L1", legacy=True, ws=0, maxbe=None)
        a.all_topics(f)
        r = report(a, scan=True)
        self.assertEqual(r["status"], "BOOK_INCOMPLETE")
        self.assertTrue(any("LEGACY_CAPTURE" in w for w in r["topics"]["es.mbo.health"]))

    def test_the_broker_beginning_must_not_pass_the_starting_checkpoint(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=START - 10_000)
            a.add(t, "2026-11-09", 1000, 5000, "S2", start=1000, begin=1200, ws=START + 3600_000, maxbe=ms(2026, 11, 9, 12))
            a.add(t, "2026-11-10", 5000, 9000, "S3", start=5000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 1)
        a.all_topics(f)
        self.assertTrue(any("BROKER_BEGINNING_AFTER_START_CHECKPOINT:S2" in w for w in report(a)["topics"]["es.mbo.health"]))

    def test_wall_clock_end_before_start_is_refused(self):
        a = self.a
        def f(t):
            a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, we=START - 7200_000, maxbe=START - 10_000)
            a.add(t, "2026-11-10", 1000, 9000, "S3", start=1000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=TERM + 1)
        a.all_topics(f)
        self.assertTrue(any("CAPTURE_WALL_CLOCK:S1" in w for w in report(a)["topics"]["es.mbo.health"]))

    def test_closed_day_holds_no_session(self):
        r = chain.session_report(self.a.root, "dev", chain.BOOK_TOPICS, datetime.date(2026, 11, 28), chain.Cal(), False)   # a Saturday
        self.assertEqual(r["status"], "NO_SESSION")
        cal = chain.Cal()
        if cal.name == "market_calendar.py":
            r = chain.session_report(self.a.root, "dev", chain.BOOK_TOPICS, datetime.date(2026, 11, 26), cal, False)   # Thanksgiving
            self.assertEqual(r["status"], "NO_SESSION")

    def test_closures_holes_between_records_do_not_matter(self):
        """The BOOK closures (3501) leave holes between record offsets; the chain is over ranges, so a closures
        file whose records are sparse is complete when its ranges chain."""
        healthy(self.a)
        p = os.path.join(self.a.root, "kafka", "dev", "es.mbo.closures", "dt=2026-11-09")
        for fn in os.listdir(p):
            if fn.endswith(".gz"):
                with gzip.open(os.path.join(p, fn), "wb") as g:   # only two records for a 4000-offset range
                    g.write(b"CreateTime:1\tPartition:0\tOffset:1000\tk\t{}\nCreateTime:1\tPartition:0\tOffset:4999\tk\t{}\n")
        self.assertEqual(report(self.a)["status"], "BOOK_COMPLETE")

    def test_unparseable_manifest_line_is_incomplete(self):
        healthy(self.a)
        p = os.path.join(self.a.root, "kafka", "dev", "es.mbo.health", "dt=2026-11-09", "_manifest.jsonl")
        with open(p, "a") as fh: fh.write("{not json\n")
        w = report(self.a)["topics"]["es.mbo.health"]
        self.assertTrue(any(x.startswith("MANIFEST_UNPARSEABLE") for x in w), w)

    def test_cli_strict_exit_code(self):
        healthy(self.a)
        py = os.path.join(HERE, "oe-archive-session-chain.py")
        ok = subprocess.run([sys.executable, py, "--root", self.a.root, "--session", "2026-11-09", "--strict"], capture_output=True, text=True)
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr); self.assertIn("BOOK_COMPLETE", ok.stdout)
        bad = subprocess.run([sys.executable, py, "--root", self.a.root, "--session", "2026-11-10", "--strict"], capture_output=True, text=True)
        self.assertEqual(bad.returncode, 1); self.assertIn("BOOK_INCOMPLETE", bad.stdout)
        usage = subprocess.run([sys.executable, py, "--root", self.a.root, "--session", "nonsense"], capture_output=True, text=True)
        self.assertEqual(usage.returncode, 2)

    def test_scan_legacy_reads_bounds_from_the_file(self):
        a = self.a
        def f(t):
            e = a.add(t, "2026-11-08", 0, 1000, "S1", start=0, begin=0, ws=START - 3600_000, maxbe=None)   # no bucket bounds in the manifest
            a.add(t, "2026-11-10", 1000, 9000, "S3", start=1000, begin=0, ws=ms(2026, 11, 10, 14), maxbe=None)
            # the files hold bucketEnd 1 (the helper), so even scanning cannot reach the terminal: still incomplete, with the right reason
        a.all_topics(f)
        w = report(a, scan=True)["topics"]["es.mbo.health"]
        self.assertIn("p0:TERMINAL_NOT_REACHED", w)
        w2 = report(a, scan=False)["topics"]["es.mbo.health"]
        self.assertTrue(any("NO_BUCKET_BOUNDS" in x for x in w2), w2)


if __name__ == "__main__":
    unittest.main(verbosity=1)
