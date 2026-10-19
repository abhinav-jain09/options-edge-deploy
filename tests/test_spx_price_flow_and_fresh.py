"""verify-flow.sh and assert-target-fresh.sh against stand-in Kafka CLIs. The point of verify-flow: an advancing TARGET offset is not proof the
mirror moves data; the mirror's own group must advance, its lag stay bounded, and its last flushed source record appear on the target."""
import json, os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
D = ROOT / "scripts/kafka/spx-price-mirror"

STUB = r'''#!/usr/bin/env python3
import json, os, sys
name = os.path.basename(sys.argv[0]); argv = sys.argv[1:]
sc = json.load(open(os.environ["SC"])); st = os.path.join(os.path.dirname(os.environ["SC"]), "state")
n = int(open(st).read()) if os.path.exists(st) else 0
arg = lambda f: argv[argv.index(f) + 1] if f in argv else None
bs, t = arg("--bootstrap-server"), arg("--topic")
if name == "kafka-get-offsets":
    if t == "src": print(f"src:0:{sc['src_end'][min(n, 1)]}")
    else: print(f"tgt:0:{sc['tgt_end']}")
elif name == "kafka-consumer-groups":
    if sc["group"] is None: sys.exit(0)
    c = sc["group"][min(n, 1)]
    print("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG")
    print(f"g src 0 {c} {sc['src_end'][min(n, 1)]} {sc['lag']}")
elif name == "tool":
    open(os.path.join(os.path.dirname(os.environ["SC"]), "tool.args"), "w").write(" ".join(argv))
    sys.exit(0 if sc["found"] else 1)
elif name == "sleep":
    open(st, "w").write(str(n + 1))
'''


def setup(**sc):
    d = pathlib.Path(tempfile.mkdtemp()); kb = d / "kb"; kb.mkdir()
    base = dict(src_end=[100, 110], tgt_end=50, group=[100, 110], lag=0, found=True)
    base.update(sc)
    (d / "sc.json").write_text(json.dumps(base))
    for n in ("kafka-get-offsets", "kafka-consumer-groups", "tool", "sleep"):
        (kb / n).write_text(STUB); os.chmod(kb / n, 0o755)
    return d, kb


def flow(**sc):
    d, kb = setup(**sc)
    env = dict(os.environ, SC=str(d / "sc.json"), PATH=f"{kb}:{os.environ['PATH']}", FLOW_WAIT_S="0", SPX_MIRROR_CMD=str(kb / "tool"))
    r = subprocess.run(["bash", str(D / "verify-flow.sh"), "sb", "http://s", "src", "tb", "http://t", "tgt", "g", str(kb)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


def fresh(**sc):
    d, kb = setup(**sc)
    env = dict(os.environ, SC=str(d / "sc.json"), PATH=f"{kb}:{os.environ['PATH']}")
    r = subprocess.run(["bash", str(D / "assert-target-fresh.sh"), "tb", "tgt", "g", "sb", "src", str(kb)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class VerifyFlow(unittest.TestCase):
    def test_group_advances_and_last_flushed_record_is_on_the_target(self):
        rc, out = flow()
        self.assertEqual(rc, 0, out)
        self.assertIn("MIRROR FLOWING", out)

    def test_source_advances_but_group_does_not_is_a_failure_even_if_the_target_moves(self):
        rc, out = flow(group=[100, 100], tgt_end=999)
        self.assertEqual(rc, 1, out)
        self.assertIn("not consuming", out)

    def test_lag_over_the_bound_fails(self):
        self.assertEqual(flow(lag=5000)[0], 1)

    def test_record_not_found_on_the_target_fails(self):
        rc, out = flow(found=False)
        self.assertEqual(rc, 1, out)
        self.assertIn("not on the target", out)

    def test_the_lookup_is_by_timestamp_through_the_tool_for_the_last_flushed_offset(self):
        d, kb = setup()
        env = dict(os.environ, SC=str(d / "sc.json"), PATH=f"{kb}:{os.environ['PATH']}", FLOW_WAIT_S="0", SPX_MIRROR_CMD=str(kb / "tool"))
        r = subprocess.run(["bash", str(D / "verify-flow.sh"), "sb", "http://s", "src", "tb", "http://t", "tgt", "g", str(kb)], capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual((d / "tool.args").read_text(), "flow-check sb http://s src 0 109 tb http://t tgt")

    def test_no_committed_group_while_the_source_advances_fails(self):
        self.assertEqual(flow(group=None)[0], 1)

    def test_no_committed_group_on_a_flat_source_is_inconclusive(self):
        self.assertEqual(flow(group=None, src_end=[100, 100])[0], 3)

    def test_flat_source_is_inconclusive_not_flowing(self):
        rc, out = flow(src_end=[100, 100], group=[100, 100])
        self.assertEqual(rc, 3, out)


class TargetFresh(unittest.TestCase):
    def test_empty_target_passes(self):
        self.assertEqual(fresh(tgt_end=0)[0], 0)

    def test_non_empty_target_without_a_mirror_group_fails(self):
        rc, out = fresh(tgt_end=7, group=None)
        self.assertEqual(rc, 1)
        self.assertIn("not written by this mirror", out)

    def test_non_empty_target_of_an_existing_mirror_group_passes(self):
        self.assertEqual(fresh(tgt_end=7)[0], 0)


if __name__ == "__main__":
    unittest.main()
