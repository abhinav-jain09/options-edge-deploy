import importlib.util, os, unittest
spec = importlib.util.spec_from_file_location("ev", os.path.join(os.path.dirname(__file__), "../scripts/ci/schema-mirror-evidence.py"))
ev = importlib.util.module_from_spec(spec); spec.loader.exec_module(ev)
FRAMED = b"\x00\x00\x00\x0a\x8d" + b"rest"
LOOK = lambda sid: ("com.x.Flow", ["flow-value"]) if sid == 2701 else ("com.x.Other", ["other-value"])


class Evidence(unittest.TestCase):
    def test_no_record_is_unmeasured_never_pass(self):
        self.assertEqual(ev.classify(None, set(), True, LOOK)[0], "UNMEASURED")
        self.assertEqual(ev.classify(b"", {"com.x.Flow"}, False, LOOK)[0], "UNMEASURED")

    def test_unframed_topic_with_json_passes_and_with_a_frame_fails(self):
        self.assertEqual(ev.classify(b'{"schemaVers', set(), True, LOOK)[0], "PASS")
        self.assertEqual(ev.classify(FRAMED, set(), True, LOOK)[0], "FAIL")

    def test_framed_topic_must_resolve_to_a_declared_subject(self):
        self.assertEqual(ev.classify(FRAMED, {"com.x.Flow"}, False, LOOK)[0], "PASS")
        self.assertEqual(ev.classify(FRAMED, {"flow-value"}, False, LOOK)[0], "PASS")
        s, d = ev.classify(FRAMED, {"com.x.Different"}, False, LOOK)
        self.assertEqual(s, "FAIL"); self.assertIn("com.x.Flow", d)

    def test_framed_declaration_over_unframed_data_fails(self):
        self.assertEqual(ev.classify(b'{"a":1}', {"com.x.Flow"}, False, LOOK)[0], "FAIL")


class Sampling(unittest.TestCase):
    class R:
        def __init__(self, rc, out=b"", err=b""): self.returncode, self.stdout, self.stderr = rc, out, err

    def test_a_failed_read_is_an_error_never_an_empty_sample(self):
        calls = iter([self.R(0), self.R(1, b"", b"Broker: Unknown topic or partition")])
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        self.assertIn("Unknown topic", str(c.exception))

    def test_an_unreachable_broker_is_an_error(self):
        with self.assertRaises(RuntimeError):
            ev.sample("b:9092", "t", runner=lambda argv, t: self.R(1, b"", b"transport failure"))

    def test_a_successful_empty_read_is_an_empty_sample_and_a_record_is_returned(self):
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"")), b"")
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"\x00abcdefghijklmnopqrstuv")), b"\x00abcdefghijklmno")


if __name__ == "__main__":
    unittest.main()
