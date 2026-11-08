# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
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
        calls = iter([self.R(0), self.R(1, b"", b"Broker: Request timed out")])
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        self.assertIn("timed out", str(c.exception))

    def test_an_unknown_topic_is_absent_not_failed_but_other_errors_still_fail(self):
        calls = iter([self.R(0), self.R(1, b"", b"% ERROR: Topic t error: Broker: Unknown topic or partition")])
        with self.assertRaises(ev.TopicAbsent):
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        calls = iter([self.R(0), self.R(1, b"", b"Broker: Topic authorization failed")])
        with self.assertRaises(RuntimeError) as c:
            ev.sample("b:9092", "t", runner=lambda argv, t: next(calls))
        self.assertNotIsInstance(c.exception, ev.TopicAbsent)

    def test_an_unknown_topic_at_the_metadata_probe_is_also_absent(self):
        with self.assertRaises(ev.TopicAbsent):
            ev.sample("b:9092", "t", runner=lambda argv, t: self.R(1, b"", b"% ERROR: Topic t error: Broker: Unknown topic or partition"))

    def test_a_registry_error_other_than_404_is_not_read_as_no_subjects(self):
        import io, json as _j, urllib.error
        responses = {"/schemas/ids/7": _j.dumps({"schema": _j.dumps({"type": "record", "name": "Flow", "namespace": "com.x"})})}
        class Resp(io.BytesIO):
            def __enter__(self): return self
            def __exit__(self, *a): return False
        def fake(code):
            def urlopen(url, timeout=15):
                path = url.split("//", 1)[1].split("/", 1)[1]
                if path.endswith("/versions"):
                    raise urllib.error.HTTPError(url, code, "x", {}, None)
                return Resp(responses["/" + path].encode())
            return urlopen
        real = ev.urllib.request.urlopen
        try:
            ev.urllib.request.urlopen = fake(500)
            with self.assertRaises(urllib.error.HTTPError):
                ev.make_lookup("http://reg")(7)
            ev.urllib.request.urlopen = fake(404)
            self.assertEqual(ev.make_lookup("http://reg")(7), ("com.x.Flow", []))
        finally:
            ev.urllib.request.urlopen = real

    def test_an_unreachable_broker_is_an_error(self):
        with self.assertRaises(RuntimeError):
            ev.sample("b:9092", "t", runner=lambda argv, t: self.R(1, b"", b"transport failure"))

    def test_a_successful_empty_read_is_an_empty_sample_and_a_record_is_returned(self):
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"")), b"")
        self.assertEqual(ev.sample("b", "t", runner=lambda argv, t: self.R(0, b"\x00abcdefghijklmnopqrstuv")), b"\x00abcdefghijklmno")


class Evaluate(unittest.TestCase):
    MIR = {"from": "es4", "subjects": [], "unframed": True}

    def absent(self, b, t): raise ev.TopicAbsent("gone")

    def test_an_absent_topic_fails_unless_listed(self):
        self.assertEqual(ev.evaluate(self.MIR, "t", set(), self.absent, lambda: None, "b")[0], "FAIL")
        self.assertEqual(ev.evaluate(self.MIR, "t", {("es4", "t")}, self.absent, lambda: None, "b")[0], "ABSENT")

    def test_a_listed_topic_that_exists_again_fails_until_removed(self):
        s, d = ev.evaluate(self.MIR, "t", {("es4", "t")}, lambda b, t: b'{"a":1}', lambda: None, "b")
        self.assertEqual(s, "FAIL"); self.assertIn("remove it from the list", d)
        self.assertEqual(ev.evaluate(self.MIR, "t", set(), lambda b, t: b'{"a":1}', lambda: None, "b")[0], "PASS")

    def test_a_read_error_fails_even_when_listed(self):
        def boom(b, t): raise RuntimeError("timed out")
        self.assertEqual(ev.evaluate(self.MIR, "t", {("es4", "t")}, boom, lambda: None, "b")[0], "FAIL")


if __name__ == "__main__":
    unittest.main()
