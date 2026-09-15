"""Partition counts dev can no longer get wrong (2026-09-15 partition audit).

OPTIONS_EDGE_PARTITION_ONLY_TOPICS lists every application topic dev needs that is not declared in
OPTIONS_EDGE_TOPICS, with one partition count. dev-cleanup.sh creates the missing ones at that count
before any service starts and grows smaller copies, touching no config. These tests evaluate the real
topics.env and run the real shell function against a fake kafka-topics.
"""
import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOPICS_ENV = ROOT / "scripts" / "kafka" / "topics.env"
DEV_CLEANUP = ROOT / "scripts" / "ops" / "dev-cleanup.sh"
BASH = "/bin/bash" if Path("/bin/bash").exists() else "bash"


def env_var(name):
    out = subprocess.run([BASH, "-c", f'source "{TOPICS_ENV}" >/dev/null 2>&1; printf "%s" "${{{name}:-}}"'],
                         capture_output=True, text=True, check=True).stdout
    return out.split()


def counts(entries):
    return {e.rsplit(":", 1)[0]: int(e.rsplit(":", 1)[1]) for e in entries}


FAKE_KT = textwrap.dedent(
    r'''
    import json, sys
    S = sys.argv[1]; args = sys.argv[2:]
    st = json.load(open(S)); st["calls"].append(args)
    def arg(k): return args[args.index(k) + 1]
    if "--describe" in args and "--topic" not in args:
        for t, n in sorted(st["topics"].items()):
            print(f"Topic: {t}\tTopicId: x\tPartitionCount: {n}\tReplicationFactor: 1\tConfigs: cleanup.policy=compact")
            for p in range(n): print(f"\tTopic: {t}\tPartition: {p}\tLeader: 1")
    elif "--create" in args:
        st["topics"].setdefault(arg("--topic"), int(arg("--partitions")))
    elif "--alter" in args:
        import re
        for name in [n for n in st["topics"] if re.fullmatch(arg("--topic"), n)]:   # --topic is a regex, as in kafka-topics
            st["topics"][name] = int(arg("--partitions"))
    json.dump(st, open(S, "w"))
    '''
)


class PartitionOnlyDeclarationTest(unittest.TestCase):
    def test_entries_are_unique_positive_and_not_declared_twice(self):
        entries = env_var("OPTIONS_EDGE_PARTITION_ONLY_TOPICS")
        self.assertGreater(len(entries), 40)
        names = [e.rsplit(":", 1)[0] for e in entries]
        self.assertEqual(len(names), len(set(names)), "a topic listed twice")
        for e in entries:
            name, _, n = e.rpartition(":")
            self.assertTrue(n.isdigit() and int(n) >= 1, e)
            self.assertNotIn("-prod-", name, "dev-cleanup must not create prod-named app topics on dev")
        declared = set(counts(env_var("OPTIONS_EDGE_TOPICS")))
        self.assertEqual(sorted(set(names) & declared), [], "declare a topic in ONE list only")

    def test_no_topic_whose_owner_refuses_a_config_less_create(self):
        # These owners verify an existing topic's config and refuse to start on a mismatch, so a
        # partition-only (config-less, broker-default) create would stop them (review round 4).
        names = counts(env_var("OPTIONS_EDGE_PARTITION_ONLY_TOPICS"))
        for strict in ("options.es-cvd-spx-levels", "strike-intel.gamma-tilt.shadow"):
            self.assertNotIn(strict, names)

    def test_counts_the_audit_proved_load_bearing(self):
        po = counts(env_var("OPTIONS_EDGE_PARTITION_ONLY_TOPICS"))
        # Streams sources / co-partitioned inputs whose wrong count wedges a reader.
        for name, n in {"strike-intelligence-by-strike": 32, "option-price-behavior-by-strike": 32,
                        "options.databento.seller-activity": 32, "options.databento.spot-band-flow": 32,
                        "dealer-ledger-profile": 32, "dealer-ledger-signal-fired": 32,
                        "context-tape.direction.commissioning": 1, "context-tape.direction.checkpoint": 1,
                        "options.spx.strike-invasion.events": 1,
                        # dev readers that halt without them and have no creator on dev
                        "es.futures.cvd.levels": 1, "approach-monitor-input-repartition": 1,
                        "positions.approach.current": 1, "positions.approach.events": 1, "positions.approach.alerts": 1}.items():
            self.assertEqual(po.get(name), n, name)

    def test_declarations_raised_to_what_owners_create(self):
        declared = counts(env_var("OPTIONS_EDGE_TOPICS"))
        for name in ("options.es-strike-intel-spx-aligned", "options.spx.greek-move-auth.current",
                     "options.spx.greek-move-auth.events", "options.databento.maxpain",
                     "options.databento.oi.nowcast.by-strike", "options.spx.spread-skew.current",
                     "options.spx.spread-skew.events", "options.spx.strike-invasion.current"):
            self.assertEqual(declared[name], 32, name)

    def test_es_spx_align_sources_are_exact(self):
        exact = set(env_var("OPTIONS_EDGE_EXACT_PARTITION_TOPICS"))
        declared = counts(env_var("OPTIONS_EDGE_TOPICS"))
        for name, n in {"es.options.databento.gex.spxbridge": 4, "es.strike-intelligence-by-strike": 32,
                        "es.options.indicators.bars": 8}.items():
            self.assertIn(name, exact)
            self.assertEqual(declared[name], n)


class EnsurePartitionOnlyTopicsTest(unittest.TestCase):
    def run_fn(self, topics, entries):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "kt.py").write_text(FAKE_KT)
            (d / "state.json").write_text(json.dumps({"topics": topics, "calls": []}))
            text = DEV_CLEANUP.read_text()
            start = text.index("ensure_partition_only_topics() {")
            fn = text[start:text.index("\n}\n", start) + 3]
            script = (f'KT="python3 {d / "kt.py"} {d / "state.json"}"\nBS=localhost:19092\n'
                      f'OPTIONS_EDGE_PARTITION_ONLY_TOPICS="{" ".join(entries)}"\n{fn}\nensure_partition_only_topics\n')
            out = subprocess.run([BASH, "-c", script], capture_output=True, text=True, check=True).stdout
            return out, json.loads((d / "state.json").read_text())

    def test_creates_missing_grows_smaller_reports_larger_and_sets_no_config(self):
        out, st = self.run_fn({"grow.me": 1, "too.big": 32, "ok.one": 4},
                              ["new.one:32", "grow.me:32", "too.big:1", "ok.one:4"])
        self.assertEqual(st["topics"], {"new.one": 32, "grow.me": 32, "too.big": 32, "ok.one": 4})
        self.assertIn("created 1, grown 1", out)
        self.assertIn("too.big(32>1)", out)
        for call in st["calls"]:
            self.assertNotIn("--config", call)
        describes = [c for c in st["calls"] if "--describe" in c]
        self.assertEqual(len(describes), 1, "one bulk describe, not a JVM per topic")
        self.assertFalse([c for c in st["calls"] if "--alter" in c and "ok.one" in c])

    def test_describe_failure_creates_nothing_blind(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            text = DEV_CLEANUP.read_text()
            start = text.index("ensure_partition_only_topics() {")
            fn = text[start:text.index("\n}\n", start) + 3]
            calls = d / "calls"
            script = (f'KT="sh -c \'echo \\"$*\\" >> {calls}; exit 1\' kt"\nBS=x\n'
                      f'OPTIONS_EDGE_PARTITION_ONLY_TOPICS="a.b:4"\n{fn}\nensure_partition_only_topics\n')
            out = subprocess.run([BASH, "-c", script], capture_output=True, text=True).stdout
            self.assertIn("NOT ensured", out)
            self.assertNotIn("--create", calls.read_text() if calls.exists() else "")

    def test_alter_escapes_dots(self):
        out, st = self.run_fn({"a.b": 1, "a-b": 1}, ["a.b:4"])
        alters = [c for c in st["calls"] if "--alter" in c]
        self.assertEqual(alters[0][alters[0].index("--topic") + 1], "a\\.b")

    def test_ensure_topics_runs_it_before_the_reconcile(self):
        text = DEV_CLEANUP.read_text()
        start = text.index("ensure_topics() {")
        body = text[start:text.index("\n}\n", start)]
        self.assertIn("PARTITION_ONLY_TOPICS", body.split("\n")[3] + body)     # parsed from topics.env
        self.assertLess(body.index("ensure_partition_only_topics"), body.index("reconcile_declared_topics"))


if __name__ == "__main__":
    unittest.main()
