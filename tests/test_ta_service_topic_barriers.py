"""ensure-es-trade-tools-topics.sh and ensure-wave-cycle-topics.sh (the first-deploy barriers of the two new TA-books services) against the stateful
stand-in (tests/fake_kafka_cli.py): absent topics are created at the declared shape and verified, an existing topic is never altered, every difference the
SERVICE refuses (partitions, policy set, exact vs floor retention, message bound) fails closed and names the topic, and the shapes the barriers carry are
the ones scripts/kafka/topics.env declares."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_kafka_cli.py"
TT, WC = ROOT / "scripts/kafka/ensure-es-trade-tools-topics.sh", ROOT / "scripts/kafka/ensure-wave-cycle-topics.sh"
DAYS90, TWO_SESSIONS = "7776000000", "172800000"

# topic -> (policy, retention.ms, min max.message.bytes)
TT_TOPICS = {
    "futures.pf.columns": ("compact", "-1", 6144), "futures.pf.chart": ("compact", "-1", 20480),
    "futures.pf.events": ("compact,delete", DAYS90, 8192), "futures.volume-nodes": ("compact", "-1", 20480),
    "futures.flow.bars": ("compact,delete", DAYS90, 8192), "futures.flow.events": ("compact,delete", DAYS90, 6144),
    "futures.tradetools.state": ("compact", "-1", 528384),
}
WC_TOPICS = {"options.spx.wave-cycle.waves": ("delete", TWO_SESSIONS, 1048576), "options.spx.wave-cycle.cycles": ("delete", TWO_SESSIONS, 1048576),
             "options.spx.wave-cycle.recovery": ("compact", "-1", 1048576)}


def declarations(variables: list[str]) -> dict[str, list[str]]:
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy: str, retention: str, parts: int = 1, max_bytes: int = 1048576, isr: int = 1, end: int = 5) -> dict:
    return {"partitions": parts, "rf": 1, "end": end,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "max.message.bytes": str(max_bytes), "min.insync.replicas": str(isr)}}


def run(script: Path, existing: dict):
    tmp = Path(tempfile.mkdtemp())
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ["kafka-topics", "kafka-configs"]:
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    state, calls = tmp / "state.json", tmp / "calls"
    state.write_text(json.dumps({"topics": existing}))
    calls.touch()
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls),
               KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1")
    r = subprocess.run(["bash", str(script)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())["topics"]


def full(table: dict) -> dict:
    return {t: shaped(p, r, max_bytes=mb if mb > 1048576 else 1048576) for t, (p, r, mb) in table.items()}


class EsTradeToolsBarrier(unittest.TestCase):
    def test_absent_topics_are_created_at_the_services_shape(self):
        rc, out, _, topics = run(TT, {})
        self.assertEqual(rc, 0, out)
        for t, (policy, ret, _) in TT_TOPICS.items():
            self.assertEqual(topics[t]["partitions"], 1, t)
            self.assertEqual(topics[t]["configs"]["cleanup.policy"], policy, t)
            self.assertEqual(topics[t]["configs"]["retention.ms"], ret, t)
            self.assertEqual(topics[t]["configs"]["max.message.bytes"], "1048576", t)
        self.assertIn("=== ensure-es-trade-tools-topics OK ===", out)

    def test_existing_right_topics_are_left_alone_and_a_reordered_policy_set_passes(self):
        existing = full(TT_TOPICS)
        existing["futures.pf.events"]["configs"]["cleanup.policy"] = "delete,compact"
        rc, out, calls, _ = run(TT, existing)
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--create" in c or "--alter" in c or "--delete" in c for c in calls))

    def test_every_difference_the_service_refuses_fails_closed_and_names_the_topic(self):
        cases = {
            "partitions": ("futures.pf.chart", lambda d: d.update(partitions=32)),
            "compact topic with the 90 day retention": ("futures.pf.columns", lambda d: d["configs"].update({"retention.ms": DAYS90})),
            "90 day topic one ms short": ("futures.flow.bars", lambda d: d["configs"].update({"retention.ms": "7775999999"})),
            "90 day topic longer (the service compares for equality)": ("futures.pf.events", lambda d: d["configs"].update({"retention.ms": "7776000001"})),
            "compact topic that is compact,delete": ("futures.tradetools.state", lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "90 day topic that is plain delete": ("futures.flow.events", lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "state chunk bound one byte short": ("futures.tradetools.state", lambda d: d["configs"].update({"max.message.bytes": "528383"})),
            "min.insync.replicas below the minimum": ("futures.volume-nodes", lambda d: d["configs"].update({"min.insync.replicas": "0"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = full(TT_TOPICS)
            mutate(existing[topic])
            rc, out, calls, topics = run(TT, existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertNotIn(f"ok: '{topic}'", out, label)
            self.assertFalse(any("--delete" in c or "--alter" in c for c in calls), f"{label}: a barrier never alters or recreates")
            self.assertEqual(topics[topic]["partitions"], existing[topic]["partitions"])


class WaveCycleBarrier(unittest.TestCase):
    def test_absent_topics_are_created_and_the_recovery_log_gets_the_small_segment_cadence(self):
        rc, out, _, topics = run(WC, {})
        self.assertEqual(rc, 0, out)
        for t, (policy, ret, _) in WC_TOPICS.items():
            self.assertEqual(topics[t]["partitions"], 1, t)
            self.assertEqual(topics[t]["configs"]["cleanup.policy"], policy, t)
        self.assertEqual(topics["options.spx.wave-cycle.waves"]["configs"]["retention.ms"], TWO_SESSIONS)
        rec = topics["options.spx.wave-cycle.recovery"]["configs"]
        self.assertEqual((rec["retention.ms"], rec["segment.bytes"], rec["max.compaction.lag.ms"]), ("-1", "16777216", "300000"))

    def test_waves_and_cycles_retention_is_a_floor_and_the_recovery_retention_is_not_judged(self):
        existing = full(WC_TOPICS)
        existing["options.spx.wave-cycle.waves"]["configs"]["retention.ms"] = "999999999999"
        existing["options.spx.wave-cycle.cycles"]["configs"]["retention.ms"] = "-1"
        existing["options.spx.wave-cycle.recovery"]["configs"]["retention.ms"] = "604800000"   # what the service leaves it at: it sets none
        rc, out, _, _ = run(WC, existing)
        self.assertEqual(rc, 0, out)

    def test_every_difference_the_service_refuses_fails_closed(self):
        cases = {
            "waves one ms below the floor": ("options.spx.wave-cycle.waves", lambda d: d["configs"].update({"retention.ms": "172799999"})),
            "cycles compacted": ("options.spx.wave-cycle.cycles", lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "waves compact,delete": ("options.spx.wave-cycle.waves", lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "recovery delete": ("options.spx.wave-cycle.recovery", lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "recovery 32 partitions": ("options.spx.wave-cycle.recovery", lambda d: d.update(partitions=32)),
            "cycles message bound short": ("options.spx.wave-cycle.cycles", lambda d: d["configs"].update({"max.message.bytes": "1048575"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = full(WC_TOPICS)
            mutate(existing[topic])
            rc, out, calls, _ = run(WC, existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertFalse(any("--delete" in c for c in calls), label)


class BarriersAgreeWithTopicsEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES",
                              "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])

    def test_every_barrier_topic_is_declared_one_partition_exact_with_the_barriers_retention(self):
        retention = dict(x.split("=", 1) for x in self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        for table in (TT_TOPICS, WC_TOPICS):
            for t, (policy, ret, _) in table.items():
                self.assertIn(f"{t}:1", self.d["OPTIONS_EDGE_TOPICS"], t)
                self.assertIn(t, self.d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
                self.assertEqual(retention[t], ret, t)
                pure = policy == "compact"
                self.assertEqual(t in self.d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], pure, t)
                if "," in policy:
                    self.assertIn(t, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
                if ret == "-1":
                    self.assertIn(t, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)

    def test_the_pure_compact_trade_tools_topics_declare_their_byte_contract_and_no_topic_is_versioned(self):
        for t, (policy, ret, _) in TT_TOPICS.items():
            if policy == "compact":
                self.assertIn(f"{t}=-1", self.d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"], t)
        for t in list(TT_TOPICS) + list(WC_TOPICS):
            self.assertNotRegex(t, r"\.v\d+$")


if __name__ == "__main__":
    unittest.main()
