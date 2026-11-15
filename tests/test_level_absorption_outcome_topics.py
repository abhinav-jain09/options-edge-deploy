# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring of the in-service outcomes of level-absorption-service (liq-V step 1; options-edge-documents design/es-liquidity/liq-V-in-service-outcomes-design.md sections 6, 11, 12).

Two dev-only topics (futures.level-absorption.outcomes: delete, 30 days, 256 MiB; futures.level-absorption.scoreboard: pure compact, RESET-PRESERVED and DEV-CLEAN-PRESERVED), their schema
manifest entries (contracts 0.94.0, ids 2813-2814), the first-deploy barrier, the gateway reader env, the level-absorption env, and the REMOVAL of the scorecard ConfigMap volume and SCORECARD_DIR."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_kafka_cli.py"
BARRIER = ROOT / "scripts/kafka/ensure-level-absorption-outcome-topics.sh"
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
OUTCOMES = "futures.level-absorption.outcomes"
SCOREBOARD = "futures.level-absorption.scoreboard"
THIRTY_DAYS = "2592000000"
CAP = "268435456"
VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS",
        "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_ES4_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"]


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def declarations(variables, environment=None):
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy, retention, bytes_, parts=1, isr=1, maxbytes="1048576"):
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_, "max.message.bytes": maxbytes, "min.insync.replicas": str(isr)}}


def right_topics():
    return {OUTCOMES: shaped("delete", THIRTY_DAYS, CAP), SCOREBOARD: shaped("compact", "-1", "-1")}


def run_barrier(topics, environment="dev"):
    tmp = Path(tempfile.mkdtemp())
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ("kafka-topics", "kafka-configs"):
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    state, calls = tmp / "state.json", tmp / "calls"
    state.write_text(json.dumps({"topics": topics}))
    calls.touch()
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls), KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1")
    if environment is not None:
        env["ENVIRONMENT"] = environment
    r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())


class Topics(unittest.TestCase):
    def test_both_topics_are_declared_once_one_partition_exact_and_the_shapes_are_right(self):
        d = declarations(VARS)
        for t in (OUTCOMES, SCOREBOARD):
            self.assertEqual(d["OPTIONS_EDGE_TOPICS"].count(f"{t}:1"), 1, t)
            self.assertEqual(d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"].count(t), 1, t)
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        self.assertEqual((retention[OUTCOMES], bytes_[OUTCOMES]), (THIRTY_DAYS, CAP))
        self.assertEqual((retention[SCOREBOARD], bytes_[SCOREBOARD]), ("-1", "-1"))
        self.assertIn(SCOREBOARD, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(OUTCOMES, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(OUTCOMES, d["OPTIONS_EDGE_COMPACTED_TOPICS"])

    def test_the_scoreboard_is_preserved_by_every_reset_path_and_the_outcomes_stay_deletable(self):
        d = declarations(VARS)
        self.assertIn(SCOREBOARD, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(SCOREBOARD, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"])
        self.assertNotIn(SCOREBOARD, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        for t in (OUTCOMES,):
            self.assertNotIn(t, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
            self.assertNotIn(t, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
            self.assertNotIn(t, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"])

    def test_the_premarket_reset_spares_the_scoreboard_and_only_it(self):
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        self.assertIsNotNone(re.search(regex, SCOREBOARD))
        for t in (OUTCOMES, f"{SCOREBOARD}2", f"x{SCOREBOARD}", f"{SCOREBOARD}x", "futures.level-absorption.voice"):
            self.assertIsNone(re.search(regex, t), t)

    def test_dev_and_unset_declare_them_and_every_other_environment_declares_none(self):
        base = declarations(VARS)
        self.assertEqual(declarations(VARS, "dev"), base)
        for environment in ("production", "experiment", "es4", "staging"):
            other = declarations(VARS, environment)
            self.assertFalse([x for v in VARS for x in other[v] if OUTCOMES in x or SCOREBOARD in x], environment)

    def test_the_text_readers_see_the_indented_declaration(self):
        """The 2026-11-06 loss: readers anchored at column 0 missed an indented dev-only block. The shared helper (executed, not sourced) must list the scoreboard."""
        r = subprocess.run(["bash", str(ROOT / "scripts/kafka/reset-preserved-topics.sh")], capture_output=True, text=True, env={**os.environ, "ENVIRONMENT": "dev"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn(SCOREBOARD, r.stdout.split())
        self.assertNotIn(OUTCOMES, r.stdout.split())
        lines = [l for l in read("scripts/kafka/topics.env").splitlines() if re.match(r"^\s*OPTIONS_EDGE_(RESET|DEV_CLEAN)_PRESERVED_TOPICS=.*" + re.escape(SCOREBOARD), l) or re.match(r"^\s*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=.*" + re.escape(SCOREBOARD), l)]
        self.assertEqual(len(lines), 2)
        self.assertTrue(all(l.startswith("  ") for l in lines), "indented inside the dev-only if")

    def test_it_is_not_held_down_by_the_dev_cleanup_lists(self):
        cleanup = read("scripts/ops/dev-cleanup.sh")
        self.assertNotIn(SCOREBOARD, cleanup)
        self.assertNotIn(OUTCOMES, cleanup)


class Manifest(unittest.TestCase):
    def entries(self):
        return {e["id"]: e for e in json.loads(read("schema-manifest/manifest.json"))["entries"] if e.get("id") in (2813, 2814)}

    def test_the_two_entries_are_dev_only_futures_family_under_the_topic_subjects(self):
        e = self.entries()
        self.assertEqual(set(e), {2813, 2814})
        want = {2813: ("futures.level-absorption.outcomes-value", "LevelAbsorptionOutcome", OUTCOMES), 2814: ("futures.level-absorption.scoreboard-value", "LevelAbsorptionScoreboard", SCOREBOARD)}
        for i, (subject, rec, topic) in want.items():
            x = e[i]
            self.assertEqual((x["kind"], x["subject"], x["version"], x["family"], x["environments"]), ("MIRRORED", subject, 1, "futures", ["dev"]))
            self.assertEqual(x["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/levelabsorption/{rec}.avsc"})
            self.assertEqual(x["topics"], [{"name": topic, "kind": "data"}])
            self.assertEqual(x["producers"], [{"service": "level-absorption", "environments": ["dev"]}])
            snap = ROOT / "schema-manifest/schemas" / x["schemaFile"]
            h = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True).stdout.strip().splitlines()[-1]
            self.assertEqual(h, x["schemaHash"])
        self.assertEqual(e[2813]["consumers"], [])
        self.assertEqual(e[2814]["consumers"], [{"service": "feed-gateway", "accepts": [2814], "environments": ["dev"]}])

    def test_the_family_block_is_extended_and_the_manifest_validates(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        self.assertIn([2813, 2813], m["familyBlocks"]["futures"])
        self.assertIn([2814, 2814], m["familyBlocks"]["futures"])
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_both_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        o, s = state["topics"][OUTCOMES], state["topics"][SCOREBOARD]
        self.assertEqual((o["partitions"], o["configs"]["cleanup.policy"], o["configs"]["retention.ms"], o["configs"]["retention.bytes"]), (1, "delete", THIRTY_DAYS, CAP))
        self.assertEqual((s["partitions"], s["configs"]["cleanup.policy"], s["configs"]["retention.ms"], s["configs"]["retention.bytes"]), (1, "compact", "-1", "-1"))
        self.assertIn("=== ensure-level-absorption-outcome-topics OK ===", out)

    def test_it_is_dev_only_and_fails_closed_before_touching_anything(self):
        for environment in ("production", "es4", "", None):
            rc, out, calls, state = run_barrier({}, environment=environment)
            self.assertEqual(rc, 1, f"{environment!r}: {out}")
            self.assertIn("DEV ONLY", out)
            self.assertEqual(calls, [])
            self.assertEqual(state["topics"], {})

    def test_a_second_run_changes_nothing(self):
        _, _, _, state = run_barrier({})
        rc, out, calls, _ = run_barrier(state["topics"])
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--create" in c or "--delete" in c or "--alter" in c for c in calls))

    def test_every_difference_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {
            "scoreboard delete policy": (SCOREBOARD, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "scoreboard finite retention": (SCOREBOARD, lambda d: d["configs"].update({"retention.ms": "86400000"})),
            "scoreboard byte-capped": (SCOREBOARD, lambda d: d["configs"].update({"retention.bytes": CAP})),
            "scoreboard 4 partitions": (SCOREBOARD, lambda d: d.update(partitions=4)),
            "scoreboard compact,delete": (SCOREBOARD, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "outcomes compacted": (OUTCOMES, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "outcomes one ms short": (OUTCOMES, lambda d: d["configs"].update({"retention.ms": "2591999999"})),
            "outcomes never ages out": (OUTCOMES, lambda d: d["configs"].update({"retention.ms": "-1"})),
            "outcomes another byte cap": (OUTCOMES, lambda d: d["configs"].update({"retention.bytes": "-1"})),
            "outcomes 3 partitions": (OUTCOMES, lambda d: d.update(partitions=3)),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(topic, out, label)
            self.assertFalse(any("--alter" in c or "--delete" in c for c in calls), label)
            self.assertFalse(any("--create" in c for c in calls), label)

    def test_the_level_absorption_barrier_calls_it_on_dev_only(self):
        sh = read("scripts/kafka/ensure-level-absorption-topics.sh")
        m = re.search(r'if \[ "\$\{ENVIRONMENT:-\}" = "dev" \]; then\n  if ! bash "\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)/ensure-level-absorption-outcome-topics\.sh"', sh)
        self.assertIsNotNone(m, "the outcome barrier is called inside an exact-dev guard")
        self.assertIn("outcome topics: NOT ensured", sh)


class Wiring(unittest.TestCase):
    def test_the_gateway_dev_patch_and_slice_carry_the_scoreboard_reader_env(self):
        for rel in ("k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/services/feed-gateway/overlays/dev/manifest.yaml"):
            t = read(rel)
            self.assertRegex(t, r"- name: KAFKA_SCORECARD_SCOREBOARD_TOPICS\n\s+value: \"?futures\.level-absorption\.scoreboard\"?", rel)

    def test_the_level_absorption_dev_patch_and_slice_carry_the_two_topic_env_and_production_none(self):
        for rel in ("k8s/overlays/dev/level-absorption-dev-enable-patch.yaml", "k8s/services/level-absorption/overlays/dev/manifest.yaml"):
            t = read(rel)
            self.assertRegex(t, rf"- name: KAFKA_LEVEL_ABSORPTION_OUTCOMES_TOPIC\n\s+value: {re.escape(OUTCOMES)}", rel)
            self.assertRegex(t, rf"- name: KAFKA_LEVEL_ABSORPTION_SCOREBOARD_TOPIC\n\s+value: {re.escape(SCOREBOARD)}", rel)
        for rel in ("k8s/services/level-absorption/overlays/production/manifest.yaml", "k8s/services/level-absorption/base/level-absorption-deployment.yaml"):
            self.assertNotIn("OUTCOMES_TOPIC", read(rel), rel)
            self.assertNotIn("SCOREBOARD", read(rel), rel)

    def test_the_web_dev_env_enables_the_scorecard_page_and_names_the_gateway_the_proxy_needs(self):
        for rel in ("k8s/overlays/dev/web-dev-env-patch.yaml", "k8s/services/web/overlays/dev/web-dev-env-patch.yaml"):
            t = read(rel)
            self.assertRegex(t, r'- name: SCORECARD_PAGE_ENABLED\n\s+value: "true"', rel)
            self.assertRegex(t, r'- name: FEED_GATEWAY_REST_URL\n\s+value: "http://feed-gateway-service:8091"', rel)

    def test_the_scorecard_configmap_volume_and_scorecard_dir_are_gone_from_every_web_render(self):
        for rel in ("k8s/overlays/dev/web-scorecard-patch.yaml", "k8s/services/web/overlays/dev/web-scorecard-patch.yaml"):
            self.assertFalse((ROOT / rel).exists(), rel)
        for rel in ("k8s/overlays/dev/kustomization.yaml", "k8s/services/web/overlays/dev/kustomization.yaml"):
            self.assertNotIn("- path: web-scorecard-patch.yaml", read(rel), rel)
        for base in ("k8s/overlays", "k8s/services/web"):
            for p in (ROOT / base).rglob("*.yaml"):
                t = "\n".join(l for l in p.read_text().splitlines() if not l.lstrip().startswith("#"))   # a comment may name what was removed
                self.assertNotIn("SCORECARD_DIR", t, str(p))
                self.assertNotIn("name: scorecard-latest", t, str(p))


if __name__ == "__main__":
    unittest.main()
