# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring of the Line health feature, milestone M0 (requirement rev 25 LH-O-1, LH-O-2, LH-L-*; design 3.3): DEV ONLY.

Covers: the seven futures.line-health.* topics' declared shapes in topics.env (one partition, policy, retention.ms, retention.bytes -1, the 9 MiB snapshot bound, the reset classes), their
absence from a production declaration (the explicit-production guard) and presence on dev and with ENVIRONMENT unset, the topic barrier (scripts/kafka/ensure-line-health-topics.sh) against the
stateful stand-in tests/fake_kafka_cli.py, the fixed-id schema map (3701-3707, shared with no other map), the schema-manifest entries (family block 3701-3710 without overlap, hashes equal to the
snapshots, no producer or consumer binding at M0 so no service becomes schema-gated), the registration wiring of the dev registry import, and the absence of any production wiring."""
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
BARRIER = ROOT / "scripts/kafka/ensure-line-health-topics.sh"
P = "futures.line-health"
STATE, EVENTS, VOICE, USER_LINES, STATUS, CLOSURES, SNAPSHOT = (f"{P}.{s}" for s in ("state", "events", "voice", "user-lines", "status", "closures", "snapshot"))
TOPICS = (STATE, EVENTS, VOICE, USER_LINES, STATUS, CLOSURES, SNAPSHOT)
SEVEN_DAYS = "604800000"
# topic -> (cleanup.policy, retention.ms, retention.bytes, max.message.bytes)
SHAPES = {STATE: ("compact,delete", SEVEN_DAYS, "-1", 1048576), EVENTS: ("delete", SEVEN_DAYS, "-1", 1048576), VOICE: ("delete", SEVEN_DAYS, "-1", 1048576),
          USER_LINES: ("compact,delete", SEVEN_DAYS, "-1", 1048576), STATUS: ("compact", "-1", "-1", 1048576), CLOSURES: ("delete", SEVEN_DAYS, "-1", 1048576),
          SNAPSHOT: ("compact", "-1", "-1", 9437184)}
NS = "com.optionsedge.contracts.linehealth"
# the CURRENT ids (what the loop writes). The state, event and status subjects are VERSION 2 since contracts 0.82.0 (a registered id never changes content): their version-1 ids stay as archival.
IDS = {"LineHealthState": 3708, "LineHealthEvent": 3709, "LineHealthVoice": 3703, "LineHealthUserLine": 3704, "LineHealthStatus": 3710, "LineHealthClosure": 3706,
       "LineHealthSnapshot": 3707}
ARCHIVAL_V1 = {"LineHealthState": 3701, "LineHealthEvent": 3702, "LineHealthStatus": 3705}
TOPIC_OF = dict(zip(IDS, TOPICS))
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES",
        "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS",
        "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES", "OPTIONS_EDGE_ES4_TOPICS"]


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def declarations(environment: str | None = None) -> dict[str, list[str]]:
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in VARS)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy: str, retention: str, bytes_: str, max_bytes: int, parts: int = 1, isr: int = 1) -> dict:
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_, "max.message.bytes": str(max_bytes), "min.insync.replicas": str(isr)}}


def run_barrier(existing: dict, environment: str = "dev") -> tuple[int, str, list[str], dict]:
    with tempfile.TemporaryDirectory() as d:
        t = Path(d)
        (t / "bin").mkdir()
        for tool in ("kafka-topics", "kafka-configs"):
            shutil.copy(FAKE, t / "bin" / tool)
            (t / "bin" / tool).chmod(0o755)
        (t / "state.json").write_text(json.dumps({"topics": existing}))
        (t / "calls").write_text("")
        env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
        env.update(PATH=f"{t / 'bin'}:{env['PATH']}", FAKE_KAFKA_STATE=str(t / "state.json"), FAKE_KAFKA_CALLS=str(t / "calls"), KAFKA_BOOTSTRAP_SERVERS="mock:9092",
                   KAFKA_TOPIC_REPLICATION_FACTOR="1")
        if environment:
            env["ENVIRONMENT"] = environment
        r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
        return r.returncode, r.stdout + r.stderr, (t / "calls").read_text().splitlines(), json.loads((t / "state.json").read_text())["topics"]


def right_topics() -> dict:
    return {t: shaped(*s) for t, s in SHAPES.items()}


class Barrier(unittest.TestCase):
    def test_an_empty_cluster_gets_exactly_seven_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        found = {t for t in state if t.startswith(P)}
        self.assertEqual(found, set(TOPICS), "the read-back counts exactly the seven")
        for t, (policy, ms, by, mb) in SHAPES.items():
            c = state[t]
            self.assertEqual((c["partitions"], c["configs"]["cleanup.policy"], c["configs"]["retention.ms"], c["configs"]["retention.bytes"], c["configs"]["max.message.bytes"]),
                             (1, policy, ms, by, str(mb)), t)
        self.assertEqual(sum("--create" in c for c in calls), 7)
        self.assertFalse(any(x in c for c in calls for x in ("--delete", "--alter", "--entity-type brokers", "--entity-default")))
        rc2, out2, calls2, _ = run_barrier(state)
        self.assertEqual(rc2, 0, out2)
        self.assertFalse(any("--create" in c or "--alter" in c or "--delete" in c for c in calls2), "a second run changes nothing")

    def test_it_is_dev_only_and_fails_before_any_cli_call(self):
        for env in ("production", "es4", ""):
            rc, out, calls, _ = run_barrier({}, env)
            self.assertEqual(rc, 1, env)
            self.assertIn("DEV ONLY", out)
            self.assertEqual(calls, [], f"ENVIRONMENT='{env}' must not reach a CLI")

    def test_every_difference_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {
            "state plain delete": (STATE, lambda c: c["configs"].update({"cleanup.policy": "delete"})),
            "state pure compact": (STATE, lambda c: c["configs"].update({"cleanup.policy": "compact"})),
            "state retention short": (STATE, lambda c: c["configs"].update({"retention.ms": "604799999"})),
            "state 32 partitions": (STATE, lambda c: c.update({"partitions": 32})),
            "events compacted": (EVENTS, lambda c: c["configs"].update({"cleanup.policy": "compact,delete"})),
            "events with a cap nobody asked for": (EVENTS, lambda c: c["configs"].update({"retention.bytes": "268435456"})),
            "voice kept forever": (VOICE, lambda c: c["configs"].update({"retention.ms": "-1"})),
            "user-lines plain delete": (USER_LINES, lambda c: c["configs"].update({"cleanup.policy": "delete"})),
            "status compact,delete": (STATUS, lambda c: c["configs"].update({"cleanup.policy": "compact,delete"})),
            "status finite bytes": (STATUS, lambda c: c["configs"].update({"retention.bytes": "1048576"})),
            "closures compacted": (CLOSURES, lambda c: c["configs"].update({"cleanup.policy": "compact"})),
            "snapshot delete": (SNAPSHOT, lambda c: c["configs"].update({"cleanup.policy": "delete"})),
            "snapshot message bound below 9 MiB": (SNAPSHOT, lambda c: c["configs"].update({"max.message.bytes": "1048576"})),
            "closures min.insync.replicas below the minimum": (CLOSURES, lambda c: c["configs"].update({"min.insync.replicas": "0"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or "--alter" in c for c in calls), label)
            self.assertEqual(state[topic]["partitions"], existing[topic]["partitions"], label)

    def test_the_barrier_is_executable_shares_the_routine_is_chained_and_is_in_the_ci_loop(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())
        la = read("scripts/kafka/ensure-level-absorption-topics.sh")
        self.assertIn('[ "${ENVIRONMENT:-}" = "dev" ]', la)
        self.assertIn("ensure-line-health-topics.sh", la)
        test = ROOT / "scripts/kafka/ensure-line-health-topics-test.sh"
        self.assertTrue(os.access(test, os.X_OK))
        self.assertIn("scripts/kafka/ensure-line-health-topics-test.sh", read("scripts/ci/validate-services.sh"))


class TopicsEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dev = declarations("dev")
        cls.unset = declarations(None)
        cls.prod = declarations("production")

    def test_the_seven_topics_are_declared_one_partition_exact_with_the_declared_policy_retention_and_bytes(self):
        for d in (self.dev, self.unset):
            retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
            bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
            maxb = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"] if "=" in x)
            for t, (policy, ms, by, mb) in SHAPES.items():
                self.assertIn(f"{t}:1", d["OPTIONS_EDGE_TOPICS"], t)
                self.assertIn(t, d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
                self.assertEqual(retention[t], ms, t)
                self.assertEqual(t in d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], policy == "compact", t)
                self.assertEqual(t in d["OPTIONS_EDGE_COMPACTED_TOPICS"], policy == "compact,delete", t)
                if policy == "compact":
                    self.assertEqual(bytes_.get(t), "-1", t)
                else:
                    self.assertNotIn(t, bytes_, f"{t}: no byte cap was asked for")
                self.assertEqual(maxb.get(t), "9437184" if t == SNAPSHOT else None, t)
                self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"], t)
                self.assertNotRegex(t, r"\.v\d+$")
            self.assertEqual(len([x for x in d["OPTIONS_EDGE_TOPICS"] if x.startswith(P)]), 7)

    def test_every_retention_minus_one_topic_has_exactly_one_reset_class(self):
        for t in (STATUS, SNAPSHOT):
            self.assertIn(t, self.dev["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
            self.assertNotIn(t, self.dev["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
        for t in (STATE, EVENTS, VOICE, USER_LINES, CLOSURES):
            self.assertNotIn(t, self.dev["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"] + self.dev["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)

    def test_production_declares_no_line_health_token_anywhere_and_keeps_every_other_declaration(self):
        for k, v in self.prod.items():
            self.assertFalse([x for x in v if "line-health" in x], f"{k} still names a line-health topic in a production declaration")
        self.assertTrue(any("line-health" in x for x in self.dev["OPTIONS_EDGE_TOPICS"]), "the guard must have something to strip on dev (sentinel)")
        for k in VARS:
            self.assertTrue(set(self.prod[k]) <= set(self.dev[k]), f"{k}: production declares nothing dev does not")

    def test_the_production_guard_fails_closed_when_it_matches_nothing(self):
        text = read("scripts/kafka/topics.env")
        a = text.index('if [ "${ENVIRONMENT:-}" = "production" ]; then\n  _lh_n=0')
        block = text[a:text.index("# Rithmic Test ES depth: identity mirrored")]
        base = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
        empty = subprocess.run(["bash", "-c", f'ENVIRONMENT=production; OPTIONS_EDGE_TOPICS="a:1 b:1"; {block}'], capture_output=True, text=True, env=base)
        self.assertNotEqual(empty.returncode, 0, "a guard that removes nothing must fail")
        self.assertIn("removed no futures.line-health token", empty.stderr)
        # a sourced file: the failure is the source's non-zero status
        full = subprocess.run(["bash", "-c", f'ENVIRONMENT=production; OPTIONS_EDGE_TOPICS="a:1 futures.line-health.state:1 b:1"; {block} echo "$OPTIONS_EDGE_TOPICS"'],
                              capture_output=True, text=True, env=base)
        self.assertEqual((full.returncode, full.stdout.strip()), (0, "a:1 b:1"))
        sourced = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"'], capture_output=True, text=True, env={**base, "ENVIRONMENT": "production"})
        self.assertEqual(sourced.returncode, 0, sourced.stderr)


class SchemaMapAndManifest(unittest.TestCase):
    MAP = ROOT / "scripts/kafka/schema-id-maps/line-health.map"

    def rows(self):
        return [tuple(l.split("|")) for l in self.MAP.read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")]

    def test_the_map_is_exactly_the_seven_records_and_the_three_version_two_rows(self):
        rows = self.rows()
        # version 1 (ids 3701-3707, the state, event and status files under v1/) first, then the three version-2 rows, each with the registry version in the fourth column
        expected = []
        for n, i in ARCHIVAL_V1.items():
            expected.append((f"{NS}.{n}", i, f"v1/{n}.avsc", "1"))
        for n in ("LineHealthVoice", "LineHealthUserLine", "LineHealthClosure", "LineHealthSnapshot"):
            expected.append((f"{NS}.{n}", IDS[n], f"{n}.avsc"))
        for n in ARCHIVAL_V1:
            expected.append((f"{NS}.{n}", IDS[n], f"{n}.avsc", "2"))
        key = lambda r: (r[1] if len(r) > 1 else "")
        self.assertEqual(sorted(((r[0], int(r[1])) + tuple(r[2:]) for r in rows), key=lambda r: r[1]), sorted(((e[0], e[1]) + tuple(e[2:]) for e in expected), key=lambda r: r[1]))
        versions = [r for r in rows if len(r) == 4]
        v1_positions = [i for i, r in enumerate(rows) if len(r) == 4 and r[3] == "1"]
        v2_positions = [i for i, r in enumerate(rows) if len(r) == 4 and r[3] == "2"]
        self.assertTrue(max(v1_positions) < min(v2_positions), "version-2 rows after all version-1 rows, so the preflight pass can see the predecessor of every row")
        self.assertEqual(len(versions), 6)
        self.assertEqual(sorted(int(r[1]) for r in rows), list(range(3701, 3711)), "the family block 3701-3710 is full and each id appears once")

    def test_no_id_is_shared_with_any_other_map(self):
        ours = {int(r[1]) for r in self.rows()}
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other == self.MAP:
                continue
            for line in other.read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    self.assertNotIn(int(line.split("|")[1]), ours, f"{other.name} reuses a line-health id")

    def test_the_manifest_carries_the_family_block_and_the_seven_dev_entries_bound_to_level_absorption_on_dev(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        self.assertEqual(m["familyBlocks"]["linehealth"], [[3701, 3710]])
        for fam, ranges in m["familyBlocks"].items():
            if fam != "linehealth":
                for lo, hi in ranges:
                    self.assertTrue(hi < 3701 or lo > 3710, f"{fam} overlaps the linehealth block")
        used = {e["id"] for e in m["entries"] if e.get("family") != "linehealth" and "id" in e}
        self.assertFalse(used & set(range(3701, 3711)), "no other manifest entry sits in the block")
        mine = [e for e in m["entries"] if e.get("family") == "linehealth"]
        self.assertEqual(sorted(e["id"] for e in mine), sorted(list(IDS.values()) + list(ARCHIVAL_V1.values())))
        for name, old in ARCHIVAL_V1.items():
            a = next(x for x in mine if x["id"] == old)
            arch_consumers = [{"service": "break-line", "accepts": [3701, 3708], "environments": ["dev"]}] if name == "LineHealthState" else []   # break-line-service reads both versions of the state (rev 14, BS13-SL-006)
            self.assertEqual((a["kind"], a["version"], a["subject"], a["environments"], a["producers"], a["consumers"]), ("MIRRORED", 1, f"{NS}.{name}", ["dev"], [], arch_consumers), f"{name}: archival version 1, nobody writes it")
            self.assertEqual(a["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/linehealth/v1/{name}.avsc"}, name)
            self.assertEqual(a["schemaFile"], f"{NS}.{name}/v1.avsc")
        for name, sid in IDS.items():
            e = next(x for x in mine if x["subject"] == f"{NS}.{name}" and x["id"] == sid)
            two = name in ARCHIVAL_V1
            self.assertEqual((e["kind"], e["version"], e["id"], e["environments"]), ("MIRRORED", 2 if two else 1, sid, ["dev"]), name)
            self.assertEqual(e["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/linehealth/{name}.avsc"}, name)
            self.assertEqual(e["topics"], [{"name": TOPIC_OF[name], "kind": "data"}], name)
            producers = [] if name == "LineHealthUserLine" else [{"service": "level-absorption", "environments": ["dev"]}]
            consumers = [{"service": "break-line", "accepts": [3701, 3708], "environments": ["dev"]}] if name == "LineHealthState" else []   # break-line-service reads the line health state (rev 14 system lines, BS13-SL-006)
            self.assertEqual((e["producers"], e["consumers"]), (producers, consumers), f"{name}: level-absorption is the only producer binding on dev (user lines are written by the web tier at M5); the feed gateway is not schema-gated for ANY relay (no schema-requirements.json, no consumer binding for the liquidity-wall or validator relays either), so the line-health relay is rolled in the same change by code (assertFramedAsAnyVersion), not by a manifest binding")
            snapshot = ROOT / "schema-manifest/schemas" / e["schemaFile"]
            text = snapshot.read_text()
            self.assertEqual(text, json.dumps(json.loads(text), separators=(",", ":"), ensure_ascii=False), f"{name}: one-line compact JSON, no trailing newline")
            self.assertEqual(json.loads(text)["name"], name)
            self.assertEqual(json.loads(text)["namespace"], NS)

    @unittest.skipUnless(shutil.which("java") and JAR.is_file(), "needs java and the pinned tool jar")
    def test_every_snapshot_hashes_to_its_entry_and_the_tool_accepts_the_manifest(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        count = 0
        for e in m["entries"]:
            if e.get("family") == "linehealth":
                have = subprocess.run(["java", "-jar", str(JAR), "hash", str(ROOT / "schema-manifest/schemas" / e["schemaFile"])], capture_output=True, text=True, check=True).stdout.strip()
                self.assertEqual(have, e["schemaHash"], e["subject"])
                count += 1
        self.assertEqual(count, 10, "seven current entries and the three archival version-1 entries")
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


class RegistrationWiring(unittest.TestCase):
    def test_the_jenkinsfile_is_not_grown_for_line_health(self):
        """Jenkinsfile.service-deploy sits at the 64 KB method limit of the pipeline compiler: the M0 edit to the geometry stage made build 3107 fail with 'Method too large'
        and was reverted. The line-health import must not touch this file."""
        text = read("Jenkinsfile.service-deploy")
        self.assertNotIn("line-health", text)
        self.assertNotIn("linehealth", text)
        self.assertIn("params.REGISTER_GEOMETRY_SCHEMAS && params.SERVICE == 'amt-order-bridge'", text)

    def test_the_topic_stage_runs_the_level_absorption_barrier_on_dev_only(self):
        text = read("Jenkinsfile.service-deploy")
        a = text.index("stage('Verify level-absorption output topics')")
        stage = text[a:text.index("stage('Apply level-absorption configuration')")]
        self.assertIn("params.SERVICE == 'level-absorption' && params.ENVIRONMENT == 'dev'", stage)
        self.assertIn("scripts/kafka/ensure-level-absorption-topics.sh", stage)
        self.assertLess(a, text.index("stage('Deploy (service-scoped)')"), "the barrier precedes the rollout")


class NoProductionWiring(unittest.TestCase):
    def test_no_production_file_names_a_line_health_topic_or_schema(self):
        for rel in ("k8s/overlays/production", "k8s/overlays/experiment", "k8s/es4", "image-tags/production.yaml", "scripts/es4"):
            path = ROOT / rel
            if not path.exists():
                continue
            files = [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]
            for f in files:
                text = f.read_text(errors="ignore")
                for n in ("line-health", "LineHealth"):
                    self.assertNotIn(n, text, f"{f.relative_to(ROOT)} must not mention {n}")


if __name__ == "__main__":
    unittest.main()
