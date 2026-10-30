"""Deploy wiring of break-classifier-service (Phase F), live on dev AND production through per-environment enable patches.

Covers: the topic barrier (no broker-wide change) (scripts/kafka/ensure-break-classification-topics.sh) against the stateful stand-in (tests/fake_kafka_cli.py), the topics.env
declarations, the per-topic max.message.bytes mechanism (topic-shape.lib.sh, dev-cleanup.sh), the registration of the service everywhere a service must be, the
flag/replica contract (base 0 / "false", literal "true" only in the two enable patches and their generated slices), the lifecycle lists and the fixed-id schema map."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_kafka_cli.py"
BARRIER = ROOT / "scripts/kafka/ensure-break-classification-topics.sh"
SVC, DEP, IMG, VAR = "break-classifier", "break-classifier-service", "options-edge-break-classifier", "BREAK_CLASSIFIER_IMAGE"
OUT, CP = "options.spx.break-classification", "options.spx.break-classification.checkpoint"
X1, X2 = "options.spx.market-structure.break-events", "options.spx.market-structure.swing-pivots"
E = "options.spx.market-structure.equal-levels"   # T-5: EqualLevelRecord, written only while BREAK_EQUAL_LEVELS_PUBLISH is true
TOPICS = (OUT, CP, X1, X2, E)
DAYS90, TWO_SESSIONS, EIGHT_MIB, NINE_MIB, ONE_MIB, STOCK_BROKER = "7776000000", "172800000", 8388608, 9437184, 1048576, 1048588
# topic -> (policy, retention.ms)
SHAPES = {OUT: ("delete", DAYS90), CP: ("compact", "-1"), X1: ("delete", TWO_SESSIONS), X2: ("delete", TWO_SESSIONS), E: ("delete", TWO_SESSIONS)}


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def env_values(path: Path, name: str) -> list[str]:
    return [v.strip("\"'") for v in re.findall(rf"name: {name}\n\s+value: (\S+)", path.read_text())]


def declarations(variables: list[str]) -> dict[str, list[str]]:
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy: str, retention: str, parts: int = 1, max_bytes: int = ONE_MIB, isr: int = 1) -> dict:
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "max.message.bytes": str(max_bytes), "min.insync.replicas": str(isr)}}


def right_topics() -> dict:
    return {t: shaped(p, r, max_bytes=NINE_MIB if t == CP else ONE_MIB) for t, (p, r) in SHAPES.items()}


def cli_dir(tmp: Path) -> Path:
    """kafka-topics and kafka-configs only: the barrier must not need (or call) kafka-broker-api-versions any more."""
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ["kafka-topics", "kafka-configs"]:
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    return bin_dir


def run_barrier(topics: dict, topic_alter_fails: bool = False):
    tmp = Path(tempfile.mkdtemp())
    state, calls = tmp / "state.json", tmp / "calls"
    state.write_text(json.dumps({"topics": topics}))
    calls.touch()
    env = dict(os.environ, PATH=f"{cli_dir(tmp)}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls),
               KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1")
    if topic_alter_fails:
        env["FAKE_KAFKA_TOPIC_MAXBYTES_ALTER_FAILS"] = "1"
    r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
    final = json.loads(state.read_text())
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), final


def broker_touches(calls: list[str]) -> list[str]:
    """Any call that reads or writes a BROKER config or lists brokers. The barrier makes none."""
    return [c for c in calls if "--entity-type brokers" in c or "kafka-broker-api-versions" in c or "--entity-default" in c]


def checkpoint_pins(calls: list[str]) -> list[str]:
    return [c for c in calls if "--entity-type topics" in c and "--alter" in c and "max.message.bytes" in c]


class Barrier(unittest.TestCase):
    def test_a_fresh_cluster_gets_the_five_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual(broker_touches(calls), [], "the barrier makes no broker-wide change and does not even read a broker config")
        self.assertNotIn("message.max.bytes", " ".join(calls), "the cluster-wide setting is never named in a command")
        for t, (policy, ret) in SHAPES.items():
            cfg = state["topics"][t]["configs"]
            self.assertEqual((state["topics"][t]["partitions"], cfg["cleanup.policy"], cfg["retention.ms"]), (1, policy, ret), t)
        self.assertEqual(state["topics"][CP]["configs"]["max.message.bytes"], str(NINE_MIB), "the TOPIC-level limit on the checkpoint")
        for t in (OUT, X1, X2):
            self.assertEqual(state["topics"][t]["configs"]["max.message.bytes"], str(ONE_MIB), t)
        cp = state["topics"][CP]["configs"]
        self.assertEqual((cp["segment.bytes"], cp["max.compaction.lag.ms"]), ("16777216", "300000"))
        self.assertEqual(checkpoint_pins(calls), [], "a created checkpoint is born at 9437184: no separate pin")
        self.assertIn("=== ensure-break-classification-topics OK ===", out)

    def test_a_second_run_changes_nothing_about_the_limit(self):
        rc, out, _, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        rc2, out2, calls2, state2 = run_barrier(state["topics"])
        self.assertEqual(rc2, 0, out2)
        self.assertEqual(checkpoint_pins(calls2), [])
        self.assertEqual(broker_touches(calls2), [])
        self.assertFalse(any("--create" in c or "--delete" in c for c in calls2))
        self.assertEqual(state2["topics"][CP]["configs"]["max.message.bytes"], str(NINE_MIB))

    def test_an_existing_checkpoint_without_its_own_limit_is_pinned_on_that_topic_only(self):
        """The dev state that caused the outage: the checkpoint inherited the (raised) broker default and had no topic-level value."""
        for label, mutate in (("no topic-level value", lambda c: c.pop("max.message.bytes")), ("topic-level at the 1 MiB default", lambda c: c.update({"max.message.bytes": str(STOCK_BROKER)})),
                              ("one byte under 8 MiB", lambda c: c.update({"max.message.bytes": str(EIGHT_MIB - 1)}))):
            existing = right_topics()
            mutate(existing[CP]["configs"])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 0, f"{label}: {out}")
            self.assertEqual(len(checkpoint_pins(calls)), 1, label)
            self.assertIn(f"--entity-name {CP} --alter --add-config max.message.bytes={NINE_MIB}", checkpoint_pins(calls)[0], label)
            self.assertEqual(state["topics"][CP]["configs"]["max.message.bytes"], str(NINE_MIB), label)
            self.assertEqual(broker_touches(calls), [], label)
            for t in (OUT, X1, X2, E):
                self.assertFalse(any(f"--entity-name {t} " in c and "--alter" in c for c in calls), f"{label}: {t} is never altered")

    def test_a_checkpoint_limit_at_or_above_8_mib_is_left_alone_and_never_lowered(self):
        for value in (EIGHT_MIB, NINE_MIB, 64 * ONE_MIB):
            existing = right_topics()
            existing[CP]["configs"]["max.message.bytes"] = str(value)
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 0, out)
            self.assertEqual(checkpoint_pins(calls), [], str(value))
            self.assertEqual(state["topics"][CP]["configs"]["max.message.bytes"], str(value))

    def test_a_failed_checkpoint_pin_fails_closed(self):
        existing = right_topics()
        existing[CP]["configs"].pop("max.message.bytes")
        rc, out, _, state = run_barrier(existing, topic_alter_fails=True)
        self.assertEqual(rc, 1, out)
        self.assertIn("could not set max.message.bytes", out)
        self.assertIn("FAILED", out)
        self.assertNotIn("OK ===", out)
        self.assertNotIn("max.message.bytes", state["topics"][CP]["configs"])

    def test_every_difference_the_service_refuses_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {
            "output retention one over (the service compares for equality)": (OUT, lambda d: d["configs"].update({"retention.ms": "7776000001"})),
            "output retention one short": (OUT, lambda d: d["configs"].update({"retention.ms": "7775999999"})),
            "output compacted": (OUT, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "output 32 partitions": (OUT, lambda d: d.update(partitions=32)),
            "checkpoint delete": (CP, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "checkpoint 32 partitions": (CP, lambda d: d.update(partitions=32)),
            "parents below the floor": (X1, lambda d: d["configs"].update({"retention.ms": "172799999"})),
            "parents compacted": (X1, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "pivots 32 partitions": (X2, lambda d: d.update(partitions=32)),
            "min.insync.replicas below the minimum": (X2, lambda d: d["configs"].update({"min.insync.replicas": "0"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertNotIn(f"ok: '{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or ("--alter" in c and "--entity-type topics" in c) for c in calls), f"{label}: a barrier never alters or recreates a topic")
            self.assertEqual(state["topics"][topic]["partitions"], existing[topic]["partitions"], label)

    def test_a_reordered_policy_set_and_a_longer_parent_retention_pass(self):
        existing = right_topics()
        existing[X1]["configs"]["retention.ms"] = "999999999999"
        rc, out, _, _ = run_barrier(existing)
        self.assertEqual(rc, 0, out)

    def test_the_checkpoints_own_retention_is_not_judged(self):
        existing = right_topics()
        existing[CP]["configs"]["retention.ms"] = "604800000"
        rc, out, _, _ = run_barrier(existing)
        self.assertEqual(rc, 0, out)

    def test_the_shared_create_routine_keeps_the_1_mib_default_for_every_existing_caller(self):
        for script in ("ensure-wave-cycle-topics.sh", "ensure-es-trade-tools-topics.sh"):
            tmp = Path(tempfile.mkdtemp())
            state, calls = tmp / "state.json", tmp / "calls"
            state.write_text(json.dumps({"topics": {}}))
            calls.touch()
            env = dict(os.environ, PATH=f"{cli_dir(tmp)}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls), KAFKA_BOOTSTRAP_SERVERS="b:1")
            self.assertEqual(subprocess.run(["bash", str(ROOT / "scripts/kafka" / script)], capture_output=True, text=True, env=env).returncode, 0, script)
            creates = [c for c in calls.read_text().splitlines() if "--create" in c]
            self.assertTrue(creates)
            for c in creates:
                self.assertIn("--config max.message.bytes=1048576 --config min.insync.replicas=1", c, c)


class TopicsEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES",
                              "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS",
                              "OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES", "OPTIONS_EDGE_ES4_TOPICS"])

    def test_the_four_topics_are_declared_one_partition_exact_with_the_barriers_shape(self):
        retention = dict(x.split("=", 1) for x in self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        for t, (policy, ret) in SHAPES.items():
            self.assertIn(f"{t}:1", self.d["OPTIONS_EDGE_TOPICS"], t)
            self.assertIn(t, self.d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
            self.assertEqual(retention[t], ret, t)
            self.assertEqual(t in self.d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], policy == "compact", t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
            self.assertNotRegex(t, r"\.v\d+$")
            self.assertNotIn(t, self.d["OPTIONS_EDGE_ES4_TOPICS"], "the service runs in dev and production, never on es4 (no options.indicators.bars there)")

    def test_the_checkpoint_is_reset_rebuildable_with_its_inputs_and_nothing_is_reset_preserved(self):
        self.assertIn(CP, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertIn("es.futures.footprint.bars", self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], "the input the checkpoint's offsets cover is itself reset")
        for t in TOPICS:
            self.assertNotIn(t, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        for t in TOPICS:
            self.assertIsNone(re.search(regex, t), f"premarket-reset must wipe {t} together with its inputs")

    def test_only_the_checkpoint_carries_a_max_message_bytes_contract_at_the_barriers_value(self):
        self.assertEqual(self.d["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"], [f"{CP}=9437184"])

    def test_the_inputs_have_the_partition_counts_the_manifest_declares(self):
        self.assertIn("options.indicators.bars:8", self.d["OPTIONS_EDGE_TOPICS"])
        self.assertIn("es.futures.footprint.bars:1", self.d["OPTIONS_EDGE_TOPICS"])
        env = {e["name"]: e.get("value") for e in self.container("k8s/services/break-classifier/base/break-classifier-deployment.yaml")["env"]}
        self.assertEqual(env["BREAK_SPX_BARS_PARTITIONS"], "8")
        self.assertEqual((env["BREAK_ES_FOOTPRINT_TOPIC"], env["BREAK_PARENT_TOPIC"], env["BREAK_PIVOT_TOPIC"]), ("es.futures.footprint.bars", X1, X2))

    @staticmethod
    def container(rel: str) -> dict:
        return yaml.safe_load(read(rel))["spec"]["template"]["spec"]["containers"][0]


class MaxMessageBytesMechanism(unittest.TestCase):
    """topic-shape.lib.sh create_topic / alter_topic_config honour OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES; an unlisted topic gets no max.message.bytes."""

    def run_lib(self, body: str, topics: dict | None = None):
        tmp = Path(tempfile.mkdtemp())
        state, calls = tmp / "state.json", tmp / "calls"
        state.write_text(json.dumps({"topics": topics or {}}))
        calls.touch()
        script = (f'set -euo pipefail; source "{ROOT}/scripts/kafka/topics.env"; source "{ROOT}/scripts/kafka/topic-shape.lib.sh"; '
                  f'REPLICATION_FACTOR=1 RETENTION_MS=604800000 CLEANUP_POLICY=delete MIN_ISR=1 KAFKA_BOOTSTRAP_SERVERS=b:1 KAFKA_TOPIC_CONFIG_RETRY_ATTEMPTS=1; {body}')
        env = dict(os.environ, PATH=f"{cli_dir(tmp)}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls))
        r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
        return r, json.loads(state.read_text())["topics"]

    def test_create_topic_sets_the_bound_for_the_declared_topic_only(self):
        r, topics = self.run_lib(f'create_topic {CP} 1; create_topic {OUT} 1')
        self.assertEqual(r.returncode, 0, r.stderr)
        cp = topics[CP]["configs"]
        self.assertEqual((cp["max.message.bytes"], cp["cleanup.policy"], cp["segment.bytes"]), ("9437184", "compact", "16777216"))
        self.assertNotIn("max.message.bytes", topics[OUT]["configs"])

    def test_alter_topic_config_reapplies_the_bound_to_an_existing_topic(self):
        r, topics = self.run_lib(f'alter_topic_config {CP} compact; alter_topic_config {OUT} delete', {CP: shaped("compact", "-1"), OUT: shaped("delete", DAYS90)})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(topics[CP]["configs"]["max.message.bytes"], "9437184")
        self.assertEqual(topics[OUT]["configs"]["max.message.bytes"], str(ONE_MIB), "an unlisted topic's bound is untouched")

    def test_the_es4_topic_set_never_inherits_the_dev_prod_declaration(self):
        r, _ = self.run_lib('TOPIC_SET=es4; topic_shape_resolve_declarations >/dev/null; echo "[$(topic_max_message_bytes ' + CP + ')]"')
        self.assertEqual(r.stdout.strip().splitlines()[-1], "[]", r.stderr)


class DevCleanReconcilesTheBound(unittest.TestCase):
    """scripts/ops/dev-cleanup.sh: the create pass and the reconcile pass carry max.message.bytes, driven by the real topics.env declarations of the four topics."""

    @staticmethod
    def function(name: str) -> str:
        return re.search(rf"\n({name}\(\) \{{.*?\n\}}\n)", read("scripts/ops/dev-cleanup.sh"), re.S).group(1)

    def harness(self, existing: dict):
        tmp = Path(tempfile.mkdtemp())
        keep = "|".join(re.escape(t) for t in TOPICS)
        full = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"; for v in OPTIONS_EDGE_TOPICS OPTIONS_EDGE_COMPACTED_TOPICS OPTIONS_EDGE_PURE_COMPACT_TOPICS '
                               f'OPTIONS_EDGE_EXACT_PARTITION_TOPICS OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES '
                               f'OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES; do echo "$v=${{!v}}"; done'], capture_output=True, text=True, check=True).stdout
        lines = []
        for line in full.splitlines():
            name, _, value = line.partition("=")
            lines.append(f'{name}="{" ".join(e for e in value.split() if re.match(rf"^({keep})(:|=|$)", e))}"')
        repo = tmp / "deploy"
        (repo / "scripts/kafka").mkdir(parents=True)
        (repo / "scripts/kafka/topics.env").write_text("\n".join(lines) + "\n")
        for cmd in (["init", "-q"], ["add", "."], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"]):
            subprocess.run(["git", "-C", str(repo), *cmd], check=True, capture_output=True)
        state, calls = tmp / "state.json", tmp / "calls"
        state.write_text(json.dumps({"topics": existing}))
        calls.touch()
        script = "\n".join([
            "set -uo pipefail", f'DEPLOY_REPO="{repo}"; TOPICS_ENV_REF="HEAD:scripts/kafka/topics.env"; KT=kafka-topics; KC=kafka-configs; BS=b:1',
            "ensure_partition_only_topics() { :; }; ensure_dev_only_estimate_topics() { :; }; export DEV_CLEANUP_TOPIC_PARALLELISM=1",
            self.function("topic_desired"), self.function("reconcile_declared_topics"), self.function("create_topics_parallel"), self.function("ensure_topics"),
            "ensure_topics"])
        env = dict(os.environ, PATH=f"{cli_dir(tmp)}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls))
        r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
        return r, json.loads(state.read_text())["topics"], calls.read_text().splitlines()

    def test_a_wipe_recreates_the_checkpoint_at_the_bound_and_the_others_at_their_retention(self):
        r, topics, _ = self.harness({})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(topics[CP]["configs"]["max.message.bytes"], "9437184")
        self.assertEqual(topics[CP]["configs"]["cleanup.policy"], "compact")
        self.assertEqual(topics[OUT]["configs"]["retention.ms"], DAYS90)
        self.assertEqual(topics[X1]["configs"]["retention.ms"], TWO_SESSIONS)
        for t in TOPICS:
            self.assertEqual(topics[t]["partitions"], 1, t)
            if t != CP:
                self.assertNotIn("max.message.bytes", topics[t]["configs"], t)

    def test_the_reconcile_pass_repairs_a_checkpoint_a_producer_auto_created_at_the_broker_default(self):
        existing = {t: shaped(p, r, max_bytes=ONE_MIB) for t, (p, r) in SHAPES.items()}
        r, topics, _ = self.harness(existing)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn(f"reconciled {CP} -> max.message.bytes=9437184", r.stdout)
        self.assertEqual(topics[CP]["configs"]["max.message.bytes"], "9437184")

    def test_an_already_right_checkpoint_is_not_touched(self):
        existing = {t: shaped(p, r, max_bytes=NINE_MIB if t == CP else ONE_MIB) for t, (p, r) in SHAPES.items()}
        r, _, calls = self.harness(existing)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("shape check: 0 config(s) reconciled", r.stdout)
        self.assertFalse(any("--alter" in c for c in calls))


class Registered(unittest.TestCase):
    def test_services_yaml_registers_it_as_standalone_dev_and_production(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        self.assertEqual((reg[SVC]["managedBy"], reg[SVC]["envs"], reg[SVC]["image"], reg[SVC]["deployments"], reg[SVC]["sliceGenerated"]),
                         ("standalone", ["dev", "production"], IMG, [DEP], True))

    def test_the_image_variable_is_in_every_image_resolution_list(self):
        self.assertEqual(len(re.findall(rf"^{VAR}=", read("scripts/deploy/resolve-images.sh"), re.M)), 2, "both heredoc branches")
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/image-preflight.sh", "Jenkinsfile"):
            self.assertIn(VAR, read(rel), rel)
        self.assertGreaterEqual(read("Jenkinsfile").count(VAR), 3, "param, defaults map and the promote list")
        self.assertIn(f"'{VAR}': '{SVC}'", read("Jenkinsfile"))

    def test_image_tags_and_overlays_reference_it(self):
        self.assertIn(f"{DEP}: 192.168.100.252:5000/{IMG}:dev", read("image-tags/dev.yaml"))
        self.assertIn(f"{DEP}: 192.168.100.252:5000/{IMG}:prod", read("image-tags/production.yaml"))
        for env, tag in (("dev", "dev"), ("production", "prod")):
            text = read(f"k8s/overlays/{env}/kustomization.yaml")
            self.assertIn(f"../../services/{SVC}/base", text)
            self.assertIn(f"- path: break-classifier-{'dev' if env == 'dev' else 'prod'}-enable-patch.yaml\n    target:\n      kind: Deployment\n      name: {DEP}", text)
            self.assertRegex(text, rf"- name: 192.168.100.252:5000/{IMG}\n(    newName: host.docker.internal:5001/{IMG}\n)?    newTag: {tag}\n")
        self.assertIn(f"newName: host.docker.internal:5001/{IMG}", read("k8s/overlays/dev/kustomization.yaml"))

    def test_it_is_in_no_hold_list_because_it_runs_live_on_both_environments(self):
        pattern = rf"(?<![\w-]){re.escape(DEP)}(?![\w-])"
        for rel in ("scripts/ops/oe-boot-bringup.sh", "scripts/ops/morning-autostart.sh"):
            text = "\n".join(l for l in read(rel).splitlines() if re.match(r"\s*KEEP_DOWN=", l))
            self.assertTrue(text, rel)
            self.assertNotRegex(text, pattern, rel)
        text = "\n".join(l for l in read("scripts/ops/dev-cleanup.sh").splitlines() if l.startswith("DISABLED_DEV="))
        self.assertTrue(text)
        self.assertNotRegex(text, pattern)

    def test_the_service_deploy_job_offers_it_registers_its_schemas_and_runs_its_barrier(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertIn(f"'{SVC}',", text)
        register = text[text.index("stage('Register TA-books schemas')"):text.index("stage('Recreate indicator topics (Avro cutover)')")]
        self.assertIn("params.REGISTER_TA_SCHEMAS && (params.SERVICE == 'market-structure'", register)
        self.assertIn("params.SERVICE == 'break-classifier'))", register)
        stage = text[text.index("stage('Verify break-classification topics')"):text.index("stage('Verify market-structure output topics')")]
        self.assertIn("params.SERVICE == 'break-classifier' && !params.DEPLOY_DRY_RUN", stage)
        self.assertIn("scripts/kafka/ensure-break-classification-topics.sh", stage)
        self.assertLess(text.index("stage('Verify break-classification topics')"), text.index("stage('Deploy (service-scoped)')"), "the barrier precedes the rollout")
        self.assertLess(text.index("stage('Register TA-books schemas')"), text.index("stage('Verify break-classification topics')"), "the schemas are registered before the barrier")

    def test_the_barrier_script_is_executable_and_sources_the_shared_routine(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())


class FlagsAndReplicas(unittest.TestCase):
    ENABLE = {"dev": "k8s/overlays/dev/break-classifier-dev-enable-patch.yaml", "production": "k8s/overlays/production/break-classifier-prod-enable-patch.yaml"}
    BASE = "k8s/services/break-classifier/base/break-classifier-deployment.yaml"

    @staticmethod
    def deployment(rel: str) -> dict:
        docs = [d for d in yaml.safe_load_all(read(rel)) if d]
        assert [d["kind"] for d in docs] == ["Deployment"], rel
        return docs[0]

    def test_the_shared_base_is_inert_and_single_replica(self):
        d = self.deployment(self.BASE)
        self.assertEqual((d["spec"]["replicas"], d["spec"]["strategy"]["type"], d["metadata"]["name"]), (0, "Recreate", DEP))
        self.assertEqual(env_values(ROOT / self.BASE, "BREAK_CLASSIFIER_ENABLED"), ["false"])
        c = d["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(c["name"], "break-classifier")
        self.assertEqual(c["image"], f"192.168.100.252:5000/{IMG}:dev")
        self.assertEqual(c["ports"][0]["containerPort"], 8171)
        self.assertEqual((c["readinessProbe"]["httpGet"]["path"], c["livenessProbe"]["httpGet"]["path"]), ("/health/ready", "/health/live"))
        self.assertEqual(c["resources"], {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "768Mi"}})
        self.assertEqual([list(x) for x in (c["envFrom"][0], c["envFrom"][1])], [["configMapRef"], ["secretRef"]])
        self.assertEqual((c["envFrom"][0]["configMapRef"]["name"], c["envFrom"][1]["secretRef"]["name"]), ("options-edge-config", "options-edge-runtime-secrets"))
        env = {e["name"]: e.get("value") for e in c["env"]}
        self.assertEqual(env["BREAK_X6_ATTESTATION_PATH"], "/app/release/x6-attestation.txt")
        self.assertEqual(env["BREAK_HEALTH_PORT"], "8171")
        self.assertNotIn("JAVA_TOOL_OPTIONS", env, "the image ENTRYPOINT fixes the heap; a value here would be ignored and lie")
        self.assertNotIn("command", c, "the image ENTRYPOINT is the command; the manifest overrides nothing")
        self.assertNotIn("args", c)
        self.assertNotIn("TOPIC_PREFIX", env)

    def test_each_environments_slice_runs_one_live_replica_and_the_shared_base_does_not(self):
        for env in ("dev", "production"):
            d = self.deployment(f"k8s/services/{SVC}/overlays/{env}/manifest.yaml")
            self.assertEqual((d["spec"]["replicas"], d["spec"]["strategy"]["type"]), (1, "Recreate"), env)
            self.assertEqual(env_values(ROOT / f"k8s/services/{SVC}/overlays/{env}/manifest.yaml", "BREAK_CLASSIFIER_ENABLED"), ["true"], env)
            self.assertEqual(env_values(ROOT / f"k8s/services/{SVC}/overlays/{env}/manifest.yaml", "ENVIRONMENT"), [env], env)
            self.assertIn(IMG, d["spec"]["template"]["spec"]["containers"][0]["image"])
            patch = self.deployment(self.ENABLE[env])
            self.assertEqual(patch["spec"]["replicas"], 1)
            self.assertEqual(env_values(ROOT / self.ENABLE[env], "BREAK_CLASSIFIER_ENABLED"), ["true"])

    def test_the_flag_is_true_only_in_the_two_enable_patches_and_their_generated_slices(self):
        allowed = {*self.ENABLE.values(), f"k8s/services/{SVC}/overlays/dev/manifest.yaml", f"k8s/services/{SVC}/overlays/production/manifest.yaml"}
        for path in (ROOT / "k8s").rglob("*.yaml"):
            rel = str(path.relative_to(ROOT))
            for v in env_values(path, "BREAK_CLASSIFIER_ENABLED"):
                self.assertEqual(v, "true" if rel in allowed else "false", rel)
        for rel in allowed:
            self.assertEqual(env_values(ROOT / rel, "BREAK_CLASSIFIER_ENABLED"), ["true"], rel)

    def test_the_gateway_relay_is_on_in_both_environments_in_source_and_generated_slice(self):
        for rel in ("k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/overlays/production/feed-gateway-prod-env-patch.yaml",
                    "k8s/services/feed-gateway/overlays/dev/manifest.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"):
            self.assertEqual(env_values(ROOT / rel, "GATEWAY_BREAK_CLASSIFICATION_ENABLED"), ["true"], rel)
            self.assertEqual(env_values(ROOT / rel, "KAFKA_BREAK_CLASSIFICATION_TOPIC"), [], "the code default is exactly the declared topic")

    def test_the_production_render_equals_the_committed_slice(self):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/production")], capture_output=True, text=True, check=True, timeout=180).stdout
        docs = [d for d in yaml.safe_load_all(out) if isinstance(d, dict) and d.get("kind") == "Deployment" and d["metadata"]["name"] == DEP]
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["spec"]["replicas"], 1)
        self.assertEqual(docs[0], self.deployment(f"k8s/services/{SVC}/overlays/production/manifest.yaml"))

    def test_the_patch_headers_state_the_honest_state_and_that_no_broker_is_changed(self):
        for rel in self.ENABLE.values():
            text = read(rel)
            for must in ("p=1681/2001", "NO_MEASURED_EDGE", "NO producer on main", "87511c9acb7a897850c2fa436d59c45eaef24481", "KEEP_DOWN",
                         "no digest is recorded here"):
                self.assertIn(must, text, f"{rel}: {must}")
            self.assertNotRegex(text, r"sha256:[0-9a-f]{64}", "an image digest is pinned at deploy time, never invented")


class SchemaMap(unittest.TestCase):
    MAP = ROOT / "scripts/kafka/schema-id-maps/break-classifier.map"
    ROWS = [("com.optionsedge.contracts.breakclass.BreakMark", 2601, "BreakMark.avsc"), ("com.optionsedge.contracts.breakclass.BreakOutcome", 2602, "BreakOutcome.avsc"),
            ("com.optionsedge.contracts.breakclass.BreakStatus", 2603, "BreakStatus.avsc"),
            ("com.optionsedge.contracts.breakclass.BreakClassifierCheckpoint", 2604, "BreakClassifierCheckpoint.avsc"),
            ("com.optionsedge.contracts.breakclass.input.BreakParentRecord", 2605, "BreakParentRecord.avsc"),
            ("com.optionsedge.contracts.breakclass.input.SwingPivotRecord", 2606, "SwingPivotRecord.avsc"),
            ("com.optionsedge.contracts.breakclass.EqualLevelRecord", 2607, "EqualLevelRecord.avsc")]

    def rows(self):
        return [tuple(l.split("|")) for l in self.MAP.read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")]

    def test_the_map_is_exactly_the_six_subjects_of_contracts_0_37_0_and_the_equal_level_record_of_0_44_0_with_version_1(self):
        self.assertEqual([(s, int(i), f) for s, i, f in self.rows()], self.ROWS)

    def test_the_rows_match_a_contracts_checkout_when_one_is_available(self):
        """CONTRACTS_SCHEMAS_DIR (the checkout's schemas-registry/ta at 87511c9a) or the sibling checkout; absent in CI, where the stand-in registry tests above cover the map."""
        candidates = [os.environ.get("CONTRACTS_SCHEMAS_DIR", ""), str(ROOT.parent / "options-edge-contracts/schemas-registry/ta")]
        base = next((Path(c) for c in candidates if c and (Path(c) / "BreakMark.avsc").is_file()), None)
        if base is None:
            self.skipTest("no contracts checkout")
        for subject, _, file in self.ROWS:
            schema = json.loads((base / file).read_text())
            self.assertEqual(f"{schema['namespace']}.{schema['name']}", subject)


if __name__ == "__main__":
    unittest.main()
