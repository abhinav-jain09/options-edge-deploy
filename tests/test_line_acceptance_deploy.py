# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""Deploy wiring of line-acceptance-service (VWAP-ACCEPTANCE-REJECTION): DEV ONLY, LIVE and UNVALIDATED since 2026-11-02 (owner override of design K9): replicas 1, LINE_ACCEPTANCE_ENABLED "true",
five evidence/line gates true and GATE_PHASEF_EVIDENCE false, NOT held down by any lifecycle list.

Covers: the base and the dev overlay (source render, committed slice) at replicas 1 / flag "true" / the gates / health port 8172, the absence of the name from the three lifecycle lists, the six topics' declared policies
in topics.env (status NOT compacted and retention -1), the topic barrier (scripts/kafka/ensure-line-acceptance-topics.sh) against the stateful stand-in tests/fake_kafka_cli.py, the
fixed-id schema map, the registration of the service everywhere a service must be, and the absence of any production wiring."""
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
BARRIER = ROOT / "scripts/kafka/ensure-line-acceptance-topics.sh"
SVC, DEP, CONTAINER, IMG, VAR = "line-acceptance", "line-acceptance-service", "line-acceptance", "options-edge-line-acceptance", "LINE_ACCEPTANCE_IMAGE"
P = "options.spx.line-acceptance"
OUT, MON, STATUS, CP, JOURNAL, DLQ = P, P + ".monitor", P + ".status", P + ".checkpoint", P + ".journal", P + ".dlq"
TOPICS = (OUT, MON, STATUS, CP, JOURNAL, DLQ)
SEVEN_DAYS, EIGHT_MIB, NINE_MIB, ONE_MIB = "604800000", 8388608, 9437184, 1048576
# topic -> (cleanup.policy, retention.ms, topic-level max.message.bytes the barrier creates it with)
SHAPES = {OUT: ("delete", SEVEN_DAYS, ONE_MIB), MON: ("compact", "-1", ONE_MIB), STATUS: ("delete", "-1", ONE_MIB), CP: ("compact", "-1", NINE_MIB),
          JOURNAL: ("delete", SEVEN_DAYS, NINE_MIB), DLQ: ("delete", SEVEN_DAYS, ONE_MIB)}
IDS = {"LineEpisodeCore": 3101, "LineEpisodeEvidence": 3102, "LineEpisodeOutcome": 3103, "LineEpisodeIntegrity": 3104, "LeanEvent": 3105, "SessionCoverage": 3106,
       "LineMonitorStatus": 3107, "CalibrationStatus": 3108, "LineAcceptanceCheckpoint": 3109, "IngressBatch": 3110, "LineAcceptanceDlq": 3111}


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
    return {t: shaped(p, r, max_bytes=b) for t, (p, r, b) in SHAPES.items()}


def run_barrier(topics: dict, environment: str | None = "dev", topic_alter_fails: bool = False):
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
    if topic_alter_fails:
        env["FAKE_KAFKA_TOPIC_MAXBYTES_ALTER_FAILS"] = "1"
    r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_the_six_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c or "message.max.bytes" in c], [], "no broker-wide change")
        for t, (policy, ret, max_bytes) in SHAPES.items():
            cfg = state["topics"][t]["configs"]
            self.assertEqual((state["topics"][t]["partitions"], cfg["cleanup.policy"], cfg["retention.ms"], cfg["max.message.bytes"]), (1, policy, ret, str(max_bytes)), t)
        cp = state["topics"][CP]["configs"]
        self.assertEqual((cp["segment.bytes"], cp["max.compaction.lag.ms"]), ("16777216", "300000"))
        self.assertIn("=== ensure-line-acceptance-topics OK ===", out)

    def test_the_barrier_is_dev_only_and_fails_closed_before_touching_anything(self):
        for environment in ("production", "es4", "", None):
            rc, out, calls, state = run_barrier({}, environment=environment)
            self.assertEqual(rc, 1, f"{environment!r}: {out}")
            self.assertIn("DEV ONLY", out)
            self.assertEqual(calls, [], f"{environment!r}: no CLI call was made")
            self.assertEqual(state["topics"], {})

    def test_a_second_run_changes_nothing(self):
        _, _, _, state = run_barrier({})
        rc, out, calls, _ = run_barrier(state["topics"])
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--create" in c or "--delete" in c for c in calls))
        self.assertFalse(any("max.message.bytes" in c and "--alter" in c for c in calls))

    def test_an_existing_checkpoint_or_journal_without_its_own_limit_is_pinned_on_that_topic_only(self):
        for topic in (CP, JOURNAL):
            existing = right_topics()
            existing[topic]["configs"].pop("max.message.bytes")
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 0, out)
            pins = [c for c in calls if "--entity-type topics" in c and "--alter" in c and "max.message.bytes" in c]
            self.assertEqual(len(pins), 1, topic)
            self.assertIn(f"--entity-name {topic} --alter --add-config max.message.bytes={NINE_MIB}", pins[0])
            self.assertEqual(state["topics"][topic]["configs"]["max.message.bytes"], str(NINE_MIB))

    def test_a_failed_pin_fails_closed_and_creates_nothing_else(self):
        existing = right_topics()
        existing[CP]["configs"].pop("max.message.bytes")
        del existing[DLQ]
        rc, out, calls, state = run_barrier(existing, topic_alter_fails=True)
        self.assertEqual(rc, 1, out)
        self.assertIn("FAILED", out)
        self.assertNotIn(DLQ, state["topics"])
        self.assertFalse(any("--create" in c for c in calls))

    def test_every_difference_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {
            "output retention one short": (OUT, lambda d: d["configs"].update({"retention.ms": "604799999"})),
            "output compacted": (OUT, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "monitor delete": (MON, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "status COMPACTED (every version must survive)": (STATUS, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "status compact,delete": (STATUS, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "status finite retention": (STATUS, lambda d: d["configs"].update({"retention.ms": SEVEN_DAYS})),
            "checkpoint delete": (CP, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "checkpoint 32 partitions": (CP, lambda d: d.update(partitions=32)),
            "journal retention differs": (JOURNAL, lambda d: d["configs"].update({"retention.ms": "86400000"})),
            "journal compacted": (JOURNAL, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "dlq 32 partitions": (DLQ, lambda d: d.update(partitions=32)),
            "min.insync.replicas below the minimum": (DLQ, lambda d: d["configs"].update({"min.insync.replicas": "0"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or ("--alter" in c and "--entity-type topics" in c) for c in calls), f"{label}: a barrier never alters or recreates a topic")
            self.assertEqual(state["topics"][topic]["partitions"], existing[topic]["partitions"], label)

    def test_the_barrier_is_executable_and_sources_the_shared_routine(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())


class TopicsEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES",
                              "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS",
                              "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES", "OPTIONS_EDGE_ES4_TOPICS"])

    def test_the_six_topics_are_declared_one_partition_exact_with_the_declared_policy_and_retention(self):
        retention = dict(x.split("=", 1) for x in self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        for t, (policy, ret, _) in SHAPES.items():
            self.assertIn(f"{t}:1", self.d["OPTIONS_EDGE_TOPICS"], t)
            self.assertIn(t, self.d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
            self.assertEqual(retention[t], ret, t)
            self.assertEqual(t in self.d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], policy == "compact", t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_ES4_TOPICS"], t)
            self.assertNotRegex(t, r"\.v\d+$")
        self.assertEqual(len([x for x in self.d["OPTIONS_EDGE_TOPICS"] if x.startswith(P)]), 6)

    def test_the_status_topic_is_not_compacted_and_never_ages_out(self):
        self.assertNotIn(STATUS, self.d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(STATUS, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"])
        self.assertIn(f"{STATUS}=-1", self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        self.assertIn(f"{STATUS}=-1", self.d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"], "retention.ms=-1 alone does not stop byte-based deletion")

    def test_the_checkpoint_and_journal_carry_their_own_topic_level_limit_and_nothing_else_does(self):
        self.assertEqual(sorted(x for x in self.d["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"] if x.startswith(P)), sorted([f"{CP}=9437184", f"{JOURNAL}=9437184"]))
        self.assertIn("options.spx.break-classification.checkpoint=9437184", self.d["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"], "the break-classifier declaration survives")

    def test_every_retention_minus_one_topic_has_exactly_one_reset_class(self):
        self.assertIn(STATUS, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertNotIn(STATUS, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        for t in (MON, CP):
            self.assertIn(t, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        self.assertIsNotNone(re.search(regex, STATUS), "the pre-market reset must spare the status topic")
        for t in TOPICS:
            if t != STATUS:
                self.assertIsNone(re.search(regex, t), t)

    def test_the_checkpoint_gets_the_recovery_topic_compaction_cadence(self):
        r = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topic-shape.lib.sh"; topic_is_recovery_checkpoint {CP} && echo yes; topic_is_recovery_checkpoint {MON} || echo no'],
                           capture_output=True, text=True)
        self.assertEqual(r.stdout.split(), ["yes", "no"], r.stderr)


def deployment(rel: str) -> dict:
    docs = [d for d in yaml.safe_load_all(read(rel)) if d]
    assert [d["kind"] for d in docs] == ["Deployment"], rel
    return docs[0]


GATES = {"GATE_STRUCTURE_ENABLED": "true", "GATE_GEOMETRY_ENABLED": "true", "GATE_VWAP_ENABLED": "true", "GATE_FOOTPRINT_ENABLED": "true", "GATE_CANDLES_ENABLED": "true",
         "GATE_PHASEF_EVIDENCE": "false", "GATE_HOST_COMPLETENESS": "BAR_MARKER"}


class LiveOnDev(unittest.TestCase):
    BASE = f"k8s/services/{SVC}/base/line-acceptance-deployment.yaml"
    SLICE = f"k8s/services/{SVC}/overlays/dev/manifest.yaml"

    def check(self, d: dict, label: str):
        self.assertEqual((d["metadata"]["name"], d["spec"]["replicas"], d["spec"]["strategy"]["type"]), (DEP, 1, "Recreate"), label)
        c = d["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(c["name"], CONTAINER, label)
        self.assertIn(IMG, c["image"], label)
        env = {e["name"]: e.get("value") for e in c["env"]}
        self.assertEqual(env["LINE_ACCEPTANCE_ENABLED"], "true", label)
        self.assertEqual(env["ENVIRONMENT"], "dev", label)
        self.assertEqual(env["LINE_ACC_HEALTH_PORT"], "8172", label)
        for name, value in GATES.items():
            self.assertEqual(env[name], value, f"{label}: {name}")
        self.assertNotIn("GATE_VWAP_ANALYSIS", env, label)   # the service refuses to start with it true; its default (false) is the only value
        self.assertEqual([p["containerPort"] for p in c["ports"]], [8172], label)
        self.assertEqual((c["readinessProbe"]["httpGet"]["path"], c["livenessProbe"]["httpGet"]["path"]), ("/health/ready", "/health/live"), label)

    def test_the_base_is_live(self):
        self.check(deployment(self.BASE), "base")
        self.assertEqual(env_values(ROOT / self.BASE, "LINE_ACCEPTANCE_ENABLED"), ["true"])

    def test_the_dev_slice_is_live(self):
        self.check(deployment(self.SLICE), "dev slice")
        self.assertEqual(env_values(ROOT / self.SLICE, "LINE_ACCEPTANCE_ENABLED"), ["true"])

    def test_the_rendered_dev_overlay_equals_the_committed_slice_and_is_live(self):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/dev")], capture_output=True, text=True, check=True, timeout=180).stdout
        docs = [d for d in yaml.safe_load_all(out) if isinstance(d, dict) and d.get("kind") == "Deployment" and d["metadata"]["name"] == DEP]
        self.assertEqual(len(docs), 1)
        self.check(docs[0], "dev render")
        self.assertEqual(docs[0], deployment(self.SLICE))

    def test_production_and_experiment_never_render_it(self):
        for env in ("production", "experiment"):
            out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays" / env)], capture_output=True, text=True, check=True, timeout=180).stdout
            self.assertNotIn(DEP, out, env)
            self.assertNotIn("LINE_ACCEPTANCE", out, env)

    def test_the_flag_is_true_only_in_the_dev_only_base_and_slice(self):
        for path in list((ROOT / "k8s").rglob("*.yaml")):
            for v in env_values(path, "LINE_ACCEPTANCE_ENABLED"):
                self.assertEqual(str(path.relative_to(ROOT)) in (self.BASE, self.SLICE), v == "true", str(path.relative_to(ROOT)))

    def test_health_port_8172_is_not_shared_with_another_service(self):
        owners = [str(p.relative_to(ROOT)) for p in (ROOT / "k8s/services").glob("*/base/*.yaml") if re.search(r"containerPort: 8172\b", p.read_text())]
        self.assertEqual(owners, [self.BASE])


class NotHeldDown(unittest.TestCase):
    """The owner switched it on for dev (2026-11-02): no lifecycle list may name it, or a start / boot would put it back to 0 (and a stale entry would be a hold nobody intended)."""
    pattern = rf"(?<![\w-]){re.escape(DEP)}(?![\w-])"

    def lists(self):
        return (("DISABLED_DEV", re.search(r"^DISABLED_DEV='([^']*)'$", read("scripts/ops/dev-cleanup.sh"), re.M).group(1).split("|")),
                ("boot KEEP_DOWN", re.search(r"^KEEP_DOWN='([^']*)'$", read("scripts/ops/oe-boot-bringup.sh"), re.M).group(1).split("|")),
                ("morning KEEP_DOWN", re.search(r'^KEEP_DOWN="\$\{KEEP_DOWN:-([^}]*)\}"$', read("scripts/ops/morning-autostart.sh"), re.M).group(1).split()))

    def test_it_is_in_none_of_the_three_lists_and_the_prod_lists_agree(self):
        lists = self.lists()
        for name, entries in lists:
            self.assertEqual(entries.count(DEP), 0, name)
        self.assertEqual(set(lists[1][1]), set(lists[2][1]), "tests/test_keep_down_lists_agree.py")

    def test_the_other_holds_are_untouched(self):
        for name, entries in self.lists():
            self.assertIn("corridor-gauge-service", entries, name)
            self.assertIn("break-score-service", entries, name)

    def test_dev_cleanup_documents_that_it_does_not_run_the_barrier(self):
        text = read("scripts/ops/dev-cleanup.sh")
        self.assertIn("does NOT run", text[text.index("line-acceptance-service was held here by name"):])
        self.assertNotIn("ensure-line-acceptance-topics.sh", "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#")))


class Registered(unittest.TestCase):
    def test_services_yaml_registers_it_as_standalone_and_dev_only(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        self.assertEqual((reg[SVC]["managedBy"], reg[SVC]["envs"], reg[SVC]["image"], reg[SVC]["deployments"], reg[SVC]["sliceGenerated"]), ("standalone", ["dev"], IMG, [DEP], True))

    def test_the_image_variable_is_in_the_dev_only_image_resolution_lists(self):
        self.assertEqual(len(re.findall(rf"^{VAR}=", read("scripts/deploy/resolve-images.sh"), re.M)), 2, "both dev-guarded heredoc branches")
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/verify-running-images.sh"):
            self.assertIn(VAR, read(rel), rel)
        self.assertIn(f'("{DEP}", "{CONTAINER}"): "{VAR}"', read("scripts/deploy/verify-running-images.sh"))
        for rel in ("scripts/deploy/resolve-images.sh", "scripts/deploy/image-lock.sh"):
            text = read(rel)
            i = text.index(VAR)
            self.assertIn('"${ENVIRONMENT:-dev}" = "dev"', text[max(0, i - 800):i], f"{rel}: only emitted for dev")

    def test_the_dev_overlay_references_it_and_maps_the_image(self):
        text = read("k8s/overlays/dev/kustomization.yaml")
        self.assertIn(f"../../services/{SVC}/base", text)
        self.assertRegex(text, rf"- name: 192.168.100.252:5000/{IMG}\n    newName: host.docker.internal:5001/{IMG}\n    newTag: dev\n")
        self.assertNotRegex(text, rf"path: {SVC}-.*patch", "no enable patch exists")
        self.assertIn(f"{DEP}: host.docker.internal:5001/{IMG}:dev", read("image-tags/dev.yaml"))

    def test_the_service_deploy_job_offers_it_registers_its_schemas_and_runs_its_dev_only_barrier(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertIn(f"'{SVC}',", text)
        register = text[text.index("stage('Register TA-books schemas')"):text.index("stage('Recreate indicator topics (Avro cutover)')")]
        self.assertIn("params.SERVICE == 'line-acceptance'))", register)
        self.assertIn("params.SERVICE == 'line-acceptance' && params.ENVIRONMENT != 'dev'", register)
        stage = text[text.index("stage('Verify line-acceptance topics')"):text.index("stage('Verify market-structure output topics')")]
        self.assertIn("(params.SERVICE == 'line-acceptance' || params.SERVICE == 'es-liquidity-wall' || params.SERVICE == 'es-liquidity-engine' || params.SERVICE == 'liquidity-validator') && !params.DEPLOY_DRY_RUN", stage)
        self.assertIn("params.ENVIRONMENT != 'dev'", stage)
        self.assertIn("scripts/kafka/ensure-line-acceptance-topics.sh", stage)
        self.assertLess(text.index("stage('Register TA-books schemas')"), text.index("stage('Verify line-acceptance topics')"))
        self.assertLess(text.index("stage('Verify line-acceptance topics')"), text.index("stage('Deploy (service-scoped)')"), "the barrier precedes the rollout")


class SchemaMap(unittest.TestCase):
    def test_the_map_is_exactly_the_eleven_records_and_ids_of_the_design(self):
        rows = [tuple(l.split("|")) for l in read("scripts/kafka/schema-id-maps/line-acceptance.map").splitlines() if l.strip() and not l.lstrip().startswith("#")]
        self.assertEqual([(s, int(i), f) for s, i, f in rows],
                         [(f"com.optionsedge.contracts.lineacc.{n}", i, f"{n}.avsc") for n, i in IDS.items()])


def sourced(environment: str | None) -> dict[str, list[str]]:
    """Every OPTIONS_EDGE_* variable of a real `source topics.env` (bash) that mentions a line-acceptance topic, as its line-acceptance tokens."""
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = (f'source "{ROOT}/scripts/kafka/topics.env"; for v in $(compgen -v OPTIONS_EDGE_); do '
              'case "${!v}" in *line-acceptance*) echo "$v=$(printf "%s\\n" ${!v} | grep line-acceptance | tr "\\n" " ")";; esac; done')
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {l.split("=", 1)[0]: l.split("=", 1)[1].split() for l in out.splitlines()}


class ProductionDeclaresNoTopic(unittest.TestCase):
    def test_an_explicit_production_source_declares_nothing_about_the_six_topics_anywhere(self):
        self.assertEqual(sourced("production"), {})

    def test_dev_and_unset_declare_all_six_with_the_same_shapes(self):
        for environment in ("dev", None):
            got = sourced(environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_TOPICS"]), sorted(f"{t}:1" for t in TOPICS), environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"]), sorted(TOPICS), environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_PURE_COMPACT_TOPICS"]), sorted([MON, CP]), environment)
            ret = dict(x.split("=") for x in got["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
            self.assertEqual(ret, {t: SHAPES[t][1] for t in TOPICS}, environment)
            self.assertEqual(got["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"], [f"{STATUS}=-1"], environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"]), sorted([f"{CP}=9437184", f"{JOURNAL}=9437184"]), environment)
            self.assertEqual(got["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], [STATUS], environment)
            self.assertEqual(sorted(got["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"]), sorted([MON, CP]), environment)

    def test_other_environments_are_not_treated_as_production(self):
        self.assertIn("OPTIONS_EDGE_TOPICS", sourced("Production "), "only the exact string 'production' strips (the PROD_ONLY predicate, inverted)")

    def test_production_keeps_every_other_declaration(self):
        def tokens(environment):
            env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
            env["ENVIRONMENT"] = environment
            out = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"; echo "$OPTIONS_EDGE_TOPICS"; echo "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"'],
                                 capture_output=True, text=True, check=True, env=env).stdout.splitlines()
            return [line.split() for line in out]
        dev, prod = tokens("dev"), tokens("production")
        for d, p in zip(dev, prod):
            self.assertEqual(sorted(x for x in d if x.startswith(P)).__len__(), 6)
            self.assertEqual([x for x in p if x.startswith(P)], [])
            # every declaration that is not line-acceptance's survives; the other dev-only family (es-liquidity-wall, futures.liquidity-wall*) is stripped by its own guard, its own test's business
            self.assertEqual([x for x in d if not x.startswith(P) and not x.startswith("futures.liquidity-wall") and not x.startswith("es.mbo.") and not x.startswith("futures.liquidity-validator")], [x for x in p if not x.startswith(P)])


class NoProductionWiring(unittest.TestCase):
    def test_no_production_file_names_the_service_its_image_or_its_topics(self):
        needles = (SVC, "LINE_ACCEPTANCE", "line-acceptance", "LINE_ACC_")
        for rel in ("k8s/overlays/production", "k8s/overlays/experiment", "k8s/es4", "k8s/base", "image-tags/production.yaml", "Jenkinsfile", "scripts/es4"):
            path = ROOT / rel
            files = [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]
            self.assertTrue(files, rel)
            for f in files:
                text = f.read_text(errors="ignore")
                for n in needles:
                    self.assertNotIn(n, text, f"{f.relative_to(ROOT)} must not mention {n}")

    def test_the_service_has_no_production_or_experiment_slice(self):
        self.assertEqual(sorted(p.name for p in (ROOT / f"k8s/services/{SVC}/overlays").iterdir()), ["dev"])

    def test_the_production_and_experiment_renders_do_not_contain_it(self):
        for env in ("production", "experiment"):
            out = subprocess.run(["kubectl", "kustomize", str(ROOT / f"k8s/overlays/{env}")], capture_output=True, text=True, check=True, timeout=180).stdout
            self.assertNotIn("line-acceptance", out, env)

    def test_only_dev_declares_the_service_in_the_registry(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        self.assertNotIn("production", reg[SVC]["envs"])


if __name__ == "__main__":
    unittest.main()
