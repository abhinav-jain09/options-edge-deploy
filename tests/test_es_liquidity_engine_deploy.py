# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""Deploy wiring of es-liquidity-engine-service (the ES MBO liquidity engine) and the TEMPORARY drop-classifier-ref reference deployment (the G3b parity gate's reference side).

Both are DEV ONLY, single replica, strategy Recreate, health port 8080, drop engine in ES_TRADES mode on the dev broker. The engine writes the default es.drop.* prefix; the reference writes
es.drop.ref.* so the two never share an output topic. Covers the base, the dev overlay slice, the registration everywhere a dev-only service must be, and the absence of any production wiring."""
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
BARRIER = ROOT / "scripts/kafka/ensure-es-liquidity-engine-topics.sh"
BASE = "es.mbo.level-base"
LEVEL_FLOW, HEALTH, FINAL, SNAPSHOT, EVENTS, CLOSURES = "es.mbo.level-flow", "es.mbo.health", "es.mbo.level-flow-final", "es.mbo.snapshot", "es.mbo.liquidity-events", "es.mbo.closures"
# THE COVERAGE MESSAGE (EsMboCoverage 3511): pure compact, one partition, keyed by contract.
COVERAGE = "es.mbo.coverage"
EVENTS_CAP = "134217728"
ONE_DAY, CAP = "86400000", "268435456"
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
NS = "com.optionsedge.contracts.esmbo"
# subject -> {version: id}. Health and the final rows have two versions (document A revision 12 sections 4.1 and 4.2, contracts 0.89.0): 3504 and 3506 are the frozen archival version 1.
VERSIONS = {"EsMboClosure": {1: 3501}, "EsMboLevelFlow": {1: 3502}, "EsMboEvent": {1: 3503}, "EsMboHealth": {1: 3504, 2: 3509}, "EsMboSnapshot": {1: 3505},
            "EsMboLevelFlowFinal": {1: 3506, 2: 3510}, "EsMboLevelBase": {1: 3507}, "EsMboClosureFinal": {1: 3508},
            # Coverage. Listing it here is what makes the map test require its registration row and
            # the manifest test verify its entry — without it both passed over the new subject
            # entirely, and the engine runs with auto.register.schemas=false, so an unregistered
            # subject is a runtime failure to publish rather than a governance nit (Codex deploy r2).
            "EsMboCoverage": {1: 3511}}
IDS = {n: max(v.values()) for n, v in VERSIONS.items()}   # the id the engine WRITES


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def deployment(rel: str) -> dict:
    return next(d for d in yaml.safe_load_all(read(rel)) if d and d.get("kind") == "Deployment")


def env_of(dep: dict) -> dict:
    return {e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}


CASES = {
    "es-liquidity-engine": dict(dep="es-liquidity-engine-service", container="es-liquidity-engine", image="options-edge-es-liquidity-engine", var="ES_LIQUIDITY_ENGINE_IMAGE", prefix=None),
    "drop-classifier-ref": dict(dep="drop-classifier-ref-service", container="drop-classifier-ref", image="options-edge-drop-classifier", var="DROP_CLASSIFIER_IMAGE", prefix="es.drop.ref"),
}


class Manifests(unittest.TestCase):
    def test_base_and_committed_slice_are_dev_single_replica_recreate_es_trades(self):
        for svc, c in CASES.items():
            for rel in (f"k8s/services/{svc}/base/{svc}-deployment.yaml", f"k8s/services/{svc}/overlays/dev/manifest.yaml"):
                d = deployment(rel)
                self.assertEqual(d["metadata"]["name"], c["dep"], rel)
                self.assertEqual(d["spec"]["replicas"], 1, rel)
                self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", rel)
                ct = d["spec"]["template"]["spec"]["containers"][0]
                self.assertEqual(ct["name"], c["container"], rel)
                self.assertIn(c["image"], ct["image"], rel)
                e = env_of(d)
                self.assertEqual(e["HEALTH_PORT"], "8080", rel)
                self.assertEqual(e["DROP_UNDERLYING_MODE"], "ES_TRADES", rel)
                for k in ("DROP_UNDERLYING_BOOTSTRAP", "DROP_FLOW_BOOTSTRAP", "DROP_OUTPUT_BOOTSTRAP", "DROP_GAMMA_BOOTSTRAP"):
                    self.assertEqual(e[k], "host.docker.internal:19092", f"{rel} {k}: the dev broker, never a production or es4 address")
                self.assertEqual(e.get("DROP_OUTPUT_PREFIX"), c["prefix"], rel)
                self.assertEqual(ct["livenessProbe"]["httpGet"]["path"], "/health/live", rel)
                self.assertEqual(ct["readinessProbe"]["httpGet"]["path"], "/health/ready", rel)

    def test_the_engines_mbo_environment_reads_the_dev_mirror_and_admits_only_the_live_literal(self):
        for rel in ("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml", "k8s/services/es-liquidity-engine/overlays/dev/manifest.yaml"):
            e = env_of(deployment(rel))
            self.assertEqual(e["ES_MBO_ENABLED"], "true", rel)
            self.assertEqual(e["ES_MBO_BOOTSTRAP"], "host.docker.internal:19092", rel)
            self.assertEqual(e["ES_MBO_OUTPUT_BOOTSTRAP"], "host.docker.internal:19092", rel)
            self.assertEqual(e["ES_MBO_ENVIRONMENT"], "Rithmic Paper Trading", rel)
            self.assertEqual(e["ES_MBO_ALLOW_TEST"], "false", rel)
            self.assertRegex(e["ES_MBO_CONTRACT_TABLE"], r"^\d{4}-\d{2}-\d{2}=ES[HMUZ]\d$", rel)
        self.assertNotIn("ES_MBO_ENABLED", env_of(deployment("k8s/services/drop-classifier-ref/base/drop-classifier-ref-deployment.yaml")))

    def test_the_events_topic_is_a_reset_preserved_uncompacted_log_capped_at_128_mib_and_dev_only(self):
        d = declarations(Topics.VARS + ["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(f"{EVENTS}:1", d["OPTIONS_EDGE_TOPICS"])
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertIn(EVENTS, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertIn(f"{EVENTS}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        self.assertIn(f"{EVENTS}={EVENTS_CAP}", d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])
        prod = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"], "production")
        for v, tokens in prod.items():
            self.assertNotIn(EVENTS, [x.split(":")[0].split("=")[0] for x in tokens], f"{v} names the events topic on production")

    def test_the_shared_reset_helper_preserves_the_events_log(self):
        def preserved(env):
            e = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
            e["ENVIRONMENT"] = env
            return subprocess.run(["bash", str(ROOT / "scripts/kafka/reset-preserved-topics.sh")], capture_output=True, text=True, env=e).stdout.split()
        self.assertIn(EVENTS, preserved("dev"))   # the helper parses the literal assignment, as it does for the wall's events log: production safety is the topics.env strip guard (tested above)
        text = read("scripts/kafka/topics.env")
        self.assertEqual(len(re.findall(r'^OPTIONS_EDGE_RESET_PRESERVED_TOPICS="', text, re.M)), 1, "the helper requires exactly ONE assignment")
        self.assertEqual(len(re.findall(r'^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="', text, re.M)), 1)
        self.assertIn(SNAPSHOT, re.search(r'^OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="([^"]*)"', text, re.M).group(1).split())

    def test_the_premarket_reset_spares_the_events_log_and_nothing_else_of_the_family(self):
        import re
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        self.assertIsNotNone(re.search(regex, EVENTS))
        for other in (LEVEL_FLOW, HEALTH, FINAL, BASE, SNAPSHOT):
            self.assertIsNone(re.search(regex, other), other)

    def test_the_vacuum_detector_runs_with_the_measured_descriptive_bounds(self):
        e = env_of(deployment("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml"))
        self.assertEqual((e["ES_MBO_VACUUM_DEPTH_TICKS"], e["ES_MBO_VACUUM_DEPTH_BELOW"], e["ES_MBO_VACUUM_RUN_AT_LEAST"]), ("10", "398", "0"))

    def test_the_engine_snapshots_every_sixty_buckets(self):
        self.assertEqual(env_of(deployment("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml"))["ES_MBO_SNAPSHOT_INTERVAL_BUCKETS"], "60")

    def test_the_final_stage_is_on_with_descriptive_ledger_values_only_for_the_engine(self):
        e = env_of(deployment("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml"))
        self.assertEqual((e["ES_MBO_FILL_WINDOW_MS"], e["ES_MBO_TRADE_ORDER_MS"], e["ES_MBO_TAPE_TOPIC"]), ("100", "2000", "underlying.es.trades"))
        self.assertNotIn("ES_MBO_FILL_WINDOW_MS", env_of(deployment("k8s/services/drop-classifier-ref/base/drop-classifier-ref-deployment.yaml")))

    def test_the_engine_attributes_fills_with_the_exact_matching_only_in_its_own_deployment(self):
        e = env_of(deployment("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml"))
        self.assertEqual(e["ES_MBO_FILL_MATCHING"], "optimal")
        self.assertNotIn("ES_MBO_FILL_MATCHING", env_of(deployment("k8s/services/drop-classifier-ref/base/drop-classifier-ref-deployment.yaml")))
        # the generated dev slice carries it too (mirror rule)
        self.assertIn("ES_MBO_FILL_MATCHING", open("k8s/services/es-liquidity-engine/overlays/dev/manifest.yaml").read())

    def test_the_tape_is_admitted_only_from_the_databento_trades_feed_for_the_front_contract(self):
        e = env_of(deployment("k8s/services/es-liquidity-engine/base/es-liquidity-engine-deployment.yaml"))
        self.assertEqual((e["ES_MBO_TAPE_SOURCE"], e["ES_MBO_TAPE_SOURCE_SCHEMA"], e["ES_MBO_INSTRUMENT_TABLE"]), ("DATABENTO", "trades", "ESZ6=10252"))
        # every contract of the contract table has an instrument row (a roll adds both rows together)
        contracts = {c.split("=")[1] for c in e["ES_MBO_CONTRACT_TABLE"].split(",")}
        instruments = {c.split("=")[0] for c in e["ES_MBO_INSTRUMENT_TABLE"].split(",")}
        self.assertEqual(contracts, instruments)
        self.assertNotIn("ES_MBO_TAPE_SOURCE", env_of(deployment("k8s/services/drop-classifier-ref/base/drop-classifier-ref-deployment.yaml")))
        self.assertIn("ES_MBO_INSTRUMENT_TABLE", open("k8s/services/es-liquidity-engine/overlays/dev/manifest.yaml").read())

    def test_the_two_deployments_never_share_an_output_prefix_or_name(self):
        engine, ref = (env_of(deployment(f"k8s/services/{s}/base/{s}-deployment.yaml")) for s in CASES)
        self.assertNotEqual(engine.get("DROP_OUTPUT_PREFIX"), ref.get("DROP_OUTPUT_PREFIX"))
        self.assertTrue(ref["DROP_OUTPUT_PREFIX"].startswith("es.drop."))

    def test_no_service_object_and_no_production_or_experiment_reference(self):
        for svc in CASES:
            self.assertFalse(any(d and d.get("kind") == "Service" for d in yaml.safe_load_all(read(f"k8s/services/{svc}/overlays/dev/manifest.yaml"))), svc)
            for env in ("production", "experiment", "es4"):
                self.assertFalse((ROOT / f"k8s/services/{svc}/overlays/{env}").exists(), f"{svc}: no {env} overlay")
                self.assertNotIn(f"services/{svc}/", read(f"k8s/overlays/{env}/kustomization.yaml") if (ROOT / f"k8s/overlays/{env}/kustomization.yaml").exists() else "", f"{svc} {env}")
        self.assertNotIn("es-liquidity-engine", read("k8s/base/kustomization.yaml"))


class Registration(unittest.TestCase):
    def test_services_yaml_declares_both_dev_only_standalone_slices(self):
        by = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        for svc, c in CASES.items():
            self.assertEqual(by[svc]["envs"], ["dev"], svc)
            self.assertEqual(by[svc]["managedBy"], "standalone")
            self.assertTrue(by[svc]["sliceGenerated"])
            self.assertEqual(by[svc]["deployments"], [c["dep"]])
            self.assertEqual(by[svc]["image"], c["image"])

    def test_dev_overlay_resource_image_remap_and_image_tag(self):
        k = read("k8s/overlays/dev/kustomization.yaml")
        for svc in CASES:
            self.assertIn(f"  - ../../services/{svc}/base\n", k)
        self.assertIn("newName: host.docker.internal:5001/options-edge-es-liquidity-engine", k)
        self.assertIn("es-liquidity-engine-service: host.docker.internal:5001/options-edge-es-liquidity-engine:dev", read("image-tags/dev.yaml"))

    def test_the_engine_image_variable_is_wired_in_every_deploy_script_for_dev_only(self):
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/resolve-images.sh", "scripts/deploy/verify-running-images.sh"):
            self.assertIn("ES_LIQUIDITY_ENGINE_IMAGE", read(rel), rel)
        self.assertIn('("drop-classifier-ref-service", "drop-classifier-ref"): "DROP_CLASSIFIER_IMAGE"', read("scripts/deploy/verify-running-images.sh"))

    def test_service_deploy_offers_both_services(self):
        j = read("Jenkinsfile.service-deploy")
        self.assertIn("      'es-liquidity-engine',\n", j)
        self.assertIn("      'drop-classifier-ref',\n", j)


def declarations(variables, environment=None):
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy, retention, bytes_="-1", parts=1, isr=1):
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_, "max.message.bytes": "1048576", "min.insync.replicas": str(isr)}}


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


def barrier_stage_body() -> str:
    """The body of the ONE Jenkinsfile stage that runs the topic barriers.

    Scoped, because two global searches of the file can each be satisfied somewhere unrelated — the
    service named in another list and the case arm in another stage — while the stage that matters
    omits it (Codex deploy r14).
    """
    j = read("Jenkinsfile.service-deploy")
    needle = "ensure-es-liquidity-engine-topics.sh ;;"
    i = j.index(needle)
    start = j.rindex("    stage(", 0, i)
    end = j.find("\n    stage(", i)
    body = j[start:end if end != -1 else len(j)]
    # SANITY ON THE EXTRACTION ITSELF, because it is lexical: a `stage(` inside a comment or a string
    # could make it select the wrong span, and an empty or truncated body would then satisfy a
    # "not in" assertion while proving nothing (Codex deploy r15). These are cheap and make a bad
    # extraction fail loudly instead of quietly.
    if len(body) < 200 or "steps {" not in body or needle not in body:
        raise AssertionError(
            "the barrier stage extraction looks wrong: %d chars, steps=%s, dispatch=%s"
            % (len(body), "steps {" in body, needle in body))
    if body.count("ensure-es-liquidity-engine-topics.sh ;;") != 1:
        raise AssertionError("the extracted span covers more than one dispatch — it is not one stage")
    return body


def right_topics():
    return {LEVEL_FLOW: shaped("delete", ONE_DAY, CAP), FINAL: shaped("delete", ONE_DAY, CAP), BASE: shaped("delete", ONE_DAY, CAP), HEALTH: shaped("delete", ONE_DAY), SNAPSHOT: shaped("compact", "-1", "-1"), EVENTS: shaped("delete", "-1", EVENTS_CAP), CLOSURES: shaped("compact", "-1", "-1"),
            # COVERAGE belongs in the already-correct set, or every drift case built from it hands
            # the barrier a cluster missing the topic and tests creation instead of drift.
            COVERAGE: shaped("compact", "-1", "-1")}


class Topics(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS",
            "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_ES4_TOPICS"]

    def test_every_topic_is_declared_one_partition_exact_with_the_declared_retention_and_bytes(self):
        d = declarations(self.VARS)
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        for t in (LEVEL_FLOW, HEALTH, FINAL, BASE):
            self.assertIn(f"{t}:1", d["OPTIONS_EDGE_TOPICS"])
            self.assertIn(t, d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"])
            self.assertEqual(retention[t], ONE_DAY)
            self.assertNotIn(t, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])
        self.assertEqual(bytes_.get(LEVEL_FLOW), CAP)
        self.assertEqual(bytes_.get(FINAL), CAP)
        self.assertEqual(bytes_.get(BASE), CAP)
        self.assertNotIn(HEALTH, bytes_)

    def test_the_closure_topic_is_pure_compact_and_rebuildable(self):
        d = declarations(Topics.VARS + ["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(f"{CLOSURES}:1", d["OPTIONS_EDGE_TOPICS"])
        self.assertIn(CLOSURES, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertIn(CLOSURES, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertNotIn(CLOSURES, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(f"{CLOSURES}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        prod = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], "production")
        for v, tokens in prod.items():
            self.assertNotIn(CLOSURES, [x.split(":")[0].split("=")[0] for x in tokens], v)

    def test_the_snapshot_topic_is_pure_compact_rebuildable_and_dev_only(self):
        d = declarations(self.VARS + ["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(f"{SNAPSHOT}:1", d["OPTIONS_EDGE_TOPICS"])
        self.assertIn(SNAPSHOT, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertIn(SNAPSHOT, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertNotIn(SNAPSHOT, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(f"{SNAPSHOT}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        self.assertIn(f"{SNAPSHOT}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])
        prod = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS"], "production")
        for v, tokens in prod.items():
            self.assertNotIn(SNAPSHOT, [x.split(":")[0].split("=")[0] for x in tokens], f"{v} names the snapshot topic on production")

    def test_dev_and_unset_declare_them_and_an_explicit_production_declares_neither(self):
        base = declarations(self.VARS)
        self.assertEqual(declarations(self.VARS, "dev"), base)
        prod = declarations(self.VARS, "production")
        for v in self.VARS:
            for t in (LEVEL_FLOW, HEALTH, FINAL, BASE):
                self.assertFalse(any(x == t or x.startswith(t + ":") or x.startswith(t + "=") for x in prod[v]), f"{v} declares {t} on production")


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_every_topic_at_its_shape_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        for t, policy, ret, b in ((LEVEL_FLOW, "delete", ONE_DAY, CAP), (FINAL, "delete", ONE_DAY, CAP), (BASE, "delete", ONE_DAY, CAP), (HEALTH, "delete", ONE_DAY, None), (EVENTS, "delete", "-1", EVENTS_CAP),
                                  (CLOSURES, "compact", "-1", "-1"), (SNAPSHOT, "compact", "-1", "-1"),
                                  # COVERAGE. It was absent from this matrix, so nothing here proved
                                  # the barrier creates it at its declared shape — and the test's own
                                  # name said "both topics" while it exercised seven (Codex deploy
                                  # r12). The shell suite covers it too; this is the harness that was
                                  # already here and should have been extended first.
                                  (COVERAGE, "compact", "-1", "-1")):
            d = state["topics"][t]
            self.assertEqual((d["partitions"], d["configs"]["cleanup.policy"], d["configs"]["retention.ms"]), (1, policy, ret), t)
            if b:
                self.assertEqual(d["configs"]["retention.bytes"], b)
        self.assertIn("=== ensure-es-liquidity-engine-topics OK ===", out)

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
            "level-flow retention one short": (LEVEL_FLOW, lambda d: d["configs"].update({"retention.ms": "86399999"})),
            "level-flow compacted": (LEVEL_FLOW, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "level-flow uncapped": (LEVEL_FLOW, lambda d: d["configs"].update({"retention.bytes": "-1"})),
            "level-flow another cap": (LEVEL_FLOW, lambda d: d["configs"].update({"retention.bytes": "134217728"})),
            "level-flow 4 partitions": (LEVEL_FLOW, lambda d: d.update(partitions=4)),
            "health retention 7 days": (HEALTH, lambda d: d["configs"].update({"retention.ms": "604800000"})),
            "health compacted": (HEALTH, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "health 32 partitions": (HEALTH, lambda d: d.update(partitions=32)),
            "snapshot delete": (SNAPSHOT, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "snapshot finite retention": (SNAPSHOT, lambda d: d["configs"].update({"retention.ms": ONE_DAY})),
            "snapshot byte-capped": (SNAPSHOT, lambda d: d["configs"].update({"retention.bytes": CAP})),
            # COVERAGE drift. A delete policy or a byte cap each defeat the interval's whole point:
            # the newest statement per contract must be the answer, and a cap ages out the oldest
            # contract's run so a reader finds nothing and reads it as "never observed".
            "coverage delete": (COVERAGE, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "coverage finite retention": (COVERAGE, lambda d: d["configs"].update({"retention.ms": ONE_DAY})),
            "coverage byte-capped": (COVERAGE, lambda d: d["configs"].update({"retention.bytes": CAP})),
            "coverage 4 partitions": (COVERAGE, lambda d: d.update(partitions=4)),
            "snapshot 4 partitions": (SNAPSHOT, lambda d: d.update(partitions=4)),
            "closures delete": (CLOSURES, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "closures finite retention": (CLOSURES, lambda d: d["configs"].update({"retention.ms": ONE_DAY})),
            "closures byte-capped": (CLOSURES, lambda d: d["configs"].update({"retention.bytes": CAP})),
            # A DISTINCT mutation. This key was a duplicate of the one above, and a duplicate dict
            # key is silently dropped by Python — so one declared drift case could never run while
            # the dict still read as covering it (Codex deploy r13).
            "snapshot 2 partitions": (SNAPSHOT, lambda d: d.update(partitions=2)),
            "closures 4 partitions": (CLOSURES, lambda d: d.update(partitions=4)),
            "events 4 partitions": (EVENTS, lambda d: d.update(partitions=4)),
            "events compacted": (EVENTS, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "events finite retention": (EVENTS, lambda d: d["configs"].update({"retention.ms": ONE_DAY})),
            "events uncapped": (EVENTS, lambda d: d["configs"].update({"retention.bytes": "-1"})),
            "events another cap": (EVENTS, lambda d: d["configs"].update({"retention.bytes": CAP})),
            "final uncapped": (FINAL, lambda d: d["configs"].update({"retention.bytes": "-1"})),
            "final compacted": (FINAL, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or "--alter" in c for c in calls), label)
            self.assertEqual(state["topics"][topic]["partitions"], existing[topic]["partitions"], label)

    def test_it_is_executable_sources_the_shared_routine_and_the_service_deploy_stage_runs_it(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())
        # THE SAME STAGE, not two global searches. The previous version asserted a
        # `.contains(params.SERVICE)` list anywhere in the file and the case arm anywhere else, so it
        # would pass with the service named in an UNRELATED list while the topic-barrier stage
        # omitted it (Codex deploy r14). Both assertions are now made against the body of the one
        # stage that runs this barrier.
        stage = barrier_stage_body()
        self.assertIn("es-liquidity-engine) scripts/kafka/ensure-es-liquidity-engine-topics.sh ;;", stage,
                      "the barrier stage must dispatch to this service's barrier")
        self.assertRegex(stage, r"\[[^\]]*'es-liquidity-engine'[^\]]*\]\.contains\(params\.SERVICE\)",
                         "and its own `when` must gate on a service list containing it — the"
                         " `.contains()` that replaced the equality chain when the file hit the"
                         " Groovy CPS 64 KB method limit")

    def test_the_barrier_has_a_mocked_negative_control_suite_and_the_gate_runs_it(self):
        # The barrier had NO mocked suite: the only coverage was this file asserting that the
        # Jenkinsfile mentions the script and the script mentions the shared routine, both of which
        # pass however wrong the shapes are. So es.mbo.coverage was asserted textually and no more
        # (Codex deploy r2). The suite now drives the real barrier against mocked CLIs, and this
        # requires it to exist and to be wired into the gate — a suite nothing runs is not coverage.
        suite = ROOT / "scripts/kafka/ensure-es-liquidity-engine-topics-test.sh"
        self.assertTrue(suite.is_file(), "the barrier's negative-control suite must exist")
        self.assertTrue(os.access(suite, os.X_OK))
        self.assertIn("bash scripts/kafka/ensure-es-liquidity-engine-topics-test.sh",
                      read(".github/workflows/deploy-validation.yml"))


class Schemas(unittest.TestCase):
    def test_the_map_names_exactly_the_fixed_ids_with_their_versions_and_no_other_map_uses_them(self):
        rows = [l.split("|") for l in read("scripts/kafka/schema-id-maps/es-liquidity-engine.map").splitlines() if l and not l.startswith("#")]
        self.assertEqual(sorted((r[0], int(r[1]), int(r[3]) if len(r) == 4 else 1) for r in rows),
                         sorted((f"{NS}.{n}", i, v) for n, vs in VERSIONS.items() for v, i in vs.items()))
        files = {(r[0], int(r[1])): r[2] for r in rows}
        for n in ("EsMboHealth", "EsMboLevelFlowFinal"):
            self.assertEqual(files[(f"{NS}.{n}", VERSIONS[n][1])], f"v1/{n}.avsc", "version 1 is the frozen archival file")
            self.assertEqual(files[(f"{NS}.{n}", VERSIONS[n][2])], f"{n}.avsc", "version 2 is the current file")
        order = [(r[0], int(r[3]) if len(r) == 4 else 1) for r in rows]
        for n in ("EsMboHealth", "EsMboLevelFlowFinal"):
            self.assertLess(order.index((f"{NS}.{n}", 1)), order.index((f"{NS}.{n}", 2)))
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other.name == "es-liquidity-engine.map":
                continue
            text = other.read_text()
            for vs in VERSIONS.values():
                for i in vs.values():
                    self.assertNotRegex(text, rf"\|{i}\|", f"{other.name} uses {i}")

    def test_the_manifest_governs_both_subjects_with_hashes_equal_to_the_snapshots_and_the_mbo_consumer_binding(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        by = {e["subject"]: e for e in m["entries"] if e.get("version") == 1}
        for n, vs in VERSIONS.items():
            for v, i in vs.items():
                e = next(x for x in m["entries"] if x["subject"] == f"{NS}.{n}" and x["version"] == v)
                self.assertEqual((e["id"], e["family"], e["environments"]), (i, "esmbo", ["dev"]))
                current = v == max(vs)
                self.assertEqual(e["producers"], [{"service": "es-liquidity-engine", "environments": ["dev"]}] if current else [], f"{n} v{v}: only the current version has a producer")
                snap = ROOT / "schema-manifest/schemas" / e["subject"] / f"v{v}.avsc"
                out = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
                self.assertEqual(out, e["schemaHash"], f"{n} v{v}")
                self.assertEqual(e["source"]["path"], f"schemas-registry/esmbo/{'' if current or len(vs) == 1 else 'v1/'}{n}.avsc", f"{n} v{v}")
        # line health (level-absorption) reads both versions of the final rows
        for v, i in VERSIONS["EsMboLevelFlowFinal"].items():
            e = next(x for x in m["entries"] if x["subject"] == f"{NS}.EsMboLevelFlowFinal" and x["version"] == v)
            self.assertEqual(e["consumers"], [{"service": "level-absorption", "accepts": [3506, 3510], "environments": ["dev"]}])
        self.assertEqual(m["familyBlocks"]["esmbo"], [[3501, 3510], [3511, 3511]])
        flow = by["com.optionsedge.contracts.deltaflow.DeltaFlowEvent"]
        self.assertIn({"service": "es-liquidity-engine", "accepts": [3007], "environments": ["dev"]}, flow["consumers"], "the drop engine inside the service reads the delta flow (dev id 3007)")
        mbo = by["es.rithmic.mbo-value"]
        self.assertIn({"service": "es-liquidity-engine", "accepts": [3302], "environments": ["dev"]}, mbo["consumers"])

    def test_registration_rides_the_schema_sync_gate_on_the_apply_path_not_a_stage_list(self):
        """service-deploy.sh gates every service the manifest names (producer or consumer) with scripts/ci/schema-sync.sh against the digest-pinned image's
        options-edge.schema-requirements label, before any workload change, and that gate registers the manifest's absent ids; es-liquidity-engine is named, so it is gated."""
        m = json.loads(read("schema-manifest/manifest.json"))
        named = {x["service"] for e in m["entries"] for x in (e.get("producers", []) + e.get("consumers", []))}
        self.assertIn("es-liquidity-engine", named)
        sh = read("scripts/deploy/service-deploy.sh")
        self.assertIn("bash scripts/ci/schema-sync.sh", sh)

        # EXECUTED, not matched. The replacement for the old stale assertion was textual too, so a
        # service-specific `EXEMPT:` bypass could have been introduced while every asserted string
        # survived (Codex deploy r14). The scope script is run.
        self.assertIn('bash scripts/ci/schema-sync-scope.sh "$SERVICE" "$ENVIRONMENT"', sh,
                      "the deploy must ask the scope script whether this service is gated")
        for service in ("es-liquidity-engine", "strike-hold"):
            r = subprocess.run(["bash", "scripts/ci/schema-sync-scope.sh", service, "dev"],
                               cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertEqual(r.stdout.strip(), "GATE",
                             f"{service} must be GATEd in dev, or its schemas are never registered")
        # And the ratchet's other arm really is reachable, so GATE above is a decision and not a
        # constant: a service the manifest names nowhere is reported as uncovered.
        r = subprocess.run(["bash", "scripts/ci/schema-sync-scope.sh", "no-such-service", "dev"],
                           cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.stdout.strip(), "NOT_GATED_UNCOVERED", r.stdout + r.stderr)
        # The GATE branch must be the one that runs schema-sync.sh, and the uncovered branch must
        # say so out loud rather than passing quietly.
        self.assertIn("NOT_GATED_UNCOVERED", sh)
        self.assertIn("es-liquidity-engine", named)

    def test_the_manifest_validates_with_the_governance_tool(self):
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()


class CoverageTopicDeclaration(unittest.TestCase):
    """THE COVERAGE MESSAGE's topic, as topics.env declares it and the barrier enforces it.

    It is PURE COMPACT and keyed by contract because the newest statement per contract is the whole
    answer; a reader wants the CURRENT run, and a retained history of every run would make the
    newest one something it has to search for. Asserted here because the shape is the difference
    between "the current interval" and "a log you must scan".
    """

    VARS = ("OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS",
            "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES",
            "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS")

    def test_declared_pure_compact_one_partition_on_dev_and_absent_on_production(self):
        dev = declarations(self.VARS, "dev")
        self.assertIn(COVERAGE + ":1", dev["OPTIONS_EDGE_TOPICS"],
                      "one partition, declared")
        self.assertIn(COVERAGE, dev["OPTIONS_EDGE_PURE_COMPACT_TOPICS"],
                      "pure compact: the newest statement per contract IS the content")
        self.assertIn(COVERAGE + "=-1", dev["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        self.assertIn(COVERAGE + "=-1", dev["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])
        self.assertIn(COVERAGE, dev["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"],
                      "REBUILDABLE: a missing coverage record makes a reader refuse to claim an "
                      "absence, which is the safe direction")
        prod = declarations(self.VARS, "production")
        for var, val in prod.items():
            self.assertNotIn(COVERAGE, val,
                             "dev only: production must declare nothing (%s)" % var)

    def test_the_barrier_creates_and_verifies_it(self):
        src = (ROOT / "scripts/kafka/ensure-es-liquidity-engine-topics.sh").read_text()
        self.assertIn("COVERAGE=", src, "the barrier must name the topic")
        self.assertIn('ensure_with_bytes "$COVERAGE" compact -1 -1', src,
                      "and enforce pure compact rather than leaving it to the broker default")
