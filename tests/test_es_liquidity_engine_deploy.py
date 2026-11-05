"""Deploy wiring of es-liquidity-engine-service (the ES MBO liquidity engine) and the TEMPORARY drop-classifier-ref reference deployment (the G3b parity gate's reference side).

Both are DEV ONLY, single replica, strategy Recreate, health port 8080, drop engine in ES_TRADES mode on the dev broker. The engine writes the default es.drop.* prefix; the reference writes
es.drop.ref.* so the two never share an output topic. Covers the base, the dev overlay slice, the registration everywhere a dev-only service must be, and the absence of any production wiring."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_kafka_cli.py"
BARRIER = ROOT / "scripts/kafka/ensure-es-liquidity-engine-topics.sh"
LEVEL_FLOW, HEALTH = "es.mbo.level-flow", "es.mbo.health"
ONE_DAY, CAP = "86400000", "268435456"
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
NS = "com.optionsedge.contracts.esmbo"
IDS = {"EsMboLevelFlow": 3502, "EsMboHealth": 3504}


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


def right_topics():
    return {LEVEL_FLOW: shaped("delete", ONE_DAY, CAP), HEALTH: shaped("delete", ONE_DAY)}


class Topics(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS",
            "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_ES4_TOPICS"]

    def test_the_two_topics_are_declared_one_partition_exact_with_the_declared_retention_and_bytes(self):
        d = declarations(self.VARS)
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        for t in (LEVEL_FLOW, HEALTH):
            self.assertIn(f"{t}:1", d["OPTIONS_EDGE_TOPICS"])
            self.assertIn(t, d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"])
            self.assertEqual(retention[t], ONE_DAY)
            self.assertNotIn(t, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])
        self.assertEqual(bytes_.get(LEVEL_FLOW), CAP)
        self.assertNotIn(HEALTH, bytes_)

    def test_dev_and_unset_declare_them_and_an_explicit_production_declares_neither(self):
        base = declarations(self.VARS)
        self.assertEqual(declarations(self.VARS, "dev"), base)
        prod = declarations(self.VARS, "production")
        for v in self.VARS:
            for t in (LEVEL_FLOW, HEALTH):
                self.assertFalse(any(x == t or x.startswith(t + ":") or x.startswith(t + "=") for x in prod[v]), f"{v} declares {t} on production")


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_both_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        for t, ret, b in ((LEVEL_FLOW, ONE_DAY, CAP), (HEALTH, ONE_DAY, None)):
            d = state["topics"][t]
            self.assertEqual((d["partitions"], d["configs"]["cleanup.policy"], d["configs"]["retention.ms"]), (1, "delete", ret), t)
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
        j = read("Jenkinsfile.service-deploy")
        self.assertIn("params.SERVICE == 'es-liquidity-engine'", j)
        self.assertIn("es-liquidity-engine) scripts/kafka/ensure-es-liquidity-engine-topics.sh ;;", j)


class Schemas(unittest.TestCase):
    def test_the_map_names_exactly_the_two_fixed_ids_and_no_other_map_uses_them(self):
        rows = [l.split("|") for l in read("scripts/kafka/schema-id-maps/es-liquidity-engine.map").splitlines() if l and not l.startswith("#")]
        self.assertEqual({r[0]: int(r[1]) for r in rows}, {f"{NS}.{n}": i for n, i in IDS.items()})
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other.name == "es-liquidity-engine.map":
                continue
            text = other.read_text()
            for i in IDS.values():
                self.assertNotRegex(text, rf"\|{i}\|", f"{other.name} uses {i}")

    def test_the_manifest_governs_both_subjects_with_hashes_equal_to_the_snapshots_and_the_mbo_consumer_binding(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        by = {e["subject"]: e for e in m["entries"]}
        for n, i in IDS.items():
            e = by[f"{NS}.{n}"]
            self.assertEqual((e["id"], e["version"], e["family"], e["environments"]), (i, 1, "esmbo", ["dev"]))
            self.assertEqual(e["producers"], [{"service": "es-liquidity-engine", "environments": ["dev"]}])
            snap = ROOT / "schema-manifest/schemas" / e["subject"] / "v1.avsc"
            out = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
            self.assertEqual(out, e["schemaHash"], n)
        self.assertEqual(m["familyBlocks"]["esmbo"], [[3502, 3502], [3504, 3504]])
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
        self.assertRegex(sh, r'select\(\.service == \\"\$SERVICE\\"\)')

    def test_the_manifest_validates_with_the_governance_tool(self):
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
