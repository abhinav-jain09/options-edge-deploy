"""Deploy wiring of liquidity-validator-service (the live READ-ONLY liquidity validator).

DEV ONLY, single replica, strategy Recreate, health port 8080 (/healthz, /readyz). It reads five existing topics without a consumer group and publishes three single-partition Avro topics
(futures.liquidity-validator.{evaluations,data-gaps,daily}, ids 3601-3603). It never trades. Covers the base, the dev overlay slice, the registration everywhere a dev-only service must be,
the topics and their barrier, the schema map and manifest, and the absence of any production wiring."""
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
BARRIER = ROOT / "scripts/kafka/ensure-liquidity-validator-topics.sh"
SVC = "liquidity-validator"
EVALUATIONS, GAPS, DAILY = "futures.liquidity-validator.evaluations", "futures.liquidity-validator.data-gaps", "futures.liquidity-validator.daily"
TOPICS = (EVALUATIONS, GAPS, DAILY)
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
NS = "com.optionsedge.contracts.liquidityvalidator"
IDS = {"LiquidityValidatorEvaluation": 3601, "LiquidityValidatorDataGap": 3602, "LiquidityValidatorDaily": 3603}
BASE = f"k8s/services/{SVC}/base/{SVC}-deployment.yaml"
SLICE = f"k8s/services/{SVC}/overlays/dev/manifest.yaml"
INPUTS = {
    "LIQUIDITY_VALIDATOR_GEOMETRY_TOPIC": "options.spx.market-structure.geometry",
    "LIQUIDITY_VALIDATOR_EVENTS_TOPIC": "futures.level-absorption.events",
    "LIQUIDITY_VALIDATOR_AGGRESSOR_TOPIC": "es.futures.aggressor-flow",
    "LIQUIDITY_VALIDATOR_FOOTPRINT_TOPIC": "es.futures.footprint.bars",
    "LIQUIDITY_VALIDATOR_TAPE_TOPIC": "underlying.es.trades",
}


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def deployment(rel: str) -> dict:
    return next(d for d in yaml.safe_load_all(read(rel)) if d and d.get("kind") == "Deployment")


def env_of(dep: dict) -> dict:
    return {e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}


class Manifests(unittest.TestCase):
    def test_base_and_committed_slice_are_dev_single_replica_recreate_with_the_dev_guard_and_health_paths(self):
        for rel in (BASE, SLICE):
            d = deployment(rel)
            self.assertEqual(d["metadata"]["name"], "liquidity-validator-service", rel)
            self.assertEqual(d["spec"]["replicas"], 1, rel)
            self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", rel)
            ct = d["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(ct["name"], SVC, rel)
            self.assertIn("options-edge-liquidity-validator", ct["image"], rel)
            self.assertEqual(ct["livenessProbe"]["httpGet"]["path"], "/healthz", rel)
            self.assertEqual(ct["readinessProbe"]["httpGet"]["path"], "/readyz", rel)
            self.assertEqual(ct["resources"]["requests"], {"cpu": "100m", "memory": "512Mi"}, rel)
            self.assertEqual(ct["resources"]["limits"], {"memory": "1Gi"}, rel)
            e = env_of(d)
            self.assertEqual(e["LIQUIDITY_VALIDATOR_ENVIRONMENT"], "dev", rel)
            self.assertEqual(e["HEALTH_PORT"], "8080", rel)
            self.assertEqual(e["JAVA_TOOL_OPTIONS"], "-Xms256m -Xmx768m", rel)
            for k in ("KAFKA_BOOTSTRAP_SERVERS", "LIQUIDITY_VALIDATOR_DEV_BOOTSTRAP"):
                self.assertEqual(e[k], "host.docker.internal:19092", f"{rel} {k}: the dev broker, never a production or es4 address")
            self.assertEqual(e["KAFKA_BOOTSTRAP_SERVERS"], e["LIQUIDITY_VALIDATOR_DEV_BOOTSTRAP"], rel)

    def test_the_five_inputs_are_the_dev_topics_and_the_raw_tape_is_not_the_linearized_one(self):
        for rel in (BASE, SLICE):
            e = env_of(deployment(rel))
            for k, v in INPUTS.items():
                self.assertEqual(e[k], v, f"{rel} {k}")
            self.assertNotIn("linearized", e["LIQUIDITY_VALIDATOR_TAPE_TOPIC"])
            self.assertEqual((e["GRACE_MS"], e["LOOKBACK_MS"], e["PRINT_RING_MS"]), ("10000", "10800000", "120000"), rel)

    def test_no_output_topic_or_order_path_setting_is_configurable_from_the_deployment(self):
        """The three output topics are compiled in (Config), so a deploy cannot redirect the writer; nothing here names an order, broker or account setting."""
        e = env_of(deployment(BASE))
        for forbidden in ("EVALUATIONS", "DATA_GAPS", "DAILY", "ORDER", "ACCOUNT", "BROKER", "QUANTITY", "LIMIT"):
            self.assertFalse([k for k in e if forbidden in k.upper().replace("LIQUIDITY_VALIDATOR_DEV_BOOTSTRAP", "")], forbidden)

    def test_no_service_object_and_no_production_or_experiment_reference(self):
        self.assertFalse(any(d and d.get("kind") == "Service" for d in yaml.safe_load_all(read(SLICE))))
        for env in ("production", "experiment", "es4"):
            self.assertFalse((ROOT / f"k8s/services/{SVC}/overlays/{env}").exists(), f"no {env} overlay")
            p = ROOT / f"k8s/overlays/{env}/kustomization.yaml"
            self.assertNotIn(f"services/{SVC}/", p.read_text() if p.exists() else "", env)
        self.assertNotIn(SVC, read("k8s/base/kustomization.yaml"))

    def test_it_is_not_held_down_by_the_dev_cleanup_lists(self):
        cleanup = read("scripts/ops/dev-cleanup.sh")
        for name in ("DISABLED_DEV", "KEEP_DOWN"):
            m = re.search(rf"^{name}=\(?(.*?)\)?$", cleanup, re.M | re.S)
            self.assertTrue(m is None or "liquidity-validator" not in m.group(0), name)


class Registration(unittest.TestCase):
    def test_services_yaml_declares_the_dev_only_standalone_slice(self):
        by = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        s = by[SVC]
        self.assertEqual(s["envs"], ["dev"])
        self.assertEqual(s["managedBy"], "standalone")
        self.assertTrue(s["sliceGenerated"])
        self.assertEqual(s["deployments"], ["liquidity-validator-service"])
        self.assertEqual(s["image"], "options-edge-liquidity-validator")

    def test_dev_overlay_resource_image_remap_and_image_tag(self):
        k = read("k8s/overlays/dev/kustomization.yaml")
        self.assertIn(f"  - ../../services/{SVC}/base\n", k)
        self.assertIn("newName: host.docker.internal:5001/options-edge-liquidity-validator", k)
        self.assertIn("liquidity-validator-service: host.docker.internal:5001/options-edge-liquidity-validator:dev", read("image-tags/dev.yaml"))

    def test_the_image_variable_is_wired_in_every_deploy_script(self):
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/resolve-images.sh", "scripts/deploy/verify-running-images.sh"):
            self.assertIn("LIQUIDITY_VALIDATOR_IMAGE", read(rel), rel)
        self.assertIn('("liquidity-validator-service", "liquidity-validator"): "LIQUIDITY_VALIDATOR_IMAGE"', read("scripts/deploy/verify-running-images.sh"))

    def test_service_deploy_offers_it_and_runs_the_shared_barrier_stage(self):
        j = read("Jenkinsfile.service-deploy")
        self.assertIn("      'liquidity-validator',\n", j)
        self.assertIn("params.SERVICE == 'liquidity-validator') && !params.DEPLOY_DRY_RUN", j)
        self.assertIn("liquidity-validator) scripts/kafka/ensure-liquidity-validator-topics.sh ;;", j)
        self.assertEqual(j.count("Verify liquidity-validator topics"), 0, "no separate stage: the pipeline's CPS method is at the JVM limit")


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
    return {t: shaped("compact", "-1", "-1") for t in TOPICS}


class Topics(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS",
            "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_ES4_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"]

    def test_the_three_topics_are_declared_one_partition_exact_pure_compact_with_no_byte_cap(self):
        d = declarations(self.VARS)
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        for t in TOPICS:
            self.assertIn(f"{t}:1", d["OPTIONS_EDGE_TOPICS"])
            self.assertIn(t, d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"])
            self.assertIn(t, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], t)
            self.assertEqual((retention[t], bytes_[t]), ("-1", "-1"), t)
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])

    def test_every_retention_minus_one_topic_has_exactly_one_reset_class_and_the_premarket_reset_spares_all_three(self):
        d = declarations(self.VARS)
        for t in TOPICS:
            self.assertIn(t, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
            self.assertNotIn(t, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        for t in TOPICS:
            self.assertIsNotNone(re.search(regex, t), f"the pre-market reset must spare {t}")
        self.assertIsNone(re.search(regex, "futures.liquidity-validator.other"))

    def test_dev_and_unset_declare_them_and_every_other_environment_declares_none(self):
        base = declarations(self.VARS)
        self.assertEqual(declarations(self.VARS, "dev"), base)
        for environment in ("production", "experiment", "es4", "staging"):
            other = declarations(self.VARS, environment)
            for v in self.VARS:
                for t in TOPICS:
                    self.assertFalse(any(x == t or x.startswith(t + ":") or x.startswith(t + "=") for x in other[v]), f"{v} declares {t} on {environment}")
            self.assertFalse([x for v in self.VARS for x in other[v] if "liquidity-validator" in x], environment)


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_the_three_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        for t in TOPICS:
            d = state["topics"][t]
            self.assertEqual((d["partitions"], d["configs"]["cleanup.policy"], d["configs"]["retention.ms"], d["configs"]["retention.bytes"]), (1, "compact", "-1", "-1"), t)
        self.assertIn("=== ensure-liquidity-validator-topics OK ===", out)

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
        cases = {}
        for name, t in (("evaluations", EVALUATIONS), ("gaps", GAPS), ("daily", DAILY)):
            cases[f"{name} delete policy"] = (t, lambda d: d["configs"].update({"cleanup.policy": "delete"}))
            cases[f"{name} finite retention"] = (t, lambda d: d["configs"].update({"retention.ms": "86400000"}))
            cases[f"{name} byte-capped"] = (t, lambda d: d["configs"].update({"retention.bytes": "268435456"}))
            cases[f"{name} 4 partitions"] = (t, lambda d: d.update(partitions=4))
            cases[f"{name} compact,delete"] = (t, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"}))
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


class Schemas(unittest.TestCase):
    def test_the_map_names_exactly_the_three_fixed_ids_and_no_other_map_uses_them(self):
        rows = [l.split("|") for l in read("scripts/kafka/schema-id-maps/liquidity-validator.map").splitlines() if l and not l.startswith("#")]
        self.assertEqual({r[0]: int(r[1]) for r in rows}, {f"{NS}.{n}": i for n, i in IDS.items()})
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other.name == "liquidity-validator.map":
                continue
            text = other.read_text()
            for i in IDS.values():
                self.assertNotRegex(text, rf"\|{i}\|", f"{other.name} uses {i}")

    def test_the_manifest_governs_the_three_subjects_with_hashes_equal_to_the_snapshots_and_the_five_input_consumer_bindings(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        by = {e["subject"]: e for e in m["entries"] if e.get("kind") == "MIRRORED"}
        for n, i in IDS.items():
            e = by[f"{NS}.{n}"]
            self.assertEqual((e["id"], e["version"], e["family"], e["environments"]), (i, 1, "liquidityvalidator", ["dev"]))
            self.assertEqual(e["producers"], [{"service": SVC, "environments": ["dev"]}])
            self.assertEqual(e["consumers"], [], "this deployment adds no decision consumer of the research topics (a requirement constraint, not proved here)")
            snap = ROOT / "schema-manifest/schemas" / e["subject"] / "v1.avsc"
            out = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
            self.assertEqual(out, e["schemaHash"], n)
        self.assertEqual(m["familyBlocks"]["liquidityvalidator"], [[3601, 3603]])
        wanted = {2223: "GeometryAnnotationSet", 2702: "futures.level-absorption.events-value", 2701: "EsAggressorFlowSnapshot", 2034: "es.futures.footprint.bars-value"}
        bound = {e["id"]: e for e in m["entries"] if e.get("kind") == "MIRRORED" and e.get("id") in wanted and wanted[e["id"]] in e["subject"]}
        self.assertEqual(sorted(bound), sorted(wanted))
        for i, e in bound.items():
            self.assertIn({"service": SVC, "accepts": [i], "environments": ["dev"]}, e["consumers"], f"the validator reads id {i}")

    def test_registration_rides_the_schema_sync_gate_on_the_apply_path_not_a_stage_list(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        named = {x["service"] for e in m["entries"] for x in (e.get("producers", []) + e.get("consumers", []))}
        self.assertIn(SVC, named)
        sh = read("scripts/deploy/service-deploy.sh")
        self.assertIn("bash scripts/ci/schema-sync.sh", sh)

    def test_the_manifest_validates_with_the_governance_tool(self):
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
