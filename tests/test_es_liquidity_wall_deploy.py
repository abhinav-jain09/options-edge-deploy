# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""Deploy wiring of es-liquidity-wall-service (ES book liquidity walls, a NEW Kafka Streams service): DEV ONLY, replicas 1, ES_LIQUIDITY_WALL_ENABLED "true", Streams state on an emptyDir.

Covers: the base and the dev overlay (source render, committed slice) at replicas 1 / flag "true" / the declared env / health port 8080 / the emptyDir state mount, the seven topics' declared shapes
in topics.env (reset classes, the events byte cap, the four pure-compact topics at -1/-1), the topic barrier (scripts/kafka/ensure-es-liquidity-wall-topics.sh) against the stateful stand-in
tests/fake_kafka_cli.py, the fixed-id schema map (3401-3408: the dashboard has version 1 under 3403 and version 2 under 3408; shared with no other map), the schema-manifest entries (family block, hashes equal to the snapshots, the depth consumer binding),
the registration of the service everywhere a dev-only service must be, and the absence of any production wiring."""
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
BARRIER = ROOT / "scripts/kafka/ensure-es-liquidity-wall-topics.sh"
SVC, DEP, CONTAINER, IMG, VAR = "es-liquidity-wall", "es-liquidity-wall-service", "es-liquidity-wall", "options-edge-es-liquidity-wall", "ES_LIQUIDITY_WALL_IMAGE"
P = "futures.liquidity-wall"
CLOSURES, BUCKETS, DASHBOARD, EVENTS, STATE, SNAPSHOT, VOICE = (f"{P}.{s}" for s in ("closures", "buckets", "dashboard", "events", "state", "snapshot", "voice"))
TOPICS = (CLOSURES, BUCKETS, DASHBOARD, EVENTS, STATE, SNAPSHOT, VOICE)
PURE = (DASHBOARD, STATE, SNAPSHOT, VOICE)
SEVEN_DAYS, ONE_DAY, CAP = "604800000", "86400000", "268435456"
# topic -> (cleanup.policy, retention.ms, retention.bytes the declaration carries or None)
SHAPES = {CLOSURES: ("delete", SEVEN_DAYS, None), BUCKETS: ("delete", ONE_DAY, None), DASHBOARD: ("compact", "-1", "-1"), EVENTS: ("delete", "-1", CAP),
          STATE: ("compact", "-1", "-1"), SNAPSHOT: ("compact", "-1", "-1"), VOICE: ("compact", "-1", "-1")}
NS = "com.optionsedge.contracts.liquiditywall"
IDS = {"LiquidityWallClosure": 3401, "LiquidityWallBucket": 3402, "LiquidityWallDashboard": 3408, "LiquidityWallEvent": 3404, "LiquidityWallState": 3405,
       "LiquidityWallSnapshot": 3406, "LiquidityWallVoice": 3407}
TOPIC_OF = dict(zip(IDS, TOPICS))
DASHBOARD_V1_ID, DASHBOARD_V2_ID = 3403, 3408
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def declarations(variables: list[str], environment: str | None = None) -> dict[str, list[str]]:
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy: str, retention: str, bytes_: str = "-1", parts: int = 1, max_bytes: int = 1048576, isr: int = 1) -> dict:
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_, "max.message.bytes": str(max_bytes), "min.insync.replicas": str(isr)}}


def right_topics() -> dict:
    return {t: shaped(p, r, b or "-1") for t, (p, r, b) in SHAPES.items()}


def run_barrier(topics: dict, environment: str | None = "dev"):
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


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_the_seven_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c or "message.max.bytes" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        for t, (policy, ret, bytes_) in SHAPES.items():
            d = state["topics"][t]
            self.assertEqual((d["partitions"], d["configs"]["cleanup.policy"], d["configs"]["retention.ms"]), (1, policy, ret), t)
            if bytes_ is not None:
                self.assertEqual(d["configs"]["retention.bytes"], bytes_, f"{t}: created WITH its declared retention.bytes")
        self.assertIn("=== ensure-es-liquidity-wall-topics OK ===", out)

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
        self.assertFalse(any("--create" in c or "--delete" in c or "--alter" in c for c in calls))

    def test_every_difference_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {
            "closures retention one short": (CLOSURES, lambda d: d["configs"].update({"retention.ms": "604799999"})),
            "closures compacted": (CLOSURES, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "buckets at 7 days": (BUCKETS, lambda d: d["configs"].update({"retention.ms": SEVEN_DAYS})),
            "dashboard delete": (DASHBOARD, lambda d: d["configs"].update({"cleanup.policy": "delete"})),
            "dashboard compact,delete": (DASHBOARD, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"})),
            "dashboard byte-capped (byte-based deletion of a pure-compact topic)": (DASHBOARD, lambda d: d["configs"].update({"retention.bytes": CAP})),
            "events COMPACTED (every event must survive until the cap)": (EVENTS, lambda d: d["configs"].update({"cleanup.policy": "compact"})),
            "events finite retention": (EVENTS, lambda d: d["configs"].update({"retention.ms": SEVEN_DAYS})),
            "events unbounded (no cap)": (EVENTS, lambda d: d["configs"].update({"retention.bytes": "-1"})),
            "events with another cap": (EVENTS, lambda d: d["configs"].update({"retention.bytes": "134217728"})),
            "state 32 partitions": (STATE, lambda d: d.update(partitions=32)),
            "snapshot finite retention": (SNAPSHOT, lambda d: d["configs"].update({"retention.ms": SEVEN_DAYS})),
            "voice min.insync.replicas below the minimum": (VOICE, lambda d: d["configs"].update({"min.insync.replicas": "0"})),
        }
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or "--alter" in c for c in calls), f"{label}: a barrier never alters or recreates a topic")
            self.assertEqual(state["topics"][topic]["partitions"], existing[topic]["partitions"], label)

    def test_the_barrier_is_executable_sources_the_shared_routine_and_is_in_the_ci_loop(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())
        test = ROOT / "scripts/kafka/ensure-es-liquidity-wall-topics-test.sh"
        self.assertTrue(os.access(test, os.X_OK))
        self.assertIn("scripts/kafka/ensure-es-liquidity-wall-topics-test.sh", read("scripts/ci/validate-services.sh"))


class TopicsEnv(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES",
            "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS",
            "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_ES4_TOPICS"]

    @classmethod
    def setUpClass(cls):
        cls.d = declarations(cls.VARS)

    def test_the_seven_topics_are_declared_one_partition_exact_with_the_declared_policy_retention_and_bytes(self):
        retention = dict(x.split("=", 1) for x in self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in self.d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        for t, (policy, ret, b) in SHAPES.items():
            self.assertIn(f"{t}:1", self.d["OPTIONS_EDGE_TOPICS"], t)
            self.assertIn(t, self.d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
            self.assertEqual(retention[t], ret, t)
            self.assertEqual(bytes_.get(t), b, t)
            self.assertEqual(t in self.d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], policy == "compact", t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_ES4_TOPICS"], t)
            self.assertNotRegex(t, r"\.v\d+$")
        self.assertEqual(len([x for x in self.d["OPTIONS_EDGE_TOPICS"] if x.startswith(P)]), 7)

    def test_every_retention_minus_one_topic_has_exactly_one_reset_class(self):
        self.assertIn(EVENTS, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertNotIn(EVENTS, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        for t in PURE:
            self.assertIn(t, self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
        for t in (CLOSURES, BUCKETS):
            self.assertNotIn(t, self.d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"] + self.d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        self.assertIsNotNone(re.search(regex, EVENTS), "the pre-market reset must spare the events log")
        for t in TOPICS:
            if t != EVENTS:
                self.assertIsNone(re.search(regex, t), t)

    def test_an_explicit_production_source_declares_nothing_about_the_seven_topics_anywhere(self):
        env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
        env["ENVIRONMENT"] = "production"
        script = (f'source "{ROOT}/scripts/kafka/topics.env"; for v in $(compgen -v OPTIONS_EDGE_); do case "${{!v}}" in *liquidity-wall*) echo "$v";; esac; done')
        out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
        self.assertEqual(out.split(), [])

    def test_dev_and_unset_declare_the_same_seven_shapes_and_production_loses_exactly_them(self):
        for environment in ("dev", None):
            got = declarations(self.VARS, environment)
            self.assertEqual(got, self.d, environment)
        prod = declarations(self.VARS, "production")
        ours = lambda tokens: sorted(x for x in tokens if x.startswith(P))                                                      # noqa: E731
        others = lambda tokens: sorted(x for x in tokens if not x.startswith(P) and not x.startswith("options.spx.line-acceptance") and not x.startswith("es.mbo.") and not x.startswith("futures.liquidity-validator"))  # noqa: E731
        for v in self.VARS:
            self.assertEqual(ours(prod[v]), [], v)
            # production keeps every other declaration (line-acceptance's own dev-only guard strips its six the same way; that is its test's business)
            self.assertEqual(others(prod[v]), others(self.d[v]), v)
        self.assertEqual(len(ours(self.d["OPTIONS_EDGE_TOPICS"])), 7)
        self.assertEqual(len(ours(self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])), 7)
        self.assertEqual(len(ours(self.d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])), 5)


def deployment(rel: str) -> dict:
    docs = [d for d in yaml.safe_load_all(read(rel)) if d]
    assert [d["kind"] for d in docs] == ["Deployment"], rel
    return docs[0]


ENV = {"ES_LIQUIDITY_WALL_ENABLED": "true", "HEALTH_PORT": "8080", "KAFKA_STREAMS_STATE_DIR": "/var/lib/options-edge/kafka-streams",
       "ES_LIQUIDITY_WALL_STREAMS_APP_ID": "options-edge-es-liquidity-wall", "ES_LIQUIDITY_WALL_STREAMS_AUTO_OFFSET_RESET": "latest",
       "ES_LIQUIDITY_WALL_DEPTH_TOPIC": "es.rithmic.depth", "ES_LIQUIDITY_WALL_DEPTH_ENVIRONMENT": "Rithmic Paper Trading", "ES_LIQUIDITY_WALL_DEPTH_ALLOW_TEST": "false",
       "ES_LIQUIDITY_WALL_LIVE_ENVIRONMENT": "Rithmic Paper Trading", "ES_LIQUIDITY_WALL_CONTRACT_TABLE": "2026-09-14=ESZ6", "ES_LIQUIDITY_WALL_WALL_MIN_FLOOR": "127",
       "ES_LIQUIDITY_WALL_WALL_MIN_SIZE": "127", "ES_LIQUIDITY_WALL_LOW_SIZE": "58", "ES_LIQUIDITY_WALL_WALL_REL_K": "1.460", "ES_LIQUIDITY_WALL_FLIP_LEAN_THRESHOLD": "0.100", "ES_LIQUIDITY_WALL_BUCKET_GRACE_MS": "10000",
       "JAVA_TOOL_OPTIONS": "-Xms256m -Xmx1024m"}


class LiveOnDev(unittest.TestCase):
    BASE = f"k8s/services/{SVC}/base/es-liquidity-wall-deployment.yaml"
    SLICE = f"k8s/services/{SVC}/overlays/dev/manifest.yaml"

    def check(self, d: dict, label: str):
        self.assertEqual((d["metadata"]["name"], d["spec"]["replicas"], d["spec"]["strategy"]["type"]), (DEP, 1, "Recreate"), label)
        spec = d["spec"]["template"]["spec"]
        c = spec["containers"][0]
        self.assertEqual(len(spec["containers"]), 1, label)
        self.assertEqual(c["name"], CONTAINER, label)
        self.assertIn(IMG, c["image"], label)
        env = {e["name"]: e.get("value") for e in c["env"]}
        # ES_LIQUIDITY_WALL_LIVE_ENVIRONMENT (Layer B, the live-depth admission literal) is set per the document; the service refuses a blank one, so only its presence is pinned here
        self.assertIn("ES_LIQUIDITY_WALL_LIVE_ENVIRONMENT", env, label)
        self.assertEqual({k: v for k, v in env.items() if k != "ES_LIQUIDITY_WALL_LIVE_ENVIRONMENT"}, {k: v for k, v in ENV.items() if k != "ES_LIQUIDITY_WALL_LIVE_ENVIRONMENT"}, label)
        self.assertEqual(sorted(r["name"] for r in [x["configMapRef"] for x in c["envFrom"] if "configMapRef" in x] + [x["secretRef"] for x in c["envFrom"] if "secretRef" in x]),
                         ["options-edge-config", "options-edge-runtime-secrets"], label)
        self.assertEqual([p["containerPort"] for p in c["ports"]], [8080], label)
        self.assertEqual((c["readinessProbe"]["httpGet"]["path"], c["livenessProbe"]["httpGet"]["path"]), ("/health/ready", "/health/live"), label)
        self.assertEqual((c["resources"]["requests"], c["resources"]["limits"]), ({"cpu": "200m", "memory": "768Mi"}, {"memory": "1536Mi"}), label)
        self.assertEqual(c["volumeMounts"], [{"mountPath": "/var/lib/options-edge/kafka-streams", "name": "kafka-streams-state"}], label)
        self.assertEqual(spec["volumes"], [{"name": "kafka-streams-state", "emptyDir": {}}], f"{label}: Streams state on an emptyDir (as zn-gex), no PVC")

    def test_the_base_is_live(self):
        self.check(deployment(self.BASE), "base")

    def test_the_dev_slice_is_live(self):
        self.check(deployment(self.SLICE), "dev slice")

    def test_the_rendered_dev_overlay_equals_the_committed_slice(self):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/dev")], capture_output=True, text=True, check=True, timeout=180).stdout
        docs = [d for d in yaml.safe_load_all(out) if isinstance(d, dict) and d.get("kind") == "Deployment" and d["metadata"]["name"] == DEP]
        self.assertEqual(len(docs), 1)
        self.check(docs[0], "dev render")
        self.assertEqual(docs[0], deployment(self.SLICE))
        self.assertEqual(docs[0]["spec"]["template"]["spec"]["containers"][0]["image"], f"host.docker.internal:5001/{IMG}:dev")

    def test_production_and_experiment_never_render_it(self):
        for env in ("production", "experiment"):
            out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays" / env)], capture_output=True, text=True, check=True, timeout=180).stdout
            self.assertNotIn(DEP, out, env)
            self.assertNotIn("ES_LIQUIDITY_WALL", out, env)

    def test_the_flag_is_true_only_in_the_dev_only_base_and_slice(self):
        for path in list((ROOT / "k8s").rglob("*.yaml")):
            hits = re.findall(r"name: ES_LIQUIDITY_WALL_ENABLED\n\s+value: (\S+)", path.read_text())
            self.assertEqual(bool(hits), str(path.relative_to(ROOT)) in (self.BASE, self.SLICE), str(path.relative_to(ROOT)))


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
            self.assertIn('"${ENVIRONMENT:-dev}" = "dev"', text[max(0, i - 900):i], f"{rel}: only emitted for dev")

    def test_the_dev_overlay_references_it_and_maps_the_image(self):
        text = read("k8s/overlays/dev/kustomization.yaml")
        self.assertIn(f"../../services/{SVC}/base", text)
        self.assertRegex(text, rf"- name: 192.168.100.252:5000/{IMG}\n    newName: host.docker.internal:5001/{IMG}\n    newTag: dev\n")
        self.assertNotRegex(text, rf"path: {SVC}-.*patch", "no enable patch exists")
        self.assertIn(f"{DEP}: host.docker.internal:5001/{IMG}:dev", read("image-tags/dev.yaml"))

    def test_the_service_deploy_job_offers_it_and_runs_its_dev_only_barrier_before_the_rollout(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertIn(f"'{SVC}',", text)
        # the barrier shares the line-acceptance stage (the pipeline's CPS method is at the JVM's 64 KB limit; a separate stage failed to compile, build #3006)
        stage = text[text.index("stage('Verify line-acceptance topics')"):text.index("stage('Verify market-structure output topics')")]
        self.assertIn("(params.SERVICE == 'line-acceptance' || params.SERVICE == 'es-liquidity-wall' || params.SERVICE == 'es-liquidity-engine' || params.SERVICE == 'liquidity-validator') && !params.DEPLOY_DRY_RUN", stage)
        self.assertIn("params.ENVIRONMENT != 'dev'", stage)
        self.assertIn("scripts/kafka/ensure-es-liquidity-wall-topics.sh", stage)
        self.assertIn("params.SERVICE == 'es-liquidity-wall'", stage)
        self.assertLess(text.index("stage('Verify line-acceptance topics')"), text.index("stage('Deploy (service-scoped)')"), "the barrier precedes the rollout")


class SchemaMapAndManifest(unittest.TestCase):
    MAP = ROOT / "scripts/kafka/schema-id-maps/es-liquidity-wall.map"

    def rows(self):
        return [tuple(l.split("|")) for l in self.MAP.read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")]

    def test_the_map_is_the_seven_records_and_ids_with_the_dashboard_in_two_versions(self):
        rows = self.rows()
        dash = f"{NS}.LiquidityWallDashboard"
        # version 1 rows (the frozen archival file for 3403), then the version-2 row after all of them (the line-health.map format)
        self.assertEqual([r for r in rows if len(r) == 3], [(f"{NS}.{n}", str(i), f"{n}.avsc") for n, i in IDS.items() if n != "LiquidityWallDashboard"])
        self.assertEqual([r for r in rows if len(r) == 4],
                         [(dash, str(DASHBOARD_V1_ID), "v1/LiquidityWallDashboard.avsc", "1"), (dash, str(DASHBOARD_V2_ID), "LiquidityWallDashboard.avsc", "2")])
        self.assertEqual(rows[-1], (dash, str(DASHBOARD_V2_ID), "LiquidityWallDashboard.avsc", "2"), "the version-2 row follows every version-1 row")
        self.assertEqual(rows.index((dash, "3403", "v1/LiquidityWallDashboard.avsc", "1")) < rows.index(rows[-1]), True)
        text = self.MAP.read_text()
        self.assertIn("Ids 3401-3408 are the ES liquidity wall block", text)
        self.assertIn("REGISTER 3408 BEFORE the wall service image", text)
        self.assertIn("LiquidityWallDashboard (3408; 3403 archival version 1)", read("scripts/kafka/topics.env"))

    def test_no_id_is_shared_with_any_other_map(self):
        ours = {int(r[1]) for r in self.rows()}
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other == self.MAP:
                continue
            for line in other.read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    self.assertNotIn(int(line.split("|")[1]), ours, f"{other.name} reuses an es-liquidity-wall id")

    def test_the_manifest_carries_the_family_block_the_seven_dev_entries_and_the_depth_consumer_binding(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        self.assertEqual(m["familyBlocks"]["liquiditywall"], [[3401, 3408]])
        for fam, ranges in m["familyBlocks"].items():
            if fam != "liquiditywall":
                for lo, hi in ranges:
                    self.assertTrue(hi < 3401 or lo > 3408, f"{fam} overlaps the liquiditywall block")
        by_subject = {e["subject"]: e for e in m["entries"] if e.get("kind") == "MIRRORED" and e.get("version", 1) == 1}
        dash_v1 = by_subject[f"{NS}.LiquidityWallDashboard"]
        dash_v2 = next(e for e in m["entries"] if e.get("id") == DASHBOARD_V2_ID)
        gateway = [{"service": "feed-gateway", "accepts": [DASHBOARD_V1_ID, DASHBOARD_V2_ID], "environments": ["dev"]}]
        for name, sid in IDS.items():
            e = by_subject[f"{NS}.{name}"] if name != "LiquidityWallDashboard" else dash_v2
            version_one = name != "LiquidityWallDashboard"
            self.assertEqual((e["version"], e["id"], e["family"], e["environments"]), (1 if version_one else 2, sid, "liquiditywall", ["dev"]), name)
            self.assertEqual(e["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/liquiditywall/{name}.avsc"}, name)
            self.assertEqual(e["topics"], [{"name": TOPIC_OF[name], "kind": "data"}], name)
            self.assertEqual(e["producers"], [{"service": SVC, "environments": ["dev"]}], name)
            # the dashboard is read by the feed gateway (both ids); the other entries' consumer bindings (es-liquidity-engine reads events and state) belong to other changes
            if name == "LiquidityWallDashboard":
                self.assertEqual(e["consumers"], gateway, name)
            self.assertNotIn("pending", e, name)
            self.assertNotIn("baseline", e, name)
            snapshot = ROOT / "schema-manifest/schemas" / e["schemaFile"]
            self.assertTrue(snapshot.is_file(), name)
            text = snapshot.read_text()
            self.assertEqual(text, json.dumps(json.loads(text), separators=(",", ":"), ensure_ascii=False), f"{name}: one-line compact JSON, no trailing newline")
            self.assertEqual(json.loads(text)["name"], name)
        # the archival version 1 stays registered on dev under 3403: frozen file, superseded, nothing writes it, the gateway still reads it
        self.assertEqual((dash_v1["version"], dash_v1["id"], dash_v1["schemaFile"]), (1, DASHBOARD_V1_ID, f"{NS}.LiquidityWallDashboard/v1.avsc"))
        self.assertEqual(dash_v1["source"]["path"], "schemas-registry/liquiditywall/v1/LiquidityWallDashboard.avsc")
        self.assertEqual((dash_v1["producers"], dash_v1["consumers"]), ([], gateway))
        self.assertIn("superseded by version 2 id 3408", dash_v1["retainedReason"])
        self.assertEqual(dash_v2["schemaFile"], f"{NS}.LiquidityWallDashboard/v2.avsc")
        v1, v2 = (json.loads((ROOT / "schema-manifest/schemas" / e["schemaFile"]).read_text()) for e in (dash_v1, dash_v2))
        wall = lambda sch: next(f for f in sch["fields"] if f["name"] == "walls")["type"]["items"]["fields"]
        self.assertEqual([f["name"] for f in wall(v2)][-3:], ["sizeNow", "sizeTape", "wordState"])
        self.assertEqual(wall(v2)[:-3], wall(v1), "version 2 only appends the three wall fields")
        self.assertTrue(all(f["default"] is None for f in wall(v2)[-3:]))
        depth = by_subject["es.rithmic.depth-value"]
        self.assertEqual(depth["id"], 3301)
        self.assertIn({"service": SVC, "accepts": [3301], "environments": ["dev"]}, depth["consumers"])

    @unittest.skipUnless(shutil.which("java") and JAR.is_file(), "needs java and the pinned tool jar")
    def test_every_snapshot_hashes_to_its_entry_and_the_tool_accepts_the_manifest(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        for e in m["entries"]:
            if e.get("family") == "liquiditywall":
                have = subprocess.run(["java", "-jar", str(JAR), "hash", str(ROOT / "schema-manifest/schemas" / e["schemaFile"])], capture_output=True, text=True, check=True).stdout.strip()
                self.assertEqual(have, e["schemaHash"], e["subject"])
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


class GatewayFreshness(unittest.TestCase):
    """The dev gateway's dashboard freshness bound sits above the wall service's publication lag (design rev 9 section 6): measured 8.0 to 12.2 s after the bucket end on 2026-11-06."""

    @staticmethod
    def env(rel: str) -> dict[str, str]:
        out = {}
        for doc in yaml.safe_load_all(read(rel)):
            if doc and doc.get("kind") == "Deployment" and doc["metadata"]["name"] == "feed-gateway-service":
                for c in doc["spec"]["template"]["spec"]["containers"]:
                    out.update({e["name"]: e.get("value") for e in c.get("env", [])})
        return out

    def test_the_dev_slice_sets_a_bound_above_the_measured_lag_and_no_other_environment_sets_it(self):
        dev = self.env("k8s/services/feed-gateway/overlays/dev/manifest.yaml")
        self.assertEqual(dev.get("ES_LIQUIDITY_WALL_RELAY_ENABLED"), "true")
        self.assertGreaterEqual(int(dev["ES_LIQUIDITY_WALL_FRESH_MS"]), 12_200 + 5_000, "above the measured maximum lag plus a margin; the service's own grace is 10000")
        for other in ("production", "experiment"):
            rel = f"k8s/services/feed-gateway/overlays/{other}/manifest.yaml"
            if (ROOT / rel).is_file():
                self.assertNotIn("ES_LIQUIDITY_WALL_FRESH_MS", self.env(rel), other)
        self.assertIn("ES_LIQUIDITY_WALL_FRESH_MS", read("k8s/overlays/dev/feed-gateway-dev-env-patch.yaml"), "the source patch the slice is generated from")


class NoProductionWiring(unittest.TestCase):
    def test_no_production_file_names_the_service_its_image_or_its_topics(self):
        needles = (SVC, "ES_LIQUIDITY_WALL", "liquidity-wall")
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

    def test_only_dev_declares_the_service_in_the_registry(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        self.assertEqual(reg[SVC]["envs"], ["dev"])


if __name__ == "__main__":
    unittest.main()
