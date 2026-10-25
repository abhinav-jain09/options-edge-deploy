"""Deploy wiring for the TA-books documents CHART-STRUCTURE-TOOLS, CLASSICAL-CHART-PATTERNS, TICK-TRADE-TOOLS, ELLIOTT-FIB-CYCLE-TOOLS and the market-structure Avro cutover (W0) (the INDICATOR-STUDIES precedent is tests/test_indicator_studies_topics.py): every producer and gateway
flag is the literal "false" in every source and every GENERATED manifest, the two new services are registered everywhere a service must be (the five image
lists, the Jenkins choices, the overlays, image-tags, the three lifecycle hold lists) and are HELD at replicas 0, the W0 rollover flag is never committed,
the new topics are declared, and the destructive Jenkins stage keeps its gates."""
from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
P = "options.spx.market-structure."


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def env_values(path: Path, name: str) -> list[str]:
    return [v.strip("\"'") for v in re.findall(rf"name: {name}\n\s+value: (\S+)", path.read_text())]


class FlagsAreLiterallyFalse(unittest.TestCase):
    EXPECTED = {
        "MARKET_STRUCTURE_TOOLS_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                           "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                           "k8s/services/market-structure/overlays/production/manifest.yaml"],
        "MARKET_STRUCTURE_GEOMETRY_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                              "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                              "k8s/services/market-structure/overlays/production/manifest.yaml"],
        # X2 swing pivots (task T-5 of the level-absorption closure): off in the base and in production, on in dev only.
        "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED": ["k8s/services/market-structure/base/market-structure-deployment.yaml",
                                                  "k8s/services/market-structure/overlays/dev/manifest.yaml",
                                                  "k8s/services/market-structure/overlays/production/manifest.yaml"],
        "GATEWAY_STRUCTURE_TOOLS_ENABLED": [], "GATEWAY_GEOMETRY_ENABLED": [], "GATEWAY_TRADE_TOOLS_ENABLED": [], "GATEWAY_WAVE_CYCLE_ENABLED": [], "GATEWAY_BREAK_CLASSIFICATION_ENABLED": [],
        "TT_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/services/es-trade-tools/overlays/dev/manifest.yaml",
                       "k8s/services/es-trade-tools/overlays/production/manifest.yaml",
                       "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml"],
        "TT_PF_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_VP_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_FLOW_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "TT_CLIMAX_ENABLED": ["k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"],
        "WAVE_CYCLE_ENABLED": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml", "k8s/services/wave-cycle/overlays/dev/manifest.yaml",
                               "k8s/services/wave-cycle/overlays/production/manifest.yaml",
                               "k8s/overlays/production/wave-cycle-prod-enable-patch.yaml"],
        "WAVE_CYCLE_SEASONALITY_ENABLED": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml", "k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"],
        "WAVE_CYCLE_IGNORE_CHECKPOINT": ["k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"],
    }
    GATEWAY_FILES = ["k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/overlays/production/feed-gateway-prod-env-patch.yaml",
                     "k8s/services/feed-gateway/overlays/dev/manifest.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"]

    # 2026-10-21 (owner): the four gateway relay flags are ON in DEV ONLY, for the Order tab analysis layers; production and everything else stays literally false.
    DEV_GATEWAY_FILES = ["k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/services/feed-gateway/overlays/dev/manifest.yaml"]
    DEV_ON = ("GATEWAY_STRUCTURE_TOOLS_ENABLED", "GATEWAY_GEOMETRY_ENABLED", "GATEWAY_TRADE_TOOLS_ENABLED", "GATEWAY_WAVE_CYCLE_ENABLED", "GATEWAY_BREAK_CLASSIFICATION_ENABLED")

    # 2026-10-21 (owner: complete the production deployment, one layer at a time): PRODUCTION layers 2 and 3 = structure tools and geometry. Among the TA-books layer flags
    # (the names in EXPECTED), only those named here are "true", and only in the named production files; the rest stay "false" everywhere.
    PROD_ON = {
        "GATEWAY_STRUCTURE_TOOLS_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        "MARKET_STRUCTURE_TOOLS_ENABLED": ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"),
        "GATEWAY_GEOMETRY_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        # 2026-10-22 (owner: every service out of shadow, live on dev and production): the break-classification relay (Phase F) is ON in production too.
        "GATEWAY_BREAK_CLASSIFICATION_ENABLED": ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"),
        "MARKET_STRUCTURE_GEOMETRY_ENABLED": ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"),        # 2026-10-22 (owner: every service calibrated and live, never shadow): es-trade-tools (TICK-TRADE-TOOLS) and wave-cycle (ELLIOTT-FIB-CYCLE-TOOLS) run live in production.
        # The GATEWAY relay flags of those two layers stay "false" until the version-2 gateway and web are live (a later change).
        "TT_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_PF_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_VP_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_FLOW_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "TT_CLIMAX_ENABLED": ("k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml", "k8s/services/es-trade-tools/overlays/production/manifest.yaml"),
        "WAVE_CYCLE_ENABLED": ("k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"),
        "WAVE_CYCLE_SEASONALITY_ENABLED": ("k8s/overlays/production/wave-cycle-prod-enable-patch.yaml", "k8s/services/wave-cycle/overlays/production/manifest.yaml"),
    }

    # market-structure in DEV: structure tools and geometry are "true" because the running dev Deployment already reads them (the manifest had said "false", which a service-deploy would have applied);
    # swing pivots (X2) are on in dev only. See k8s/overlays/dev/market-structure-dev-env-patch.yaml.
    DEV_MS_FILES = ["k8s/overlays/dev/market-structure-dev-env-patch.yaml", "k8s/services/market-structure/overlays/dev/manifest.yaml"]
    DEV_MS_ON = ("MARKET_STRUCTURE_TOOLS_ENABLED", "MARKET_STRUCTURE_GEOMETRY_ENABLED", "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED")

    def expected_value(self, rel, name):
        if name in self.DEV_MS_ON and rel in self.DEV_MS_FILES:
            return "true"
        if rel in self.PROD_ON.get(name, ()):
            return "true"
        return "true" if name in self.DEV_ON and rel in self.DEV_GATEWAY_FILES else "false"

    def test_each_expected_source_and_generated_manifest_carries_its_flag_with_the_expected_value(self):
        for name, paths in self.EXPECTED.items():
            for rel in (paths or self.GATEWAY_FILES):
                self.assertEqual(env_values(ROOT / rel, name), [self.expected_value(rel, name)], f"{rel}: {name}")

    def test_no_manifest_anywhere_sets_any_of_these_flags_to_anything_but_false(self):
        for path in (ROOT / "k8s").rglob("*.yaml"):
            for name in self.EXPECTED:
                for v in env_values(path, name):
                    self.assertEqual(v, self.expected_value(str(path.relative_to(ROOT)), name), f"{path}: {name}")

    def test_production_enables_only_the_structure_tools_geometry_and_break_classification_relays(self):
        for rel in ("k8s/overlays/production/feed-gateway-prod-env-patch.yaml", "k8s/services/feed-gateway/overlays/production/manifest.yaml"):
            for name in self.DEV_ON:
                self.assertEqual(env_values(ROOT / rel, name), ["true" if name in ("GATEWAY_STRUCTURE_TOOLS_ENABLED", "GATEWAY_GEOMETRY_ENABLED", "GATEWAY_BREAK_CLASSIFICATION_ENABLED") else "false"], f"{rel}: {name}")
        for rel in ("k8s/overlays/production/market-structure-prod-env-patch.yaml", "k8s/services/market-structure/overlays/production/manifest.yaml"):
            self.assertEqual(env_values(ROOT / rel, "MARKET_STRUCTURE_TOOLS_ENABLED"), ["true"], rel)
        self.assertEqual(env_values(ROOT / "k8s/services/market-structure/overlays/production/manifest.yaml", "MARKET_STRUCTURE_GEOMETRY_ENABLED"), ["true"])
        self.assertEqual(env_values(ROOT / "k8s/services/market-structure/overlays/dev/manifest.yaml", "MARKET_STRUCTURE_TOOLS_ENABLED"), ["true"])
        self.assertEqual(env_values(ROOT / "k8s/services/market-structure/overlays/production/manifest.yaml", "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED"), ["false"])

    def test_the_committed_production_slices_equal_the_production_overlay_render_for_the_layer_flags(self):
        """Source/generated consistency: kustomize-render the production overlay and compare the layer flags with the committed generated slices."""
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/production")], capture_output=True, text=True, check=True, timeout=120).stdout
        rendered = {}
        for doc in yaml.safe_load_all(out):
            if isinstance(doc, dict) and doc.get("kind") == "Deployment" and doc["metadata"]["name"] in ("market-structure-service", "feed-gateway-service"):
                app = {"market-structure-service": "market-structure", "feed-gateway-service": "feed-gateway"}[doc["metadata"]["name"]]
                for e in next(c for c in doc["spec"]["template"]["spec"]["containers"] if c["name"] == app).get("env", []):
                    if e["name"] in self.EXPECTED:
                        rendered[(doc["metadata"]["name"], e["name"])] = e.get("value")
        want_keys = {("market-structure-service", "MARKET_STRUCTURE_SWING_PIVOTS_ENABLED"), ("market-structure-service", "MARKET_STRUCTURE_TOOLS_ENABLED"), ("market-structure-service", "MARKET_STRUCTURE_GEOMETRY_ENABLED"),
                     ("feed-gateway-service", "GATEWAY_STRUCTURE_TOOLS_ENABLED"), ("feed-gateway-service", "GATEWAY_GEOMETRY_ENABLED"),
                     ("feed-gateway-service", "GATEWAY_TRADE_TOOLS_ENABLED"), ("feed-gateway-service", "GATEWAY_WAVE_CYCLE_ENABLED"),
                     ("feed-gateway-service", "GATEWAY_BREAK_CLASSIFICATION_ENABLED")}
        self.assertEqual(set(rendered), want_keys)
        for dep, name in sorted(want_keys):
            rel = "k8s/services/market-structure/overlays/production/manifest.yaml" if dep == "market-structure-service" else "k8s/services/feed-gateway/overlays/production/manifest.yaml"
            self.assertEqual(env_values(ROOT / rel, name), [rendered[(dep, name)]], f"{rel}: {name}")
        self.assertEqual(rendered[("market-structure-service", "MARKET_STRUCTURE_TOOLS_ENABLED")], "true")
        self.assertEqual(rendered[("market-structure-service", "MARKET_STRUCTURE_GEOMETRY_ENABLED")], "true")

    def test_the_w0_recovery_rollover_flag_is_never_committed(self):
        for path in (ROOT / "k8s").rglob("*.yaml"):
            self.assertNotIn("name: MARKET_STRUCTURE_IGNORE_RECOVERY", path.read_text(), str(path))

    def test_the_two_new_services_declare_no_prefix_override_so_the_deployed_names_are_the_declared_ones(self):
        for rel in ("k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml", "k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"):
            self.assertNotIn("TOPIC_PREFIX", read(rel).replace("TOPIC_PREFIX and", "").replace("TOPIC_PREFIX is", "").replace("TOPIC_PREFIX,", ""))


class NewServicesAreHeldAndRegistered(unittest.TestCase):
    SERVICES = {"es-trade-tools": ("es-trade-tools-service", "ES_TRADE_TOOLS_IMAGE", "options-edge-es-trade-tools"),
                "wave-cycle": ("wave-cycle-service", "WAVE_CYCLE_IMAGE", "options-edge-wave-cycle")}

    def test_rendered_slices_are_one_deployment_at_replicas_one_on_production_and_zero_on_dev(self):
        for svc, (dep, _, image) in self.SERVICES.items():
            for env in ("dev", "production"):
                docs = [d for d in yaml.safe_load_all(read(f"k8s/services/{svc}/overlays/{env}/manifest.yaml")) if d]
                self.assertEqual([d["kind"] for d in docs], ["Deployment"], f"{svc} {env}")
                d = docs[0]
                self.assertEqual(d["metadata"]["name"], dep)
                # dev render stays HELD (dev is rolled by hand until its own enable change); production renders replicas 1 through its enable patch (2026-10-22)
                self.assertEqual(d["spec"]["replicas"], 1 if env == "production" else 0, f"{svc} {env}: see docs/ta-books/DEPLOY-ROLLOUT.md")
                self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", "single replica, a second transactional.id instance would fence the first")
                self.assertIn(image, d["spec"]["template"]["spec"]["containers"][0]["image"])

    def test_services_yaml_registers_both_as_standalone_dev_and_production(self):
        reg = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        for svc, (dep, _, image) in self.SERVICES.items():
            self.assertEqual((reg[svc]["managedBy"], reg[svc]["envs"], reg[svc]["image"], reg[svc]["deployments"]), ("standalone", ["dev", "production"], image, [dep]))

    def test_the_image_variable_is_in_every_image_resolution_list(self):
        for svc, (_, var, _) in self.SERVICES.items():
            self.assertEqual(len(re.findall(rf"^{var}=", read("scripts/deploy/resolve-images.sh"), re.M)), 2, "both heredoc branches")
            for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/image-preflight.sh", "Jenkinsfile"):
                self.assertIn(var, read(rel), f"{rel}: {var}")
            self.assertGreaterEqual(read("Jenkinsfile").count(var), 3, "param, defaults map and the promote list")

    def test_image_tags_and_overlays_reference_both(self):
        for svc, (dep, _, image) in self.SERVICES.items():
            self.assertIn(f"{dep}: 192.168.100.252:5000/{image}:dev", read("image-tags/dev.yaml"))
            self.assertIn(f"{dep}: 192.168.100.252:5000/{image}:prod", read("image-tags/production.yaml"))
            for env in ("dev", "production"):
                self.assertIn(f"../../services/{svc}/base", read(f"k8s/overlays/{env}/kustomization.yaml"))
            self.assertIn(f"newName: host.docker.internal:5001/{image}", read("k8s/overlays/dev/kustomization.yaml"))

    def test_the_production_lifecycle_lists_do_not_hold_either_down_and_the_dev_list_still_does(self):
        # production runs both (2026-10-22): a KEEP_DOWN entry would scale them to 0 at the next reboot or morning start
        for dep, _, _ in self.SERVICES.values():
            for rel in ("scripts/ops/oe-boot-bringup.sh", "scripts/ops/morning-autostart.sh"):
                text = "\n".join(l for l in read(rel).splitlines() if re.match(r"\s*KEEP_DOWN=", l))
                self.assertNotRegex(text, rf"(?<![\w-]){re.escape(dep)}(?![\w-])", f"{rel}: {dep} must not be held down on production")
            self.assertRegex(read("scripts/ops/dev-cleanup.sh"), rf"(?<![\w-]){re.escape(dep)}(?![\w-])", f"dev-cleanup.sh: {dep}")

    def test_the_service_deploy_job_offers_both_and_runs_their_topic_barriers(self):
        text = read("Jenkinsfile.service-deploy")
        for svc in self.SERVICES:
            self.assertIn(f"'{svc}',", text)
            self.assertIn(f"params.SERVICE == '{svc}' && !params.DEPLOY_DRY_RUN", text)
        self.assertIn("scripts/kafka/ensure-es-trade-tools-topics.sh", text)
        self.assertIn("scripts/kafka/ensure-wave-cycle-topics.sh", text)

    def test_the_heap_never_exceeds_the_headroom_gate_and_the_probes_match_the_services_endpoints(self):
        tt = yaml.safe_load(read("k8s/services/es-trade-tools/base/es-trade-tools-deployment.yaml"))["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("-Xmx384m", [e["value"] for e in tt["env"] if e["name"] == "JAVA_TOOL_OPTIONS"][0])
        self.assertEqual(tt["resources"]["limits"]["memory"], "1Gi")
        self.assertEqual(tt["ports"][0]["containerPort"], 8153)
        wc = yaml.safe_load(read("k8s/services/wave-cycle/base/wave-cycle-deployment.yaml"))["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(wc["ports"][0]["containerPort"], 8161)
        self.assertEqual(wc["readinessProbe"]["httpGet"]["path"], "/health/ready")
        self.assertNotIn("-Xmx", " ".join(e.get("value", "") for e in wc["env"]), "the image ENTRYPOINT fixes the heap; a value here would be ignored and lie")


class TopicsAndCutoverStage(unittest.TestCase):
    def test_tools_and_geometry_topics_are_declared_one_partition_two_sessions_exact(self):
        out = subprocess.run(["bash", "-c", f'source "{ROOT}/scripts/kafka/topics.env"; echo "$OPTIONS_EDGE_TOPICS"; echo "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"; '
                              f'echo "$OPTIONS_EDGE_EXACT_PARTITION_TOPICS"'], capture_output=True, text=True, check=True).stdout.splitlines()
        for t in (P + "tools", P + "geometry"):
            self.assertIn(f"{t}:1", out[0].split())
            self.assertIn(f"{t}=172800000", out[1].split())
            self.assertIn(t, out[2].split())
            self.assertNotRegex(t, r"\.v\d+$")

    def test_the_ensure_script_ensures_tools_and_geometry_unconditionally(self):
        text = read("scripts/kafka/ensure-market-structure-topics.sh")
        self.assertIn('ensure "$TOOLS_TOPIC" 172800000', text)
        self.assertIn('ensure "$GEOMETRY_TOPIC" 172800000', text)
        self.assertIn('ensure "$SWING_PIVOTS_TOPIC" 172800000', text)

    def test_the_wipe_stage_runs_after_the_rollout_and_keeps_its_gates(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertLess(text.index("stage('Deploy (service-scoped)')"), text.index("stage('Roll forward market-structure topics (W0 step 4)')"),
                        "the Avro writer is live BEFORE the wipe (the 2026-10-18 incident)")
        stage = text[text.index("stage('Roll forward market-structure topics (W0 step 4)')"):text.index("stage('Verify web UI (post-rollout)')")]
        for must in ("params.W0_CUTOVER_WIPE", "--producers-attested", "--expect-image-digest", "--expect-gateway-digest", "--gates-only",
                     "roll-forward-market-structure.sh --recreate"):
            self.assertIn(must, stage)
        # a dry run rehearses every gate (--gates-only) instead of skipping the stage
        self.assertNotIn("!params.DEPLOY_DRY_RUN", stage)
        # every W0-only argument is judged BEFORE the rollout stage can mutate anything
        early = text[text.index("stage('Validate W0 cutover parameters')"):text.index("stage('Deploy path')")]
        self.assertLess(text.index("stage('Validate W0 cutover parameters')"), text.index("stage('Deploy (service-scoped)')"))
        for must in ("W0_EXPECT_IMAGE_DIGEST ==~ /sha256:[0-9a-f]{64}/", "W0_EXPECT_GATEWAY_DIGEST ==~ /sha256:[0-9a-f]{64}/", "W0_PRE_CUTOVER_IDS ==~",
                     "W0_PRODUCERS_ATTESTED", "MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER", "SERVICE != 'market-structure'"):
            self.assertIn(must, early)
        # the destructive script is reachable from nowhere else
        self.assertEqual(text.count("scripts/kafka/roll-forward-market-structure.sh --recreate"), 1)

    def test_the_registration_stage_precedes_the_rollout_and_needs_the_contracts_commit(self):
        text = read("Jenkinsfile.service-deploy")
        self.assertLess(text.index("stage('Register TA-books schemas')"), text.index("stage('Deploy (service-scoped)')"))
        stage = text[text.index("stage('Register TA-books schemas')"):text.index("stage('Recreate indicator topics (Avro cutover)')")]
        self.assertIn("CONTRACTS_PERMITTED_SHA", stage)
        self.assertIn("permitted-sha-guard.sh --dir .jenkins-tmp/contracts-ta --ref main", stage)

    def test_the_rollover_parameter_reaches_service_deploy_sh_as_an_environment_variable_it_reads(self):
        self.assertIn("MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER", read("scripts/deploy/service-deploy.sh"))
        self.assertIn("booleanParam(name: 'MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER', defaultValue: false", read("Jenkinsfile.service-deploy"))


class DocumentedRolloutMatchesTheCode(unittest.TestCase):
    def test_the_rollout_doc_exists_and_names_every_script_it_relies_on(self):
        doc = read("docs/ta-books/DEPLOY-ROLLOUT.md")
        for rel in ("scripts/kafka/ensure-fixed-id-schemas.sh", "scripts/kafka/roll-forward-market-structure.sh", "scripts/kafka/ensure-market-structure-topics.sh",
                    "scripts/kafka/ensure-es-trade-tools-topics.sh", "scripts/kafka/ensure-wave-cycle-topics.sh"):
            self.assertIn(Path(rel).name, doc)
            self.assertTrue((ROOT / rel).is_file(), rel)


if __name__ == "__main__":
    unittest.main()
