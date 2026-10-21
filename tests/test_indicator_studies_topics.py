"""STUDIES_V1 deploy wiring (Phase A, PHASE-A-INDICATORS-REQUIREMENT.md A-R10.1/A-R10.4/A-R10.5, tracked item T-2): the two study
topics are declared in the REAL topics.env with the producer's shapes, ensure-indicator-topics.sh creates and reconciles them,
the es4 mirror allow-list carries the es.* pair, the schema scripts know ids 2501-2503, and the producer / gateway flags are
explicitly false in every rendered manifest."""
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
TOPICS_ENV = ROOT / "scripts/kafka/topics.env"
ENSURE = ROOT / "scripts/kafka/ensure-indicator-topics.sh"
FAKE = ROOT / "tests/fake_kafka_cli.py"
BARS, CURRENT = "options.indicators.studies.bars", "options.indicators.studies.current"
ES_BARS, ES_CURRENT = "es." + BARS, "es." + CURRENT
DAY45, DAY7 = "3888000000", "604800000"


def declarations(variables: list[str]) -> dict[str, list[str]]:
    """Source the real topics.env in bash and print each requested variable as a word list."""
    script = f'source "{TOPICS_ENV}"; ' + "; ".join(f'echo "{v}=${{{v}}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


class TopicsEnvDeclaresTheStudyTopics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = declarations(["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS",
                              "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_ES4_TOPICS",
                              "OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES",
                              "OPTIONS_EDGE_ES4_COMPACTED_TOPICS"])

    def test_dev_and_prod_declare_all_four_with_the_producers_partition_counts(self):
        for entry in (f"{BARS}:8", f"{CURRENT}:1", f"{ES_BARS}:8", f"{ES_CURRENT}:1"):
            self.assertEqual(self.d["OPTIONS_EDGE_TOPICS"].count(entry), 1, entry)

    def test_retention_is_45_days_for_bars_and_7_days_for_current_on_both_sides(self):
        for key, want in ((BARS, DAY45), (CURRENT, DAY7), (ES_BARS, DAY45), (ES_CURRENT, DAY7)):
            self.assertIn(f"{key}={want}", self.d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"], key)
        for key, want in ((ES_BARS, DAY45), (ES_CURRENT, DAY7)):
            self.assertIn(f"{key}={want}", self.d["OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES"], key)

    def test_all_four_are_exact_partition_and_the_es4_pair_is_declared_on_es4(self):
        for t in (BARS, CURRENT, ES_BARS, ES_CURRENT):
            self.assertIn(t, self.d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"], t)
        for t in (ES_BARS, ES_CURRENT):
            self.assertIn(t, self.d["OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS"], t)
        self.assertIn(f"{ES_BARS}:8", self.d["OPTIONS_EDGE_ES4_TOPICS"])
        self.assertIn(f"{ES_CURRENT}:1", self.d["OPTIONS_EDGE_ES4_TOPICS"])

    def test_only_the_dev_current_topic_is_compacted_and_prod_subtracts_it_like_signals_current(self):
        self.assertIn(CURRENT, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"])
        for t in (BARS, ES_BARS, ES_CURRENT):
            self.assertNotIn(t, self.d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
            self.assertNotIn(t, self.d["OPTIONS_EDGE_ES4_COMPACTED_TOPICS"], t)
        for t in (CURRENT, ES_CURRENT):
            self.assertIn(t, self.d["OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS"], t)
        # the bars are append-only history: never in a compacted list, so never needing a prod subtraction
        self.assertNotIn(BARS, self.d["OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS"])

    def test_no_study_topic_carries_a_version_suffix(self):
        for t in (BARS, CURRENT, ES_BARS, ES_CURRENT):
            self.assertIsNone(re.search(r"\.v\d+$", t), t)


def run_ensure(existing: dict, dry: bool = False):
    tmp = Path(tempfile.mkdtemp())
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ["kafka-topics", "kafka-configs", "kafka-get-offsets", "kafka-broker-api-versions"]:
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    state = tmp / "state.json"
    state.write_text(json.dumps({"topics": existing}))
    calls = tmp / "calls"
    calls.touch()
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls),
               KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1", INDICATOR_APP_ID="indicator-service-x",
               DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(ENSURE)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())["topics"]


def shaped(parts: int, policy: str, retention: str) -> dict:
    return {"partitions": parts, "rf": 1, "configs": {"cleanup.policy": policy, "retention.ms": retention}, "end": 5}


class EnsureIndicatorTopicsReconcilesTheStudyTopics(unittest.TestCase):
    def test_absent_study_topics_are_created_at_the_declared_shape(self):
        rc, out, _, topics = run_ensure({})
        self.assertEqual(rc, 0, out)
        self.assertEqual(topics[BARS]["partitions"], 8)
        self.assertEqual(topics[BARS]["configs"], {"cleanup.policy": "delete", "retention.ms": DAY45})
        self.assertEqual(topics[CURRENT]["partitions"], 1)
        self.assertEqual(topics[CURRENT]["configs"], {"cleanup.policy": "compact,delete", "retention.ms": DAY7})

    def test_a_drifted_policy_or_retention_is_reconciled_and_the_end_offset_is_untouched(self):
        rc, out, _, topics = run_ensure({BARS: shaped(8, "compact", "1000"), CURRENT: shaped(1, "delete", DAY7)})
        self.assertEqual(rc, 0, out)
        self.assertEqual(topics[BARS]["configs"]["cleanup.policy"], "delete")
        self.assertEqual(topics[BARS]["configs"]["retention.ms"], DAY45)
        self.assertEqual(topics[CURRENT]["configs"]["cleanup.policy"], "compact,delete")
        self.assertEqual(topics[BARS]["end"], 5, "reconcile never recreates")

    def test_a_wrong_partition_count_fails_closed_and_changes_nothing(self):
        rc, out, calls, topics = run_ensure({BARS: shaped(32, "delete", DAY45)})
        self.assertEqual(rc, 1, out)
        self.assertIn(f"FAIL: '{BARS}' has PartitionCount=32, declared 8", out)
        self.assertFalse(any("--delete" in c for c in calls))
        self.assertEqual(topics[BARS]["partitions"], 32)

    def test_dry_run_creates_nothing(self):
        rc, out, calls, topics = run_ensure({}, dry=True)
        self.assertEqual(rc, 0, out)
        self.assertNotIn(BARS, topics)
        self.assertFalse(any("--create" in c for c in calls))


class MirrorAndSchemaWiring(unittest.TestCase):
    def test_mirror_job_allow_list_and_shape_contract_carry_the_es_pair(self):
        text = (ROOT / "Jenkinsfile.es-indicator-mirror").read_text()
        self.assertIn(f"'{ES_BARS}', '{ES_CURRENT}'", text)
        self.assertIn(f"{ES_BARS})     PARTS=8; POLICY=delete;         RET={DAY45} ;;", text)
        self.assertIn(f"{ES_CURRENT})  PARTS=1; POLICY=delete; RET={DAY7} ;;", text)
        yml = (ROOT / "ansible/vars/es-mirrors.yml").read_text()
        self.assertIn(f"- name: {ES_BARS}\n        assert: {{ target: {{parts: 8, not_compacted: true}} }}", yml)
        self.assertIn(f"- name: {ES_CURRENT}\n        assert: {{ target: {{parts: 1, not_compacted: true}} }}", yml)

    def test_schema_scripts_know_the_three_study_ids(self):
        fixed = (ROOT / "scripts/kafka/ensure-indicator-schemas.sh").read_text()
        for name, id_ in (("StudyBar", 2501), ("StudiesCurrent", 2502), ("StudyEngineImage", 2503)):
            self.assertIn(f"com.optionsedge.contracts.indicators.avro.{name}|{id_}|{name}.avsc", fixed)
        self.assertIn("2501 2502 2503; do", (ROOT / "scripts/kafka/assert-indicator-schema-parity.sh").read_text())


class FlagsAreExplicitlyOff(unittest.TestCase):
    """The producer flag and the gateway flag are literal in every source and generated manifest that must carry them: "false" everywhere
    except the DEV rendering (the dev overlay switches the studies on; production, experiment and the base stay false)."""

    DEV_ON = {
        "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml",
        "k8s/services/feed-gateway/overlays/dev/manifest.yaml",
        "k8s/services/indicator-service/overlays/dev/manifest.yaml",
    }
    EXPECTED = {
        "INDICATOR_STUDIES_ENABLED": ["k8s/base/indicator-service-deployment.yaml", "k8s/es4/services/indicator-service.yaml",
                                      "k8s/services/indicator-service/overlays/dev/manifest.yaml",
                                      "k8s/services/indicator-service/overlays/experiment/manifest.yaml",
                                      "k8s/services/indicator-service/overlays/production/manifest.yaml"],
        "GATEWAY_STUDIES_ENABLED": ["k8s/overlays/dev/feed-gateway-dev-env-patch.yaml", "k8s/overlays/production/feed-gateway-prod-env-patch.yaml",
                                    "k8s/services/feed-gateway/overlays/dev/manifest.yaml",
                                    "k8s/services/feed-gateway/overlays/production/manifest.yaml"],
    }

    def test_each_expected_source_and_generated_manifest_carries_its_flag_with_the_expected_value(self):
        for name, paths in self.EXPECTED.items():
            for rel in paths:
                found = re.findall(rf"name: {name}\n\s+value: (\S+)", (ROOT / rel).read_text())
                want = "true" if rel in self.DEV_ON else "false"
                self.assertEqual([v.strip("\"'") for v in found], [want], f"{rel}: {name}")

    @staticmethod
    def rendered_flags(overlay: str) -> dict:
        """kustomize-render an overlay and return {(deployment, container, flag): value} for the two study flags, structurally."""
        import yaml
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays" / overlay)], capture_output=True, text=True, check=True).stdout
        found = {}
        for doc in yaml.safe_load_all(out):
            if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
                continue
            for c in doc["spec"]["template"]["spec"].get("containers", []):
                for e in c.get("env", []) or []:
                    if e.get("name") in ("INDICATOR_STUDIES_ENABLED", "GATEWAY_STUDIES_ENABLED"):
                        found.setdefault((doc["metadata"]["name"], c["name"], e["name"]), []).append(str(e.get("value")))
        return found

    def test_the_dev_render_enables_exactly_the_producer_container_and_the_gateway(self):
        self.assertEqual(self.rendered_flags("dev"), {
            ("indicator-service", "indicator-service", "INDICATOR_STUDIES_ENABLED"): ["true"],
            ("feed-gateway-service", "feed-gateway", "GATEWAY_STUDIES_ENABLED"): ["true"],
        })

    def test_the_production_render_carries_both_flags_and_both_are_false(self):
        self.assertEqual(self.rendered_flags("production"), {
            ("indicator-service", "indicator-service", "INDICATOR_STUDIES_ENABLED"): ["false"],
            ("feed-gateway-service", "feed-gateway", "GATEWAY_STUDIES_ENABLED"): ["false"],
        })

    def test_the_dev_patch_is_referenced_by_exactly_one_kustomization_the_dev_one(self):
        refs = [str(p.relative_to(ROOT)) for p in (ROOT / "k8s").rglob("kustomization.y*ml")
                for _ in range(p.read_text().count("indicator-studies-dev-enable-patch.yaml"))]
        self.assertEqual(refs, ["k8s/overlays/dev/kustomization.yaml"])

    @staticmethod
    def flag_entries(path: Path) -> list:
        """Every (deployment, container, flag, value) env entry of a manifest file, parsed (any YAML spelling)."""
        import yaml
        out = []
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
                continue
            for c in (doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []) or []):
                for e in c.get("env", []) or []:
                    if e.get("name") in ("INDICATOR_STUDIES_ENABLED", "GATEWAY_STUDIES_ENABLED"):
                        out.append((doc["metadata"]["name"], c["name"], e["name"], str(e.get("value"))))
        return out

    def test_the_dev_generated_slices_and_patches_carry_exactly_these_enabled_entries(self):
        want = {
            "k8s/services/indicator-service/overlays/dev/manifest.yaml": [("indicator-service", "indicator-service", "INDICATOR_STUDIES_ENABLED", "true")],
            "k8s/services/feed-gateway/overlays/dev/manifest.yaml": [("feed-gateway-service", "feed-gateway", "GATEWAY_STUDIES_ENABLED", "true")],
            "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml": [("feed-gateway-service", "feed-gateway", "GATEWAY_STUDIES_ENABLED", "true")],
            "k8s/overlays/dev/indicator-studies-dev-enable-patch.yaml": [("indicator-service", "indicator-service", "INDICATOR_STUDIES_ENABLED", "true")],
        }
        for rel, entries in want.items():
            self.assertEqual(self.flag_entries(ROOT / rel), entries, rel)

    def test_every_other_manifest_has_the_flags_false_and_an_unparseable_file_naming_a_flag_fails(self):
        import yaml
        allowed = set(self.DEV_ON) | {"k8s/overlays/dev/indicator-studies-dev-enable-patch.yaml"}
        for path in sorted(p for p in (ROOT / "k8s").rglob("*") if p.is_file() and p.suffix in (".yaml", ".yml")):
            rel = str(path.relative_to(ROOT))
            if rel in allowed:
                continue
            text = path.read_text()
            if "INDICATOR_STUDIES_ENABLED" not in text and "GATEWAY_STUDIES_ENABLED" not in text:
                continue
            try:
                entries = self.flag_entries(path)
            except yaml.YAMLError as e:
                self.fail(f"{rel} names a study flag but is not parseable YAML: {e}")
            code = [ln.split(" #")[0] for ln in text.splitlines() if not ln.lstrip().startswith("#")]
            textual = sum(ln.count(flag) for ln in code for flag in ("INDICATOR_STUDIES_ENABLED", "GATEWAY_STUDIES_ENABLED"))
            self.assertEqual(len(entries), textual, f"{rel}: every non-comment occurrence of a flag must be a structural env entry")
            self.assertTrue(all(v == "false" for (_, _, _, v) in entries), f"{rel}: {entries}")

    def test_the_destructive_recreate_allow_list_does_not_name_a_study_topic(self):
        text = ENSURE.read_text()
        allow = [line for line in text.splitlines() if line.lstrip().startswith(("BASE6=", "MIRROR4="))]
        self.assertEqual(len(allow), 2)
        self.assertFalse(any("studies" in line for line in allow))


if __name__ == "__main__":
    unittest.main()
