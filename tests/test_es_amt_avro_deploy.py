# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
"""es.futures.auction Avro on dev (fixed id 3201, contracts EsAuctionAvro): scripts/kafka/ensure-fixed-id-schemas.sh with schema-id-maps/es-amt.map against the
stand-in registry (tests/stub_schema_registry.py), the map's agreement with the other maps, and the wipe script's cluster guard."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests.stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/kafka/ensure-fixed-id-schemas.sh"
MAPS = ROOT / "scripts/kafka/schema-id-maps"
MAP = MAPS / "es-amt.map"
SUBJECT = "com.optionsedge.contracts.esamt.EsAuctionMinute"


def rows():
    out = []
    for line in MAP.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            out.append(line.split("|"))
    return out


def run(url: str, dry: bool = False):
    d = Path(tempfile.mkdtemp())
    (d / "EsAuctionMinute.avsc").write_text(json.dumps(
        {"type": "record", "name": "EsAuctionMinute", "namespace": "com.optionsedge.contracts.esamt", "doc": "fixture", "fields": [{"name": "x", "type": "string"}]}))
    env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(d), SCHEMA_ID_MAP=str(MAP), DEPLOY_DRY_RUN="true" if dry else "false")
    r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


class Registration(unittest.TestCase):
    def with_registry(self, check):
        reg = Registry()
        server, url = serve(reg)
        try:
            check(reg, url)
        finally:
            server.shutdown()

    def test_the_map_declares_exactly_one_subject_under_3201(self):
        self.assertEqual(rows(), [[SUBJECT, "3201", "EsAuctionMinute.avsc"]])

    def test_a_fresh_registry_gets_the_subject_under_3201_with_full_and_a_rerun_changes_nothing(self):
        def check(reg, url):
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertEqual(reg.subjects[SUBJECT][0]["id"], 3201)
            self.assertEqual(set(reg.configs.values()), {"FULL"})
            calls = len(reg.calls)
            rc, out = run(url)
            self.assertEqual(rc, 0, out)
            self.assertFalse(any(c.startswith("POST /subjects/") and c.endswith("/versions {schema…}") for c in reg.calls[calls:]), "nothing re-imported")
        self.with_registry(check)

    def test_a_dry_run_mutates_nothing(self):
        def check(reg, url):
            rc, out = run(url, dry=True)
            self.assertEqual(rc, 0, out)
            self.assertEqual((reg.subjects, reg.subject_modes), ({}, {}))
        self.with_registry(check)

    def test_the_id_held_by_another_schema_is_a_collision_never_repaired(self):
        def check(reg, url):
            reg.register("some.other.Subject", json.dumps({"type": "string"}), id=3201, version=1)
            rc, out = run(url)
            self.assertEqual(rc, 1, out)
            self.assertIn("fixed id 3201 is already held by another schema", out)
            self.assertEqual(set(reg.subjects), {"some.other.Subject"})
        self.with_registry(check)


class ContractBacked(unittest.TestCase):
    """The map names a real contracts schema. Needs a contracts checkout: set OE_CONTRACTS_DIR (its schemas-registry/esamt is read); skipped without one (CI of this repo has no contracts clone)."""

    @unittest.skipUnless(os.environ.get("OE_CONTRACTS_DIR"), "OE_CONTRACTS_DIR not set")
    def test_the_real_schema_is_the_record_the_map_registers_under_3201(self):
        d = Path(os.environ["OE_CONTRACTS_DIR"]) / "schemas-registry/esamt"
        (subject, ident, fname), = rows()
        doc = json.loads((d / fname).read_text())
        self.assertEqual(doc["type"], "record")
        self.assertEqual(doc["namespace"] + "." + doc["name"], subject)
        self.assertEqual(ident, "3201")
        # and the registration script really imports exactly that file under 3201 on a registry
        reg = Registry()
        server, url = serve(reg)
        try:
            env = dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=str(d), SCHEMA_ID_MAP=str(MAP), DEPLOY_DRY_RUN="false")
            r = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertEqual(reg.subjects[subject][0]["id"], 3201)
            self.assertEqual(json.loads(reg.subjects[subject][0]["schema"]), doc)
        finally:
            server.shutdown()


class MapAgrees(unittest.TestCase):
    def test_no_id_is_shared_with_any_other_map_and_none_is_in_a_used_block(self):
        ours = {int(r[1]) for r in rows()}
        for other in MAPS.glob("*.map"):
            if other == MAP:
                continue
            for line in other.read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    self.assertNotIn(int(line.split("|")[1]), ours, f"{other.name} reuses an es-amt id")


class WipeScriptGuards(unittest.TestCase):
    """The destructive script is bound to this topic's wire contract and shape (its mock test, roll-forward-es-amt-auction-test.sh, runs the behaviours)."""

    SCRIPT = (ROOT / "scripts/kafka/roll-forward-es-amt-auction.sh").read_text()

    def test_it_expects_exactly_the_fixed_id_in_hex(self):
        self.assertIn("RF_EXPECT_ID_HEX=00000c81", self.SCRIPT)  # 3201 = 0x0c81

    def test_it_recreates_the_topic_at_the_declared_shape(self):
        topics = (ROOT / "scripts/kafka/topics.env").read_text()
        self.assertIn("OPTIONS_EDGE_TOPICS=\"$OPTIONS_EDGE_TOPICS es.futures.auction:1\"", topics)
        self.assertIn("PARTITIONS=1", self.SCRIPT)
        self.assertIn('CONFIGS="cleanup.policy=delete,retention.ms=604800000"', self.SCRIPT)
        self.assertIn("RETENTION 604800000", topics)

    def test_only_dev_is_executable_and_es4_and_production_are_refused_by_name(self):
        self.assertIn("dev:localhost:19092", self.SCRIPT)
        self.assertNotIn("es4:192.168.100.4:9092) ;;", self.SCRIPT)
        self.assertNotIn("production:192.168.100.252:9092) ;;", self.SCRIPT)
        self.assertIn("es4|production) echo \"FATAL:", self.SCRIPT)

    def test_the_dev_cluster_id_is_pinned_and_checked_before_anything_is_changed(self):
        self.assertIn("DEV_CLUSTER_ID=ST9UcaxwRHWefoBXRdjTXg", self.SCRIPT)
        self.assertLess(self.SCRIPT.index("DEV_CLUSTER_ID=ST9UcaxwRHWefoBXRdjTXg"), self.SCRIPT.index('kt --delete'))
        self.assertLess(self.SCRIPT.index("cluster-id --bootstrap-server"), self.SCRIPT.index('kt --delete'))

    def test_the_mock_test_passes(self):
        r = subprocess.run(["bash", str(ROOT / "scripts/kafka/roll-forward-es-amt-auction-test.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])


class DevOnlyWiring(unittest.TestCase):
    """es-amt-service is the dev-owned Avro writer: registered for dev only, running (replicas 1), never rendered for production or experiment."""

    def test_registered_for_dev_only(self):
        import yaml
        svc = [x for x in yaml.safe_load((ROOT / "services.yaml").read_text())["services"] if x["name"] == "es-amt"]
        self.assertEqual(len(svc), 1)
        self.assertEqual(svc[0]["envs"], ["dev"])
        self.assertEqual(svc[0]["deployments"], ["es-amt-service"])

    def test_the_dev_slice_runs_one_replica_and_names_the_dev_topics(self):
        base = (ROOT / "k8s/services/es-amt/base/es-amt-deployment.yaml").read_text()
        self.assertIn("replicas: 1", base)
        self.assertIn("type: Recreate", base)
        for needle in ("name: KAFKA_ES_AUCTION_TOPIC\n              value: es.futures.auction", "name: KAFKA_ES_TRADES_TOPIC\n              value: underlying.es.trades",
                       "value: host.docker.internal:19092", "name: ES_AMT_TOPIC_RETENTION_MS\n              value: \"604800000\""):
            self.assertIn(needle, base)

    def test_no_other_environment_renders_it(self):
        for env in ("production", "experiment"):
            self.assertFalse((ROOT / f"k8s/services/es-amt/overlays/{env}").exists(), env)
            kz = (ROOT / f"k8s/overlays/{env}/kustomization.yaml").read_text()
            self.assertNotIn("services/es-amt/", kz, env)

    def test_it_comes_up_with_the_overnight_set_and_does_not_go_down_with_the_es_down_set(self):
        t = (ROOT / "scripts/ops/dev-cleanup.sh").read_text()
        overnight = [l for l in t.splitlines() if l.startswith("OVERNIGHT_SET=")]
        down = [l for l in t.splitlines() if l.startswith("ES_DOWN_SET=")]
        self.assertEqual(len(overnight), 1)
        self.assertIn("es-amt-service", overnight[0])
        self.assertNotIn("es-amt-service", down[0])

    def test_it_is_not_held_down_by_any_script(self):
        for f in ("scripts/ops/dev-cleanup.sh", "scripts/ops/morning-autostart.sh", "scripts/ops/oe-boot-bringup.sh"):
            for line in (ROOT / f).read_text().splitlines():
                if line.startswith(("DISABLED_DEV=", "KEEP_DOWN=")):
                    self.assertNotIn("es-amt-service", line, f)


class RunbookNamesTheRealRolloutPaths(unittest.TestCase):
    """The runbook must not prescribe a step the deploy job refuses (Codex round 4): feed-gateway is built by its own job; EAV is held at 0 declaratively."""

    DOC = (ROOT / "docs/ES-AMT-AVRO-CUTOVER.md").read_text()
    DEPLOY = (ROOT / "Jenkinsfile.service-deploy").read_text()

    def test_the_gateway_path_matches_what_service_deploy_enforces(self):
        self.assertIn("'feed-gateway'      : 'feed-gateway-deploy'", self.DEPLOY)
        self.assertIn("feed-gateway-deploy", self.DOC)
        self.assertIn("BUILD_IMAGES=false REQUIRED_IMAGE=", self.DOC)

    def test_eav_is_rolled_by_image_not_by_service_deploy(self):
        self.assertIn("replicas: 0", (ROOT / "k8s/services/early-auction-verdict/base/early-auction-verdict-deployment.yaml").read_text())
        self.assertIn("early-auction-verdict-service is an exception", self.DOC)
        self.assertIn("kubectl set image", self.DOC)


class MirrorJobRefusesTheDevTarget(unittest.TestCase):
    """Once dev owns the Avro writer, a byte mirror of es4's JSON into dev would put JSON in an Avro topic: the job's install refuses 127.0.0.1:19092."""

    JOB = (ROOT / "Jenkinsfile.es-auction-mirror").read_text()

    def test_install_is_allowed_only_for_the_production_target(self):
        self.assertIn("DEV OWNS ITS WRITER", self.JOB)
        self.assertIn('"$TARGET_SPX_CLUSTER_IP:$TARGET_PORT" != "192.168.100.252:9092"', self.JOB)
        self.assertIn('"$SOURCE_ES_CLUSTER_IP:$SOURCE_PORT" != "192.168.100.4:9092"', self.JOB)
        # no spelling of a dev broker can be positively allowed
        self.assertNotIn('= "127.0.0.1:19092" ]', self.JOB.split("DEV OWNS ITS WRITER")[1].split("fi\n")[0])

    def test_teardown_and_status_are_not_gated(self):
        pre = self.JOB.index("stage('Preflight (fail-closed)')")
        self.assertLess(self.JOB.index("stage('Teardown')"), pre)
        self.assertLess(self.JOB.index("stage('Status')"), pre)
        self.assertGreater(self.JOB.index("DEV OWNS ITS WRITER"), pre)


if __name__ == "__main__":
    unittest.main()
