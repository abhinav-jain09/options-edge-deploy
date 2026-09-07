"""HPSF is retired from the dev and production deploy. Prove it stays retired.

USER, 2026-09-07: "HPSF i dont need it prod and dev these servcie".

The evidence behind that decision, measured before it was made:
  * prod ran exactly one HPSF workload (hpsf-postgres-writer-service) at 0 replicas, and the
    HPSF *processing* service has no manifest in this repo at all — nothing has ever produced
    to those topics in production.
  * all 11 prod options.hpsf.* topics held 0 records, with retention.ms=-1 (infinite), so
    "empty" means never written, not aged out.
  * dev's writer was Running but every subscribed topic was empty and its consumer group had
    committed no offset on any partition. The only populated topic was options.hpsf.dlq, whose
    17,755 records were all from 2026-09-04 and all the same failure: the Databento i64::MAX
    "no bid" sentinel being parsed as a real price.
  * hpsf-stage-a / hpsf-stage-b manifests were already gone; the tests asserting them had been
    failing on main for some time (7 of 21 in the old tests/test_hpsf_ops_artifacts.py).

It was also actively harmful. scripts/kafka/topics.env created options.hpsf.{latest-signal,
market-flow,strike-score} at 1 partition while scripts/kafka/create-hpsf-topics.sh demanded 32
and refused to repair — two scripts in the SAME pipeline disagreeing about the same three
topics. That contradiction is what failed production deploy #642:

    Topic options.hpsf.market-flow exists with partitions=1 ...; expected at least partitions=32
    Refusing topic repair in HPSF topic script.

So a service nobody ran could block every production deploy. These assertions keep the
deploy path free of it.

DELIBERATELY STILL PRESENT, and not asserted against here: the standalone HPSF replay and
release-gate jobs (Jenkinsfile.hpsf-*), scripts/hpsf/*, the monitoring rules, the docs, and
es4's own es.options.hpsf.* topics. The user retired the dev/prod SERVICES, not the tooling,
and the 'hpsf-replay-mac' Jenkins agent label in the main Jenkinsfile is a node name, not an
HPSF feature.
"""

from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class HpsfRetiredFromDevAndProdTest(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text()

    def test_no_hpsf_workload_in_the_service_ssot(self) -> None:
        self.assertNotIn("hpsf-postgres-writer", self.read("services.yaml"))

    def test_no_hpsf_manifests_in_base(self) -> None:
        kustomization = self.read("k8s/base/kustomization.yaml")
        self.assertNotIn("hpsf", kustomization)
        for stale in [
            "k8s/base/hpsf-postgres-writer-deployment.yaml",
            "k8s/base/hpsf-postgres-writer-service.yaml",
        ]:
            self.assertFalse((ROOT / stale).exists(), f"{stale} should be deleted")
        self.assertFalse(
            (ROOT / "k8s/services/hpsf-postgres-writer").exists(),
            "the generated hpsf-postgres-writer service slices should be deleted",
        )

    def test_main_deploy_pipeline_does_not_touch_hpsf_topics(self) -> None:
        # This is the assertion that matters most: these two scripts are what failed #642.
        jenkinsfile = self.read("Jenkinsfile")
        self.assertNotIn("create-hpsf-topics.sh", jenkinsfile)
        self.assertNotIn("verify-hpsf-topics.sh", jenkinsfile)
        self.assertNotIn("reset-hpsf-stage-b-internal-topics.sh", jenkinsfile)
        self.assertNotIn("HPSF_PROCESSING_IMAGE", jenkinsfile)
        self.assertNotIn("HPSF_POSTGRES_WRITER_IMAGE", jenkinsfile)

    def test_generic_topic_list_no_longer_declares_hpsf_for_dev_or_prod(self) -> None:
        topics = self.read("scripts/kafka/topics.env")
        for line in topics.splitlines():
            if not line.startswith("OPTIONS_EDGE_TOPICS"):
                continue
            self.assertNotIn(
                "options.hpsf.",
                line,
                "dev/prod must not declare options.hpsf.* — that list created them at 1 "
                "partition while create-hpsf-topics.sh demanded 32, which failed deploy #642",
            )

    def test_es4_hpsf_topics_are_untouched(self) -> None:
        # Scope check in the other direction: the user retired dev and prod, not es4.
        topics = self.read("scripts/kafka/topics.env")
        self.assertIn("es.options.hpsf.dlq", topics)

    def test_deploy_scripts_carry_no_hpsf_image_or_rollout(self) -> None:
        for relative in [
            "scripts/deploy/apply.sh",
            "scripts/deploy/resolve-images.sh",
            "scripts/deploy/image-preflight.sh",
            "scripts/deploy/image-lock.sh",
            "scripts/deploy/verify-running-images.sh",
        ]:
            self.assertNotIn("hpsf", self.read(relative).lower(), relative)

    def test_image_tag_locks_carry_no_hpsf_entry(self) -> None:
        for relative in ["image-tags/dev.yaml", "image-tags/production.yaml"]:
            self.assertNotIn("hpsf", self.read(relative), relative)


if __name__ == "__main__":
    unittest.main()
