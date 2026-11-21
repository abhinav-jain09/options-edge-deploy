# CI-REQUIRED
#
"""ibkr-feed-service: the readiness probe must ask the READINESS endpoint, not the liveness one.

WHY THIS EXISTS (2026-11-21). The dev pod reported readyReplicas=1 — pod Ready, 0 restarts, for
8+ hours — while the feed could not reach IB Gateway at all and was looping every 3 seconds:

    IBKR error id=-1 code=502 msg=Couldn't connect to TWS...
    IBKR startup failed: Could not connect to IBKR at host.docker.internal:4002. Retrying in 3s.

The service was NOT at fault: /health/ready already answered 503 with the reason. The manifest's
readinessProbe was pointed at /health/live, which returns 200 unconditionally — so Kubernetes
never asked the question, and the 200 it accepted carried "ready":false in its own body. Any
readiness-based alert or dashboard was blind to a total input outage.

The split this pins (the es-trade-linearizer pattern):
  * livenessProbe  -> /health/live  : always 200, so a logged-out IB Gateway does NOT restart the
                                      pod (restarting cannot log a gateway in).
  * readinessProbe -> /health/ready : 503 with a self-describing body whenever there is no live
                                      IBKR connection, so the pod leaves the Service endpoints
                                      and shows red.
Both the base manifest and every generated slice are checked: the slices are what Jenkins applies.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEP = "ibkr-feed-service"
CONTAINER = "ibkr-feed"
LIVE_PATH = "/health/live"
READY_PATH = "/health/ready"

BASE = ROOT / "k8s/base/ibkr-feed-deployment.yaml"
SLICES = (
    ROOT / "k8s/services/ibkr-feed/overlays/dev/manifest.yaml",
    ROOT / "k8s/services/ibkr-feed/overlays/production/manifest.yaml",
)


def container_of(path: Path) -> dict:
    """The ibkr-feed container spec from a manifest that may hold several documents."""
    for doc in yaml.safe_load_all(path.read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        if doc.get("metadata", {}).get("name") != DEP:
            continue
        for container in doc["spec"]["template"]["spec"]["containers"]:
            if container.get("name") == CONTAINER:
                return container
    raise AssertionError(f"{path}: no container {CONTAINER!r} in Deployment {DEP!r}")


class IbkrFeedReadinessProbe(unittest.TestCase):
    def manifests(self) -> list[Path]:
        return [BASE, *SLICES]

    def test_every_manifest_declares_both_probes(self):
        for path in self.manifests():
            with self.subTest(manifest=str(path.relative_to(ROOT))):
                container = container_of(path)
                self.assertIn("readinessProbe", container, "a readiness probe must be declared")
                self.assertIn("livenessProbe", container, "a liveness probe must be declared")

    def test_readiness_probe_asks_the_readiness_endpoint(self):
        for path in self.manifests():
            with self.subTest(manifest=str(path.relative_to(ROOT))):
                probe = container_of(path)["readinessProbe"]
                self.assertEqual(
                    READY_PATH,
                    probe["httpGet"]["path"],
                    "readiness must ask /health/ready; /health/live is 200 unconditionally and "
                    "reported this pod Ready through a total IBKR outage on 2026-11-21",
                )

    def test_liveness_probe_stays_on_the_liveness_endpoint(self):
        # Liveness must NOT move to /health/ready: a logged-out IB Gateway would then kill the pod
        # in a loop, and restarting cannot log a gateway in.
        for path in self.manifests():
            with self.subTest(manifest=str(path.relative_to(ROOT))):
                probe = container_of(path)["livenessProbe"]
                self.assertEqual(LIVE_PATH, probe["httpGet"]["path"])

    def test_the_two_probes_never_share_one_endpoint(self):
        """The defect in one assertion: readiness and liveness answering the same question."""
        for path in self.manifests():
            with self.subTest(manifest=str(path.relative_to(ROOT))):
                container = container_of(path)
                self.assertNotEqual(
                    container["livenessProbe"]["httpGet"]["path"],
                    container["readinessProbe"]["httpGet"]["path"],
                    "readiness and liveness must not resolve to the same endpoint",
                )

    def test_slices_agree_with_the_base_on_both_probe_paths(self):
        """The slices are what Jenkins applies; a base-only fix would not reach the cluster."""
        base = container_of(BASE)
        for path in SLICES:
            with self.subTest(manifest=str(path.relative_to(ROOT))):
                sliced = container_of(path)
                for kind in ("readinessProbe", "livenessProbe"):
                    self.assertEqual(
                        base[kind]["httpGet"]["path"],
                        sliced[kind]["httpGet"]["path"],
                        f"{kind} drifted between the base and the generated slice",
                    )

    def test_the_deploy_does_not_hang_on_the_owners_gateway_login(self):
        """Readiness now tracks the IB Gateway session, which is an owner action.

        A strict readiness-gated `rollout status` would hang for the full timeout and fail the
        deploy whenever the gateway is logged out (routine overnight and at weekends), so both
        deploy paths must bound that wait and say why. The image is still proven by
        verify-running-images.sh / the §13.3 digest gate, neither of which needs Ready pods.
        """
        apply_sh = (ROOT / "scripts/deploy/apply.sh").read_text()
        self.assertNotIn(
            "rollout status deployment/ibkr-feed-service --timeout=1260s",
            apply_sh,
            "the all-deploy must not block ~21 min on the owner's IB Gateway login",
        )
        self.assertIn("rollout status deployment/ibkr-feed-service --timeout=300s", apply_sh)

        service_deploy = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        self.assertIn(
            'IBKR_ROLLOUT_READY_WAIT',
            service_deploy,
            "service-deploy must bound the ibkr-feed readiness wait",
        )

    def test_ibkr_feed_image_is_still_proven_independently_of_readiness(self):
        """Tolerating an unready rollout is only safe while this mapping exists."""
        verify = (ROOT / "scripts/deploy/verify-running-images.sh").read_text()
        self.assertIn('("ibkr-feed-service", "ibkr-feed"): "IBKR_FEED_IMAGE"', verify)


if __name__ == "__main__":
    unittest.main()
