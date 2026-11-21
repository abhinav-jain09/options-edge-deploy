# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers
# them, so this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which
# catches an import error) - none of its assertions execute. The marker is not approval of that:
# it is the classification that makes the dormancy visible. To promote this module, wire it into a
# step and change the marker to CI-REQUIRED; the guard then enforces the wiring.
"""The gateway's ES MBO relay (frames, detections, status for the /es-liquidity page) is ON for dev only.

The patch is the source, the generated slice is what service-deploy applies; a flag in one and not the other
looks configured and deploys off. Production never sets it: es-liquidity-engine-service is dev-only.
"""
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_PATCH = ROOT / "k8s/overlays/dev/feed-gateway-dev-env-patch.yaml"
DEV_SLICE = ROOT / "k8s/services/feed-gateway/overlays/dev/manifest.yaml"
SERVICES = ROOT / "services.yaml"
FLAG = "ES_MBO_RELAY_ENABLED"


def env_of(path: Path) -> dict:
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d.get("kind") == "Deployment"]
    assert len(docs) == 1, f"expected exactly one Deployment in {path}, got {len(docs)}"
    containers = docs[0]["spec"]["template"]["spec"]["containers"]
    for container in containers:
        env = {e["name"]: e.get("value") for e in container.get("env", [])}
        if FLAG in env:
            return env
    return {e["name"]: e.get("value") for e in containers[0].get("env", [])}


class RelayFlag(unittest.TestCase):
    def test_the_relay_is_on_for_dev(self):
        self.assertEqual("true", env_of(DEV_PATCH).get(FLAG))

    def test_the_generated_slice_carries_it_too(self):
        self.assertEqual("true", env_of(DEV_SLICE).get(FLAG))

    def test_production_does_not_set_it(self):
        for path in sorted((ROOT / "k8s/overlays/production").glob("*feed-gateway*.yaml")):
            self.assertNotIn(FLAG, path.read_text(), f"{path.name} must not set {FLAG}")
        prod_slice = ROOT / "k8s/services/feed-gateway/overlays/production/manifest.yaml"
        if prod_slice.is_file():
            self.assertNotIn(FLAG, prod_slice.read_text())

    def test_the_engine_it_relays_is_still_dev_only(self):
        line = [l for l in SERVICES.read_text().splitlines() if "name: es-liquidity-engine," in l]
        self.assertEqual(1, len(line), "exactly one es-liquidity-engine service entry")
        self.assertIn("envs: [dev]", line[0], "the engine is no longer dev-only: re-take the production relay decision")


if __name__ == "__main__":
    unittest.main()
