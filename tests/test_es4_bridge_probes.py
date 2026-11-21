# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers
# them, so this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which
# catches an import error) - none of its assertions execute. The marker is not approval of that:
# it is the classification that makes the dormancy visible. To promote this module, wire it into a
# step and change the marker to CI-REQUIRED; the guard then enforces the wiring.
"""The ES4 Rithmic bridge's startup and liveness probes are PROCESS-ONLY (probe.py --alive), in the repository manifest.

2026-11-20: the live Deployment carried the feed-gated `probe.py` as its startup probe, a failing first Rithmic login killed the pod every 120 s, every
restart was another login, and the restart budget stopped the bridge (depth and MBO silent for an hour). The live drift is caught by
scripts/watch/probe-drift.py; this pins the manifest side so the right value cannot be edited away.
"""
import unittest
from pathlib import Path

import yaml

MANIFEST = Path(__file__).resolve().parent.parent / "k8s/es4/services/rithmic-depth-bridge.yaml"


class BridgeProbes(unittest.TestCase):
    def containers(self):
        out = {}
        for d in yaml.safe_load_all(MANIFEST.read_text()):
            if d and d.get("kind") == "Deployment" and d["metadata"]["name"] == "rithmic-depth-bridge":
                for c in d["spec"]["template"]["spec"]["containers"]:
                    out[c["name"]] = c
        return out

    def test_startup_and_liveness_follow_the_process_never_the_feed(self):
        cs = self.containers()
        self.assertIn("collector", cs)
        for kind in ("startupProbe", "livenessProbe"):
            self.assertEqual(["python3", "/app/probe.py", "--alive"], cs["collector"][kind]["exec"]["command"],
                             "%s must be process-only: a feed-gated probe turns a failed Rithmic login into a kill loop" % kind)

    def test_readiness_is_the_feed(self):
        cs = self.containers()
        self.assertEqual(["python3", "/app/probe.py"], cs["collector"]["readinessProbe"]["exec"]["command"])


if __name__ == "__main__":
    unittest.main()
