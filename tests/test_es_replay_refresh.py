import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("refresh", ROOT / "scripts/es4/refresh-es-replay.py")
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)
DIGEST = "sha256:" + "a" * 64
CHILD = "sha256:" + "b" * 64


class ReplayRefreshTest(unittest.TestCase):
    def exercise(self, replicas=1, strategy="Recreate", ready=True, restarts=0,
                 image_id=CHILD, children=True, fail_patch=False, action="refresh-es-replay"):
        calls = []
        image = "registry/options-edge-databento-feed@" + DIGEST
        deployment = {"metadata": {"resourceVersion": "123"}, "spec": {"replicas": replicas,
            "strategy": {"type": strategy}, "template": {"spec": {"containers": [
                {"name": "es-feed", "image": "old"}]}}}}
        def run(*args):
            calls.append(args)
            if "patch" in args:
                if fail_patch:
                    raise RuntimeError("resourceVersion conflict")
                payload = json.loads(args[-1])
                self.assertEqual(payload["metadata"]["resourceVersion"], "123")
                self.assertNotIn("replicas", payload["spec"])
                container = payload["spec"]["template"]["spec"]["containers"][0]
                self.assertEqual(container["env"], [{"name": "DATABENTO_ES_TRADES_REPLAY_START_MINUTES", "value": "60"}])
                self.assertEqual(container["image"], image)
                deployment["spec"]["template"]["spec"]["containers"][0]["image"] = image
            if "get" in args and "deployment" in args:
                return json.dumps(deployment)
            if "get" in args and "pods" in args:
                return json.dumps({"items": [{"metadata": {"name": "new"}, "status": {
                    "containerStatuses": [{"name": "es-feed", "ready": ready,
                    "restartCount": restarts, "imageID": "runtime://" + image_id}]}}]})
            return ""
        index = {"manifests": [{"digest": CHILD, "platform": {"os": "linux", "architecture": "amd64"}}] if children else []}
        response = io.BytesIO(json.dumps(index).encode())
        env = {"ACTION": action, "DEPLOY_DRY_RUN": "false", "ES4_KUBECONFIG": "es4", "PROD_KUBECONFIG": "prod",
               "ES4_NS_UID": "es4-uid", "PROD_NS_UID": "prod-uid", "REGISTRY": "registry"}
        with patch.dict(refresh.os.environ, env), patch.object(refresh, "run", run), \
             patch.object(refresh.Path, "read_text", return_value=json.dumps({"containerimage.digest": DIGEST})), \
             patch.object(refresh.urllib.request, "urlopen", return_value=response):
            try:
                refresh.main()
                result = None
            except (SystemExit, RuntimeError) as exc:
                result = str(exc)
        return result, calls

    def test_success_preserves_singleton_and_pins_digest(self):
        result, calls = self.exercise()
        self.assertIsNone(result)
        self.assertEqual([c[5] for c in calls if c[0] == "kubectl" and c[5] in ("apply", "patch")], ["apply", "patch"])

    def test_inactive_or_rolling_feed_is_refused_before_mutation(self):
        for kwargs in ({"replicas": 0}, {"strategy": "RollingUpdate"}):
            result, calls = self.exercise(**kwargs)
            self.assertIn("singleton", result)
            self.assertFalse(any("apply" in c or "patch" in c for c in calls))

    def test_wrong_action_is_refused(self):
        result, calls = self.exercise(action="deploy-all")
        self.assertIn("action required", result)
        self.assertEqual(calls, [])

    def test_missing_platform_is_refused_before_mutation(self):
        result, calls = self.exercise(children=False)
        self.assertIn("amd64", result)
        self.assertFalse(any("apply" in c or "patch" in c for c in calls))

    def test_runtime_identity_and_health_are_enforced(self):
        for kwargs, message in [({"image_id": "sha256:" + "c" * 64}, "differs"),
                                ({"ready": False}, "not ready"), ({"restarts": 1}, "not ready")]:
            result, _ = self.exercise(**kwargs)
            self.assertIn(message, result)

    def test_concurrent_update_stops_rollout(self):
        result, calls = self.exercise(fail_patch=True)
        self.assertEqual(result, "resourceVersion conflict")
        self.assertFalse(any("rollout" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
