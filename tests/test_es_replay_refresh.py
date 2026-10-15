import copy
import hashlib
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
SOURCE = "c" * 40


class ReplayRefreshTest(unittest.TestCase):
    def exercise(self, replicas=1, strategy="Recreate", ready=True, restarts=0,
                 image_id=CHILD, fail_patch=False, fail_rollout=False,
                 action="refresh-es-replay", dry="false", subscription=True,
                 identity_failure=False, concurrent=False, malformed=False, rollback_conflicts=0,
                 pod_changed=False, env_missing=False, reconnect=False, logscroll=False, interrupt=False):
        calls = []
        image = "registry/options-edge-databento-feed@" + DIGEST
        before = {"metadata": {"resourceVersion": "123", "uid": "deployment-uid"},
            "spec": {"replicas": replicas, "strategy": {"type": strategy}, "template": {"spec": {"containers": [
                {"name": "es-feed", "image": "old", "env": [{"name": "EXISTING", "value": "preserved"}]}]}}}}
        deployment = copy.deepcopy(before)
        patch_count = 0
        pod_reads = 0
        log_reads = 0
        def run(*args):
            nonlocal patch_count, pod_reads, log_reads
            calls.append(args)
            if identity_failure and "scripts/es4/assert-cluster-identity.sh" in args:
                raise RuntimeError("wrong cluster")
            if "patch" in args:
                patch_count += 1
                if fail_patch and patch_count == 1:
                    raise RuntimeError("resourceVersion conflict")
                if patch_count > 1 and patch_count <= 1 + rollback_conflicts:
                    raise RuntimeError("rollback resourceVersion conflict")
                payload = json.loads(args[-1])
                self.assertEqual(payload["metadata"]["resourceVersion"], "123")
                self.assertNotIn("replicas", payload["spec"])
                if patch_count == 1:
                    selected = payload["spec"]["template"]["spec"]["containers"][0]
                    self.assertEqual(selected["env"], [{"name": "EXISTING", "value": "preserved"},
                        {"name": "DATABENTO_ES_TRADES_REPLAY_START_MINUTES", "value": "60"}])
                    self.assertEqual(selected["image"], image)
                deployment["spec"]["template"] = payload["spec"]["template"]
            if "rollout" in args and patch_count == 1 and interrupt:
                refresh.interrupted(15, None)
            if "rollout" in args and patch_count == 1 and fail_rollout:
                if concurrent:
                    deployment["spec"]["template"]["metadata"] = {"annotations": {"another": "rollout"}}
                raise RuntimeError("rollout timeout")
            if "get" in args and "deployment" in args:
                return json.dumps(deployment)
            if "get" in args and "pods" in args:
                pod_reads += 1
                pod_spec = copy.deepcopy(deployment["spec"]["template"]["spec"])
                if env_missing:
                    pod_spec["containers"][0]["env"] = []
                return json.dumps({"items": [{"metadata": {"name": "changed" if pod_changed and pod_reads > 1 else "new"}, "spec": pod_spec, "status": {
                    "containerStatuses": [{"name": "es-feed", "ready": ready,
                    "restartCount": restarts, "imageID": "runtime://" + image_id}]}}]})
            if "logs" in args:
                log_reads += 1
                if logscroll and log_reads > 1:
                    return ""
                if reconnect:
                    return "ES trades subscription: first\nES trades subscription: second"
                return 'ES trades subscription: replay_minutes=60 start=Some("2026-10-16T13:30:00Z")' if subscription else ""
            return ""
        env = {"ACTION": action, "DEPLOY_DRY_RUN": dry, "ES4_KUBECONFIG": "es4", "PROD_KUBECONFIG": "prod",
               "ES4_NS_UID": "es4-uid", "PROD_NS_UID": "prod-uid", "REGISTRY": "registry", "FEED_PERMITTED_SHA": SOURCE}
        with patch.dict(refresh.os.environ, env), patch.object(refresh, "run", run), \
             patch.object(refresh.Path, "read_text", return_value=json.dumps({"containerimage.digest": "bad" if malformed else DIGEST})), \
             patch.object(refresh, "verify_image", side_effect=RuntimeError("bad digest") if malformed else None, return_value=[DIGEST, CHILD]), \
             patch.object(refresh.time, "sleep"), patch("builtins.print"):
            try:
                refresh.main()
                result = None
            except (SystemExit, RuntimeError) as exc:
                result = str(exc)
        return result, calls, deployment, before

    def test_success_preserves_singleton_and_pins_digest(self):
        result, calls, _, _ = self.exercise()
        self.assertIsNone(result)
        self.assertEqual(sum("patch" in c for c in calls), 1)
        self.assertFalse(any("apply" in c for c in calls))
        self.assertEqual(sum("logs" in c for c in calls), 2)

    def test_inactive_or_rolling_feed_is_refused_before_mutation(self):
        for kwargs in ({"replicas": 0}, {"strategy": "RollingUpdate"}):
            result, calls, _, _ = self.exercise(**kwargs)
            self.assertIn("singleton", result)
            self.assertFalse(any("patch" in c for c in calls))

    def test_preconditions_refuse_before_mutation(self):
        for kwargs, message in [({"action": "deploy-all"}, "action required"),
                                ({"dry": "true"}, "DRY_RUN"), ({"identity_failure": True}, "wrong cluster"),
                                ({"malformed": True}, "bad digest")]:
            result, calls, _, _ = self.exercise(**kwargs)
            self.assertIn(message, result)
            self.assertFalse(any("patch" in c for c in calls))

    def test_runtime_failures_restore_exact_previous_template(self):
        for kwargs, message in [({"image_id": "sha256:" + "d" * 64}, "differs"),
                                ({"ready": False}, "not ready"), ({"restarts": 1}, "not ready"),
                                ({"fail_rollout": True}, "rollout timeout"),
                                ({"subscription": False}, "No 60-minute"), ({"pod_changed": True}, "pod changed"),
                                ({"env_missing": True}, "explicitly set"), ({"reconnect": True}, "reconnected"),
                                ({"interrupt": True}, "interrupted")]:
            result, calls, after, before = self.exercise(**kwargs)
            self.assertEqual(after, before, "rollback must restore the entire template including old env")
            self.assertIsNotNone(result)
            self.assertIn(message, result)
            self.assertEqual(sum("patch" in c for c in calls), 2)

    def test_log_line_remains_seen_after_it_scrolls_away(self):
        result, calls, _, _ = self.exercise(logscroll=True)
        self.assertIsNone(result)
        self.assertEqual(sum("patch" in c for c in calls), 1)

    def test_recovery_retries_version_conflicts(self):
        result, calls, after, before = self.exercise(fail_rollout=True, rollback_conflicts=2)
        self.assertEqual(after, before)
        self.assertIn("rollout timeout", result)
        self.assertEqual(sum("patch" in c for c in calls), 4)

    def test_initial_concurrent_update_stops_without_rollback(self):
        result, calls, _, _ = self.exercise(fail_patch=True)
        self.assertEqual(result, "resourceVersion conflict")
        self.assertFalse(any("rollout" in c for c in calls))
        self.assertEqual(sum("patch" in c for c in calls), 1)

    def test_recovery_never_overwrites_another_rollout(self):
        result, calls, after, _ = self.exercise(fail_rollout=True, concurrent=True)
        self.assertIn("Recovery refused", result)
        self.assertEqual(sum("patch" in c for c in calls), 1)
        self.assertIn("metadata", after["spec"]["template"])


class ImageProvenanceTest(unittest.TestCase):
    def exercise(self, single=False, revision=SOURCE, architecture="amd64", corrupt=False):
        blobs = {}
        def blob(value):
            body = json.dumps(value).encode()
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            blobs[digest] = body
            return digest
        config = blob({"os": "linux", "architecture": architecture, "config": {"Labels": {"org.opencontainers.image.revision": revision}}})
        child = blob({"config": {"digest": config}})
        root = child if single else blob({"manifests": [{"digest": child, "platform": {"os": "linux", "architecture": "amd64"}}]})
        def fetch(request, timeout):
            digest = request.full_url.rsplit("/", 1)[1]
            return io.BytesIO(b"{}" if corrupt else blobs[digest])
        with patch.object(refresh.urllib.request, "urlopen", fetch):
            return refresh.verify_image("registry", root, SOURCE)

    def test_real_malformed_digest_refuses_before_network(self):
        with patch.object(refresh.urllib.request, "urlopen") as network:
            with self.assertRaisesRegex(RuntimeError, "Missing immutable image digest"):
                refresh.verify_image("registry", "bad", SOURCE)
            network.assert_not_called()

    def test_index_and_single_manifest_both_bind_source(self):
        self.assertEqual(len(self.exercise()), 2)
        self.assertEqual(len(self.exercise(single=True)), 1)

    def test_wrong_source_platform_or_registry_bytes_are_refused(self):
        for kwargs, message in [({"revision": "wrong"}, "FEED_PERMITTED_SHA"),
                                ({"architecture": "arm64"}, "amd64"), ({"corrupt": True}, "digest")]:
            with self.assertRaisesRegex(RuntimeError, message):
                self.exercise(**kwargs)


if __name__ == "__main__":
    unittest.main()
