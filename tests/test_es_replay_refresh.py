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
                 pod_changed=False, env_missing=False, reconnect=False, logscroll=False, interrupt=False, caught_up=True, mismatch=False, recovery_get_failures=0, target="production", downstream_drain=True, promotion_fail=False, progress_advances=False):
        workload = "options-edge-databento-feed" if target == "dev" else "es-feed"
        container_name = "databento-feed" if target == "dev" else "es-feed"
        calls = []
        image = "registry/options-edge-databento-feed@" + DIGEST
        before = {"metadata": {"resourceVersion": "123", "uid": "deployment-uid"},
            "spec": {"replicas": replicas, "strategy": {"type": strategy}, "template": {"spec": {"containers": [
                {"name": container_name, "image": "old", "env": [{"name": "EXISTING", "value": "preserved"}]}]}}}}
        deployment = copy.deepcopy(before)
        patch_count = 0
        pod_reads = 0
        log_reads = 0
        recovery_reads = 0
        def run(*args):
            nonlocal patch_count, pod_reads, log_reads, recovery_reads
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
                if patch_count == 1 and fail_rollout:
                    recovery_reads += 1
                    if recovery_reads <= recovery_get_failures: raise RuntimeError("transient recovery GET")
                return json.dumps(deployment)
            if "get" in args and "pods" in args:
                pod_reads += 1
                pod_spec = copy.deepcopy(deployment["spec"]["template"]["spec"])
                if env_missing:
                    pod_spec["containers"][0]["env"] = []
                return json.dumps({"items": [{"metadata": {"name": "changed" if pod_changed and pod_reads > 1 else "new"}, "spec": pod_spec, "status": {
                    "containerStatuses": [{"name": container_name, "ready": ready,
                    "restartCount": restarts, "imageID": "runtime://" + image_id}]}}]})
            if "logs" in args:
                log_reads += 1
                if logscroll and log_reads > 1:
                    return ""
                if reconnect:
                    return "ES trades subscription: first\nES trades subscription: second"
                return ('ES trades subscription: replay_minutes=60 start=Some("2026-10-16T13:30:00Z")' if subscription else "") + ('\nES replay caught up: '+json.dumps({'expected_unique':20,'matched_unique':0 if mismatch else 20,'verified':not mismatch}) if caught_up else '')
            return ""
        env = {"REPLAY_TARGET":target,"DEV_KUBECONFIG":"dev","DEV_NS_UID":"dev-uid","ACTION": action, "DEPLOY_DRY_RUN": dry, "ES4_KUBECONFIG": "es4", "PROD_KUBECONFIG": "prod",
               "ES4_NS_UID": "es4-uid", "PROD_NS_UID": "prod-uid", "REGISTRY": "registry", "FEED_PERMITTED_SHA": SOURCE}
        downstream_reads=0
        health_function=refresh.downstream_healthy
        def downstream(*args):
            nonlocal downstream_reads
            downstream_reads+=1
            high,offset=(90,89) if downstream_reads==1 else (100,99) if downstream_reads==2 else (200,101)
            return {"pods":{},"highs":{"0":high},"groups":{"consumer":{"lag":high-offset,"offsets":{"0":offset},"state":"Stable"}}} if progress_advances else {"pods":{},"groups":{},"highs":{"0":100}}
        with patch.dict(refresh.os.environ, env), patch.object(refresh, "run", run), \
             patch.object(refresh.Path, "read_text", return_value=json.dumps({"containerimage.digest": "bad" if malformed else DIGEST})), \
             patch.object(refresh, "verify_image", side_effect=RuntimeError("bad digest") if malformed else None, return_value=[DIGEST, CHILD]), \
             patch.object(refresh.time, "sleep"), patch.object(refresh, "downstream_state", side_effect=downstream), patch.object(refresh,"downstream_healthy",side_effect=health_function if progress_advances else lambda *args:downstream_drain), patch.object(refresh, "promote_verified_image", side_effect=RuntimeError("registry unavailable") if promotion_fail else None) as promote, patch("builtins.print"):
            try:
                refresh.main()
                result = None
            except (SystemExit, RuntimeError) as exc:
                result = str(exc)
        self.assertEqual(promote.call_count, 1 if result is None or promotion_fail else 0)
        return result, calls, deployment, before

    def test_downstream_progress_uses_a_frozen_replay_fence(self):
        before={'pods':{'uid':2},'groups':{'consumer':{'lag':9,'state':'Stable','offsets':{'0':91}}}}
        fence={'0':100}
        after={'pods':{'uid':2},'groups':{'consumer':{'lag':20,'state':'Stable','offsets':{'0':101}}}}
        self.assertTrue(refresh.downstream_healthy(before,after,fence),'ongoing fresh lag must not undo drained replay')
        self.assertFalse(refresh.downstream_healthy(before,before,fence))
        self.assertFalse(refresh.downstream_healthy(before,{'pods':{'uid':2},'groups':{}},fence))
        with self.assertRaisesRegex(RuntimeError,'restart count changed'):
            refresh.downstream_healthy(before,{'pods':{'uid':3},'groups':{}},fence)

    def test_tag_failure_never_rolls_back_the_verified_feed(self):
        result,calls,after,before=self.exercise(promotion_fail=True)
        self.assertIn('No workload rollback',result)
        self.assertNotEqual(after,before)
        self.assertEqual(sum('patch' in c for c in calls),1)

    def test_rollout_freezes_the_replay_fence_while_live_offsets_keep_advancing(self):
        result,calls,_,_=self.exercise(progress_advances=True)
        self.assertIsNone(result)
        self.assertEqual(sum('patch' in c for c in calls),1)
        self.assertEqual(sum('logs' in c for c in calls),2)

    def test_success_preserves_singleton_and_pins_digest(self):
        result, calls, _, _ = self.exercise()
        self.assertIsNone(result)
        self.assertEqual(sum("patch" in c for c in calls), 1)
        self.assertFalse(any("apply" in c for c in calls))
        self.assertEqual(sum("logs" in c for c in calls), 2)

    def test_dev_routes_only_to_dev_native_feed(self):
        result,calls,_,_=self.exercise(target='dev')
        self.assertIsNone(result)
        writes=[c for c in calls if 'patch' in c]
        self.assertEqual(len(writes),1)
        self.assertIn('dev',writes[0]);self.assertIn('options-edge-databento-feed',writes[0])
        self.assertFalse(any('prod' in c or 'es4' in c for c in calls if c[0]=='kubectl'))

    def test_inactive_or_rolling_feed_is_refused_before_mutation(self):
        for kwargs in ({"replicas": 0}, {"strategy": "RollingUpdate"}):
            result, calls, _, _ = self.exercise(**kwargs)
            self.assertIn("singleton", result)
            self.assertFalse(any("patch" in c for c in calls))

    def test_preconditions_refuse_before_mutation(self):
        for kwargs, message in [({"action": "deploy-all"}, "action required"),
                                ({"dry": "true"}, "DRY_RUN"), ({"target":""},"Unknown replay target"), ({"identity_failure": True}, "wrong cluster"),
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
                                ({"interrupt": True}, "interrupted"), ({"caught_up": False}, "No completed"), ({"mismatch": True}, "identity verification failed"), ({"downstream_drain":False},"did not drain")]:
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

    def test_recovery_retries_transient_get_failures(self):
        result,calls,after,before=self.exercise(fail_rollout=True,recovery_get_failures=2)
        self.assertEqual(after,before)
        self.assertIn('rollout timeout',result)
        self.assertEqual(sum('patch' in c for c in calls),2)

    def test_initial_concurrent_update_stops_without_rollback(self):
        result, calls, _, _ = self.exercise(fail_patch=True)
        self.assertEqual(result, "resourceVersion conflict")
        self.assertEqual(sum("rollout" in c for c in calls),1)
        self.assertEqual(sum("patch" in c for c in calls), 1)

    def test_recovery_never_overwrites_another_rollout(self):
        result, calls, after, _ = self.exercise(fail_rollout=True, concurrent=True)
        self.assertIn("Recovery refused", result)
        self.assertEqual(sum("patch" in c for c in calls), 1)
        self.assertIn("metadata", after["spec"]["template"])


class ImageProvenanceTest(unittest.TestCase):
    def exercise(self, single=False, revision=SOURCE, architecture="amd64", corrupt=False, manifest_arch="amd64", expected_arch="amd64"):
        blobs = {}
        def blob(value):
            body = json.dumps(value).encode()
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            blobs[digest] = body
            return digest
        config = blob({"os": "linux", "architecture": architecture, "config": {"Labels": {"org.opencontainers.image.revision": revision}}})
        child = blob({"config": {"digest": config}})
        root = child if single else blob({"manifests": [{"digest": child, "platform": {"os": "linux", "architecture": manifest_arch}}]})
        def fetch(request, timeout):
            digest = request.full_url.rsplit("/", 1)[1]
            return io.BytesIO(b"{}" if corrupt else blobs[digest])
        with patch.object(refresh.urllib.request, "urlopen", fetch):
            return refresh.verify_image("registry", root, SOURCE, expected_arch)

    def test_promotion_preserves_exact_manifest_bytes(self):
        body = b'{ "schemaVersion":2, "manifests": [] }'
        digest = 'sha256:' + hashlib.sha256(body).hexdigest()
        calls = []
        class Reply(io.BytesIO):
            headers = {'Content-Type':'application/vnd.oci.image.index.v1+json'}
            status = 201
        def fetch(request, timeout):
            calls.append(request)
            return Reply(body)
        with patch.object(refresh.urllib.request, 'urlopen', fetch):
            refresh.promote_verified_image('registry',digest)
        self.assertEqual([r.get_method() for r in calls],['GET','PUT','GET'])
        self.assertEqual(calls[1].data, body)
        self.assertTrue(calls[1].full_url.endswith('/es-replay'))
        with patch.object(refresh.urllib.request, 'urlopen', return_value=Reply(b'corrupt')):
            with self.assertRaisesRegex(RuntimeError,'digest mismatch'):
                refresh.promote_verified_image('registry',digest)

    def test_real_malformed_digest_refuses_before_network(self):
        with patch.object(refresh.urllib.request, "urlopen") as network:
            with self.assertRaisesRegex(RuntimeError, "Missing immutable image digest"):
                refresh.verify_image("registry", "bad", SOURCE)
            network.assert_not_called()

    def test_index_and_single_manifest_both_bind_source(self):
        self.assertEqual(len(self.exercise()), 2)
        self.assertEqual(len(self.exercise(single=True)), 1)

    def test_native_dev_arm64_is_bound_to_the_target(self):
        self.assertEqual(len(self.exercise(architecture='arm64',manifest_arch='arm64',expected_arch='arm64')),2)
        with self.assertRaisesRegex(RuntimeError,'target-platform'):
            self.exercise(architecture='arm64',manifest_arch='arm64')

    def test_wrong_source_platform_or_registry_bytes_are_refused(self):
        for kwargs, message in [({"revision": "wrong"}, "FEED_PERMITTED_SHA"),
                                ({"architecture": "arm64"}, "amd64"), ({"corrupt": True}, "digest")]:
            with self.assertRaisesRegex(RuntimeError, message):
                self.exercise(**kwargs)


if __name__ == "__main__":
    unittest.main()
