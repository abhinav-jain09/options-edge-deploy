#!/usr/bin/env python3
"""The prune's keep set must cover EVERY container, or a prune deletes an image the cluster uses."""
import json, sys, unittest
from pathlib import Path

import importlib.util
spec = importlib.util.spec_from_file_location(
    "keepset", Path(__file__).resolve().parent / "dev-registry-keep-set.py")
keepset = importlib.util.module_from_spec(spec); spec.loader.exec_module(keepset)

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64


def deploy(init=None, main=None):
    return {"items": [{"spec": {"template": {"spec": {
        "initContainers": [{"image": i} for i in (init or [])],
        "containers": [{"image": m} for m in (main or [])]}}}}]}


class KeepSet(unittest.TestCase):
    def test_an_init_containers_digest_is_kept(self):
        """THE regression. options-edge-web pins its init container to its own digest; reading only
        containers[0] left that digest out of the keep set, the GC swept its blobs, and the pod went
        ImagePullBackOff on an image the cluster was using."""
        obj = deploy(init=[f"reg/web:dev@{D1}"], main=[f"reg/web:dev@{D2}"])
        self.assertIn(D1, keepset.keep_digests(obj))
        self.assertIn(D2, keepset.keep_digests(obj))

    def test_a_sidecars_digest_is_kept(self):
        obj = deploy(main=[f"reg/app:dev@{D1}", f"reg/sidecar:dev@{D2}"])
        self.assertEqual([D1, D2], keepset.keep_digests(obj))

    def test_statefulsets_daemonsets_and_jobs_count_too(self):
        obj = {"items": [
            {"spec": {"template": {"spec": {"containers": [{"image": f"r/a@{D1}"}]}}}},
            {"spec": {"jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"image": f"r/b@{D2}"}]}}}}}},
        ]}
        got = keepset.keep_digests(obj)
        self.assertIn(D1, got)
        self.assertIn(D2, got, "a CronJob nests one level deeper and is easy to miss")

    def test_an_unpinned_image_contributes_nothing(self):
        """A tag-only reference names no digest. It must not become an empty or bogus keep entry."""
        self.assertEqual([], keepset.keep_digests(deploy(main=["reg/app:dev"])))

    def test_duplicates_collapse_and_order_is_stable(self):
        obj = deploy(init=[f"r/a@{D2}"], main=[f"r/b@{D1}", f"r/c@{D2}"])
        self.assertEqual([D2, D1], keepset.keep_digests(obj))

    def test_nothing_running_yields_an_empty_set_not_a_crash(self):
        for empty in ({"items": []}, {}, {"items": [{"spec": {}}]}):
            self.assertEqual([], keepset.keep_digests(empty))

    def test_a_container_with_no_image_is_skipped(self):
        obj = {"items": [{"spec": {"template": {"spec": {
            "containers": [{}, {"image": f"r/a@{D3}"}]}}}}]}
        self.assertEqual([D3], keepset.keep_digests(obj))


class TagPins(unittest.TestCase):
    def test_a_workload_pinned_by_tag_alone_is_a_tag_pin(self):
        """No digest in the spec, so the digest set cannot protect it: the manifest its tag resolves
        to is live. The prune used to delete it."""
        obj = deploy(main=["host.docker.internal:5001/svc/app:2227-abc"])
        self.assertEqual([("svc/app", "2227-abc")], keepset.keep_tags(obj))

    def test_a_digest_pinned_ref_is_not_also_a_tag_pin(self):
        obj = deploy(main=[f"host.docker.internal:5001/app:dev@{D1}"])
        self.assertEqual([], keepset.keep_tags(obj))

    def test_a_bare_repo_is_latest_and_an_init_container_counts(self):
        obj = deploy(init=["host.docker.internal:5001/web-assets"], main=["localhost/x:t"])
        self.assertEqual([("web-assets", "latest"), ("x", "t")], keepset.keep_tags(obj))

    def test_split_ref_handles_registry_port_tag_and_digest(self):
        sr = keepset.split_ref
        self.assertEqual(("a/b", "t", D1), sr(f"host.docker.internal:5001/a/b:t@{D1}"))
        self.assertEqual(("a/b", None, D1), sr(f"host.docker.internal:5001/a/b@{D1}"))
        self.assertEqual(("a/b", "t", None), sr("host.docker.internal:5001/a/b:t"))
        self.assertEqual(("a/b", None, None), sr("host.docker.internal:5001/a/b"))
        self.assertEqual(("img", "1", None), sr("img:1"))

    def test_a_non_sha256_digest_pin_is_kept_not_ignored(self):
        """keep_tags skips any digest-bearing ref, so a digest the keep set cannot READ is a pin
        nothing protects."""
        d512 = "sha512:" + "a" * 128
        self.assertEqual([d512], keepset.keep_digests(deploy(main=[f"r.io:5001/a:old@{d512}"])))

    def test_the_cli_prints_digests_then_tag_pins(self):
        import subprocess
        obj = deploy(main=[f"r.io:5001/a@{D1}", "r.io:5001/b:v1"])
        out = subprocess.run([sys.executable, str(Path(keepset.__file__ or spec.origin))], input=json.dumps(obj),
                             capture_output=True, text=True).stdout.split("\n")
        self.assertIn(D1, out)
        self.assertIn("tag b v1", out)


if __name__ == "__main__":
    unittest.main()
