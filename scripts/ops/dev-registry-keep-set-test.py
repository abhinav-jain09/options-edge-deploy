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


if __name__ == "__main__":
    unittest.main()
