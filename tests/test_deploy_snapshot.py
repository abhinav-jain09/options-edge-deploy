# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""scripts/ops/deploy-snapshot.sh: the read-only before/after record that proves a roll touched one workload. Fixtures only; nothing here talks to a cluster."""
from __future__ import annotations

import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ops/deploy-snapshot.sh"


def workload(kind, name, generation=3, observed=3, image="host.docker.internal:5001/options-edge-x@sha256:aaa"):
    return {"kind": kind, "metadata": {"name": name, "generation": generation}, "status": {"observedGeneration": observed},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/name": name}}, "template": {"spec": {"containers": [{"name": name, "image": image}]}}}}


def pod(name, owner, uid, restarts=0, image_id="sha256:aaa"):
    return {"kind": "Pod", "metadata": {"name": name, "uid": uid, "labels": {"app.kubernetes.io/name": owner}},
            "status": {"containerStatuses": [{"name": owner, "restartCount": restarts, "imageID": image_id}]}}


def cluster():
    return {"kind": "List", "items": [
        workload("Deployment", "early-auction-verdict-service", generation=7, observed=7, image="host.docker.internal:5001/options-edge-early-auction-verdict@sha256:605c"),
        workload("Deployment", "es-liquidity-engine-service"),
        workload("StatefulSet", "kafka-ui"),
        pod("eav-1", "early-auction-verdict-service", "uid-eav", restarts=0, image_id="sha256:605c"),
        pod("eng-1", "es-liquidity-engine-service", "uid-eng", restarts=1),
        pod("ui-1", "kafka-ui", "uid-ui"),
    ]}


def run(*args, stdin=None, env=None):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(["bash", str(SCRIPT), *args], input=stdin, capture_output=True, text=True, env=e)


def summary(doc):
    r = run("summarize", stdin=json.dumps(doc))
    assert r.returncode == 0, r.stderr
    return r.stdout


class Summarize(unittest.TestCase):
    def test_one_sorted_line_per_workload_with_generation_images_and_pods(self):
        out = summary(cluster()).splitlines()
        self.assertEqual([l.split("\t")[0] for l in out], ["Deployment/early-auction-verdict-service", "Deployment/es-liquidity-engine-service", "StatefulSet/kafka-ui"])
        eav = out[0].split("\t")
        self.assertEqual(eav[1:3], ["generation=7", "observedGeneration=7"])
        self.assertIn("@sha256:605c", eav[3])
        self.assertEqual(eav[4], "pods=uid-eav:0:sha256:605c")
        self.assertEqual(out[1].split("\t")[4], "pods=uid-eng:1:sha256:aaa")

    def test_the_summary_is_deterministic_whatever_the_input_order(self):
        a = cluster()
        b = copy.deepcopy(a)
        b["items"].reverse()
        self.assertEqual(summary(a), summary(b))

    def test_a_restart_a_new_pod_a_new_generation_or_a_new_image_each_change_the_line(self):
        base = summary(cluster())
        for label, mutate in {
            "restart": lambda d: d["items"][3]["status"]["containerStatuses"][0].update(restartCount=1),
            "pod replaced": lambda d: d["items"][3]["metadata"].update(uid="uid-other"),
            "generation": lambda d: d["items"][0]["metadata"].update(generation=8),
            "observed": lambda d: d["items"][0]["status"].update(observedGeneration=6),
            "spec image": lambda d: d["items"][0]["spec"]["template"]["spec"]["containers"][0].update(image="x@sha256:bbb"),
            "running image id": lambda d: d["items"][3]["status"]["containerStatuses"][0].update(imageID="sha256:ccc"),
        }.items():
            d = cluster()
            mutate(d)
            self.assertNotEqual(summary(d), base, label)


class Compare(unittest.TestCase):
    def files(self, before, after):
        tmp = Path(tempfile.mkdtemp())
        (tmp / "b").write_text(summary(before))
        (tmp / "a").write_text(summary(after))
        return str(tmp / "b"), str(tmp / "a")

    def with_new_service(self):
        d = cluster()
        d["items"] += [workload("Deployment", "liquidity-validator-service", generation=1, observed=1), pod("lv-1", "liquidity-validator-service", "uid-lv")]
        return d

    def test_only_the_new_service_appearing_passes(self):
        b, a = self.files(cluster(), self.with_new_service())
        r = run("compare", b, a)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("OK:", r.stdout)

    def test_the_new_service_changing_between_two_snapshots_passes(self):
        x = self.with_new_service()
        y = copy.deepcopy(x)
        y["items"][-1]["metadata"]["uid"] = "uid-lv-2"
        y["items"][-2]["metadata"]["generation"] = 2
        b, a = self.files(x, y)
        self.assertEqual(run("compare", b, a).returncode, 0)

    def test_any_other_workload_changing_fails_and_is_named(self):
        for label, mutate in {
            "eav restarted": lambda d: d["items"][3]["status"]["containerStatuses"][0].update(restartCount=1),
            "eav pod replaced": lambda d: d["items"][3]["metadata"].update(uid="uid-new"),
            "eav re-rolled": lambda d: d["items"][0]["metadata"].update(generation=8),
            "engine image": lambda d: d["items"][1]["spec"]["template"]["spec"]["containers"][0].update(image="y@sha256:ddd"),
            "statefulset": lambda d: d["items"][2]["metadata"].update(generation=4),
        }.items():
            after = self.with_new_service()
            mutate(after)
            b, a = self.files(cluster(), after)
            r = run("compare", b, a)
            self.assertEqual(r.returncode, 1, label + r.stdout)
            self.assertIn("FAIL", r.stdout, label)
            self.assertTrue(any(n in r.stdout for n in ("early-auction-verdict-service", "es-liquidity-engine-service", "kafka-ui")), label)
            self.assertNotIn("liquidity-validator-service changed", r.stdout, label)

    def test_another_workload_appearing_or_vanishing_fails(self):
        extra = self.with_new_service()
        extra["items"].append(workload("Deployment", "something-else"))
        b, a = self.files(cluster(), extra)
        r = run("compare", b, a)
        self.assertEqual(r.returncode, 1)
        self.assertIn("something-else appeared", r.stdout)
        gone = self.with_new_service()
        gone["items"] = [i for i in gone["items"] if i["metadata"]["name"] != "kafka-ui"]
        b, a = self.files(cluster(), gone)
        r = run("compare", b, a)
        self.assertEqual(r.returncode, 1)
        self.assertIn("StatefulSet/kafka-ui vanished", r.stdout)

    def test_an_empty_snapshot_proves_nothing_and_a_missing_file_is_an_error(self):
        tmp = Path(tempfile.mkdtemp())
        (tmp / "empty").write_text("")
        (tmp / "ok").write_text(summary(cluster()))
        self.assertEqual(run("compare", str(tmp / "empty"), str(tmp / "ok")).returncode, 1)
        self.assertEqual(run("compare", str(tmp / "ok"), str(tmp / "missing")).returncode, 2)

    def test_the_allowed_workload_can_be_named(self):
        b, a = self.files(cluster(), cluster())
        changed = cluster()
        changed["items"][1]["metadata"]["generation"] = 9
        b2, a2 = self.files(cluster(), changed)
        self.assertEqual(run("compare", b2, a2).returncode, 1)
        self.assertEqual(run("compare", b2, a2, "es-liquidity-engine-service").returncode, 0)


class Capture(unittest.TestCase):
    def test_capture_makes_exactly_one_read_only_get_and_writes_the_summary(self):
        tmp = Path(tempfile.mkdtemp())
        fake = tmp / "kubectl"
        fixture = tmp / "doc.json"
        fixture.write_text(json.dumps(cluster()))
        log = tmp / "calls"
        fake.write_text(f'#!/usr/bin/env bash\necho "$*" >> "{log}"\ncat "{fixture}"\n')
        fake.chmod(0o755)
        out = tmp / "snap"
        r = run("capture", str(out), env={"KUBECTL": str(fake), "NAMESPACE": "options-edge"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(log.read_text().splitlines(), ["get deploy,sts,pods -n options-edge -o json"])
        self.assertEqual(out.read_text(), summary(cluster()))
        text = SCRIPT.read_text()
        for verb in ("apply", "delete", "patch", "scale", "rollout", "edit", "replace", "create"):
            self.assertNotRegex(text, rf'"\$KUBECTL" {verb}\b', verb)


if __name__ == "__main__":
    unittest.main()
