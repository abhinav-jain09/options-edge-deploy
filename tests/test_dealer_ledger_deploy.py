# CI-REQUIRED
#
# This marker is not decoration: scripts/ci/validate-tests-are-collectable.sh - a separate, already-wired
# step of deploy-validation.yml - fails the build unless a mandatory step of that workflow runs this
# module through `python3 -m unittest`. Without it this module is an orphan: the workflow names its test
# modules by hand and nothing discovers them, so the collectability step would IMPORT this file (catching
# an import error) while not one of its assertions ever executed. Measured when the marker was added:
# 138 modules under tests/, 26 named by the workflow, 112 never run.
#
# It matters most for test_dev_reserves_no_cpu_for_the_three_while_prod_and_es4_keep_theirs, which holds a
# position the owner stated outright - "let it use the cpu as much it want dont restrict anything" - and
# which, unwired, would have held nothing.
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class DealerLedgerDeployTest(unittest.TestCase):
    def test_dealer_ledger_kubernetes_resources_are_registered(self):
        kustomization = (ROOT / "k8s" / "base" / "kustomization.yaml").read_text()
        deployment = (ROOT / "k8s" / "base" / "dealer-ledger-deployment.yaml").read_text()
        service = (ROOT / "k8s" / "base" / "dealer-ledger-service.yaml").read_text()
        dev_overlay = (ROOT / "k8s" / "overlays" / "dev" / "kustomization.yaml").read_text()
        experiment_overlay = (ROOT / "k8s" / "overlays" / "experiment" / "kustomization.yaml").read_text()

        self.assertIn("dealer-ledger-deployment.yaml", kustomization)
        self.assertIn("dealer-ledger-service.yaml", kustomization)
        self.assertIn("name: dealer-ledger-service", deployment)
        self.assertIn("name: dealer-ledger\n", deployment)
        self.assertIn("options-edge-dealer-ledger:dev", deployment)
        self.assertIn("DEALER_LEDGER_INPUT_STRIKE_FLOW_TOPIC", deployment)
        self.assertIn("options.databento.strike-flow", deployment)
        self.assertIn("DEALER_LEDGER_INPUT_GREEKS_TOPIC", deployment)
        self.assertIn("options.databento.gex.strike", deployment)
        self.assertIn("DEALER_LEDGER_OUTPUT_STATE_TOPIC", deployment)
        self.assertIn("dealer-ledger-state", deployment)
        # SHADOW-visible in ALL envs (base, env-independent so dev==prod): the signal is computed + shown
        # but NEVER advisory while uncalibrated. ADVISORY stays false; UI_VISIBLE true; quality SHADOW.
        self.assertIn('name: DEALER_LEDGER_ADVISORY_ENABLED', deployment)
        self.assertIn('name: DEALER_LEDGER_UI_VISIBLE', deployment)
        self.assertIn("SHADOW", deployment)
        # Advisory must remain OFF even when visible — an uncalibrated signal is never auto-tradeable.
        self.assertRegex(deployment, r"DEALER_LEDGER_ADVISORY_ENABLED[\s\S]*?value:\s*\"false\"")
        self.assertIn("port: 8113", service)
        self.assertIn("host.docker.internal:5001/options-edge-dealer-ledger", dev_overlay)
        self.assertIn("host.docker.internal:5001/options-edge-dealer-ledger", experiment_overlay)

    def test_dealer_ledger_image_is_resolved_and_deployed(self):
        jenkinsfile = (ROOT / "Jenkinsfile").read_text()
        resolve_images = (ROOT / "scripts" / "deploy" / "resolve-images.sh").read_text()
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        preflight = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        monitoring = (ROOT / "scripts" / "monitoring" / "apply-prometheus-scrapes.sh").read_text()
        dev_tags = (ROOT / "image-tags" / "dev.yaml").read_text()
        prod_tags = (ROOT / "image-tags" / "production.yaml").read_text()

        self.assertIn("string(name: 'DEALER_LEDGER_IMAGE'", jenkinsfile)
        self.assertIn("'DEALER_LEDGER_IMAGE': 'dealer-ledger'", jenkinsfile)
        self.assertRegex(jenkinsfile, r"'DEALER_LEDGER_IMAGE',\s*'DEALER_LEDGER_CALIBRATION_IMAGE'")
        self.assertIn("'dealer-ledger-service'", jenkinsfile)  # DEPLOY_TARGET choice
        self.assertIn("options-edge-dealer-ledger:$image_tag", resolve_images)
        self.assertIn("DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE", resolve_images)
        self.assertIn('deployment/dealer-ledger-service dealer-ledger="$DEALER_LEDGER_IMAGE"', apply_script)
        self.assertIn("rollout restart deployment/dealer-ledger-service", apply_script)
        self.assertIn("rollout status deployment/dealer-ledger-service", apply_script)
        self.assertIn("dealer-ledger-service)", apply_script)  # targeted-deploy case
        self.assertIn("DEALER_LEDGER_IMAGE=$DEALER_LEDGER_IMAGE", preflight)
        self.assertIn("add_service_scrape dealer-ledger-service 8113", monitoring)
        # image-tags consistency guard: mapped in dev MUST be mapped in production too. All three, since
        # the ledger cannot be judged without its calibration pair.
        for name, image in (("dealer-ledger-service", "options-edge-dealer-ledger"),
                            ("dealer-ledger-calibration-scorer", "options-edge-dealer-ledger-calibration"),
                            ("dealer-ledger-calibration-accumulator", "options-edge-dealer-ledger-calibration")):
            self.assertIn("%s: 192.168.100.252:5000/%s:dev" % (name, image), dev_tags)
            self.assertIn("%s: 192.168.100.252:5000/%s:prod" % (name, image), prod_tags)

    def test_the_targeted_deploy_carries_the_WHOLE_calibration_loop(self):
        """DEPLOY_TARGET=dealer-ledger-service must deploy all three, not the ledger alone.

        The ledger emits dealer-ledger-signal-fired, the scorer turns outcomes into
        dealer-ledger-outcome-scored, the accumulator aggregates them into
        dealer-ledger-calibration-state, and the ledger reads that back through a global store. A target
        that applied the ledger alone left its calibration store permanently empty - indistinguishable
        from a broken one - while every other assertion in this file still passed. Codex found it on the
        dev-enable review; this is the assertion that would have caught it.
        """
        apply_script = (ROOT / "scripts" / "deploy" / "apply.sh").read_text()
        # Scoped to the dealer-ledger CASE BLOCK: apply.sh is thousands of lines and these names appear in
        # the full-stack path too, so an unscoped grep would pass on a ledger-only target.
        start = apply_script.index("            dealer-ledger-service)")
        end = apply_script.index("            strike-liquidity-heatmap-service)", start)
        block = apply_script[start:end]

        for name in ("dealer-ledger-service", "dealer-ledger-calibration-scorer",
                     "dealer-ledger-calibration-accumulator"):
            self.assertIn('or (.kind == "Deployment" and .metadata.name == "%s")' % name, block,
                          "the targeted render must SELECT %s (the `or (...)` filter entry, which is "
                          "distinct from the yq rewrite line that repeats the same predicate)" % name)
            self.assertIn("rollout status deployment/%s" % name, block,
                          "the targeted deploy must wait for %s to roll out" % name)
        # Both images digest-pinned before any mutation: an unpinned calibration image would otherwise
        # reach the render and only the generic unpinned check would stop it, after the pin step.
        for var in ("DEALER_LEDGER_IMAGE", "DEALER_LEDGER_CALIBRATION_IMAGE"):
            self.assertIn('%s="$(pin_ref "$%s")"' % (var, var), block,
                          "%s must be digest-pinned in the targeted path" % var)
        # The container each image is written onto, which is NOT the deployment name for the ledger.
        self.assertIn('select(.name == "dealer-ledger").image) = strenv(DEALER_LEDGER_IMAGE)', block)
        for dep in ("dealer-ledger-calibration-scorer", "dealer-ledger-calibration-accumulator"):
            self.assertIn('select(.name == "%s").image) = strenv(DEALER_LEDGER_CALIBRATION_IMAGE)' % dep,
                          block, "the %s container image must be rewritten" % dep)
        # The two DOWNSTREAM obligations of a three-deployment target, both missed first time round and
        # both found by Codex: a target that applies three images must LOCK all of them and VERIFY all of
        # them, or a "successful" deploy can leave a stale calibration image running under an enforced lock.
        lock = (ROOT / "scripts" / "deploy" / "image-lock.sh").read_text()
        lock_start = lock.index("target_image_vars()")
        lock_block = lock[lock_start: lock.index("image_lock_key_allowed", lock_start)]
        self.assertIn("dealer-ledger-service)", lock_block,
                      "image-lock.sh must handle DEPLOY_TARGET=dealer-ledger-service; without a case it "
                      "falls to 'Unsupported DEPLOY_TARGET' and enforces NEITHER ledger image")
        for var in ("DEALER_LEDGER_IMAGE", "DEALER_LEDGER_CALIBRATION_IMAGE"):
            self.assertRegex(lock_block, r"dealer-ledger-service\)[\s\S]*?%s" % var,
                             "the dealer target's lock must cover %s" % var)

        pre = (ROOT / "scripts" / "deploy" / "image-preflight.sh").read_text()
        pstart = pre.index("            dealer-ledger-service)")
        pblock = pre[pstart: pre.index("            strike-liquidity-heatmap-service)", pstart)]
        for var in ("DEALER_LEDGER_IMAGE", "DEALER_LEDGER_CALIBRATION_IMAGE"):
            self.assertIn("%s=$%s" % (var, var), pblock,
                          "image-preflight.sh must check %s for this target, or a missing calibration "
                          "image is found only after the ledger has already rolled" % var)

        verify = (ROOT / "scripts" / "deploy" / "verify-running-images.sh").read_text()
        vstart = verify.index('deploy_target == "dealer-ledger-service"')
        vblock = verify[vstart: verify.index("elif", vstart + 10)]
        for dep in ("dealer-ledger-service", "dealer-ledger-calibration-scorer",
                    "dealer-ledger-calibration-accumulator"):
            self.assertIn('"%s"' % dep, vblock,
                          "post-rollout verification of the dealer target must include %s" % dep)

        # Non-vacuity: if the block extraction ever caught nothing, every assertion above would be
        # trivially satisfied by an empty string containing nothing - assertIn would fail, but a future
        # refactor to a regex search could pass. Say the size out loud.
        self.assertGreater(len(block), 1000, "the dealer-ledger case block was not extracted")

    def test_the_three_are_enabled_on_dev_and_held_everywhere_else(self):
        """Dev runs the loop; production and es4 keep it inert. Both hold mechanisms are checked.

        replicas alone is not the switch: dev-cleanup.sh's DISABLED_DEV re-zeroes anything it names on the
        next bring-up, so a dev enable that changed only replicas would be undone. And the base must stay
        0, because it governs production and es4 too.
        """
        import re
        dev_cleanup = (ROOT / "scripts" / "ops" / "dev-cleanup.sh").read_text()
        disabled = re.search(r"^DISABLED_DEV='([^']*)'$", dev_cleanup, re.M)
        self.assertIsNotNone(disabled, "dev-cleanup.sh must carry exactly one DISABLED_DEV line")
        held = set(disabled.group(1).split("|"))
        self.assertTrue(held, "the DISABLED_DEV list parsed empty - this check would pass on anything")

        trio = ("dealer-ledger-service", "dealer-ledger-calibration-scorer",
                "dealer-ledger-calibration-accumulator")
        for name in trio:
            self.assertNotIn(name, held, "%s must NOT be held down on dev any more" % name)

        import yaml
        def replicas(path):
            out = {}
            for doc in yaml.safe_load_all((ROOT / path).read_text()):
                if doc and doc.get("kind") == "Deployment":
                    out[doc["metadata"]["name"]] = doc["spec"].get("replicas")
            return out

        dev = replicas("k8s/services/dealer-ledger/overlays/dev/manifest.yaml")
        dev.update(replicas("k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml"))
        prod = replicas("k8s/services/dealer-ledger/overlays/production/manifest.yaml")
        prod.update(replicas("k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml"))
        for name in trio:
            self.assertEqual(1, dev.get(name), "%s must be enabled (replicas 1) on dev" % name)
            self.assertEqual(0, prod.get(name), "%s must stay inert (replicas 0) in production" % name)

        # es4 too: the test is named "everywhere else", so it has to check es4 and not just production
        # (Codex minor). es4 renders these from k8s/es4/services/.
        es4 = replicas("k8s/es4/services/dealer-ledger.yaml")
        es4.update(replicas("k8s/es4/services/dealer-ledger-calibration.yaml"))
        for name in trio:
            self.assertEqual(0, es4.get(name), "%s must stay inert (replicas 0) on es4" % name)

        # Production's two bring-up paths must BOTH still hold them, or the next prod reboot starts them.
        for script in ("scripts/ops/morning-autostart.sh", "scripts/ops/oe-boot-bringup.sh"):
            text = (ROOT / script).read_text()
            for name in trio:
                self.assertIn(name, text, "%s must stay in %s's KEEP_DOWN" % (name, script))


    def test_dev_reserves_no_cpu_for_the_three_while_prod_and_es4_keep_theirs(self):
        """Owner 2026-11-08: "let it use the cpu as much it want dont restrict anything."

        A cpu REQUEST is a reservation used for scheduling, not a cap. The base reserves 2 whole CPUs for
        the ledger, and the dev node is a 16-core Mac already at ~16/16 reserved, so that reservation could
        never be satisfied: the pods sat `Pending: Insufficient cpu` and got ZERO cpu. Removing the request
        on dev lets them schedule, and because there is no cpu LIMIT anywhere they can then use as much as
        they want - which is what was asked for.

        This is asserted in BOTH dev paths because they are independent sources of truth: the kustomize
        overlay feeds the full-stack apply, while service-deploy.sh reads k8s/services/*/overlays/dev/
        manifest.yaml. A change to one alone would be reverted by a deploy through the other.

        Production, es4 and experiment must KEEP their reservations and stay at replicas 0 - they run on
        real hardware where a reservation is how the ledger is guaranteed its 2 CPUs. All three
        non-dev environments are asserted, so a dev-only change cannot silently widen.
        """
        import yaml

        trio = {"dealer-ledger": "dealer-ledger-service",
                "dealer-ledger-calibration-scorer": "dealer-ledger-calibration-scorer",
                "dealer-ledger-calibration-accumulator": "dealer-ledger-calibration-accumulator"}

        def cpu_requests(paths):
            """container name -> (cpu request or 'NONE', deployment replicas), across the given manifests.

            Replicas come back alongside the request because the blast-radius claim this test makes is
            BOTH "the other environments keep their reservations" AND "they stay inert" - Codex found that
            an es4 enablement would have passed the first version of this test, which read only the cpu.
            """
            found = {}
            for rel in paths:
                for doc in yaml.safe_load_all((ROOT / rel).read_text()):
                    if not doc or doc.get("kind") != "Deployment":
                        continue
                    for c in doc["spec"]["template"]["spec"]["containers"]:
                        if c["name"] in trio:
                            req = ((c.get("resources") or {}).get("requests") or {})
                            found[c["name"]] = (req.get("cpu", "NONE"), doc["spec"].get("replicas"))
            return found

        dev_service = cpu_requests([
            "k8s/services/dealer-ledger/overlays/dev/manifest.yaml",
            "k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml",
        ])
        # Non-vacuity: all three containers must actually have been located, or an empty dict would make
        # every "no cpu request" assertion below trivially true.
        self.assertEqual(set(trio), set(dev_service),
                         "the dev per-service manifests must define all three containers")
        for name in trio:
            cpu, replicas = dev_service[name]
            self.assertEqual("NONE", cpu,
                             "%s must reserve NO cpu on dev (owner: do not restrict it); a reservation it "
                             "cannot get leaves it Pending with zero cpu" % name)
            self.assertEqual(1, replicas,
                             "%s must be enabled (replicas 1) in the dev per-service manifest, which is "
                             "what service-deploy.sh applies" % name)

        # The dev kustomize overlay must say the same thing, and it is a PATCH, so assert on the RENDER
        # rather than the patch file - a patch that failed to apply would otherwise read as compliant.
        import subprocess
        render = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s" / "overlays" / "dev")],
                                capture_output=True, text=True)
        self.assertEqual(0, render.returncode,
                         "the dev overlay must render: %s" % render.stderr[-800:])
        dev_overlay, dev_replicas = {}, {}
        for doc in yaml.safe_load_all(render.stdout):
            if not doc or doc.get("kind") != "Deployment":
                continue
            for c in doc["spec"]["template"]["spec"]["containers"]:
                if c["name"] in trio:
                    req = ((c.get("resources") or {}).get("requests") or {})
                    dev_overlay[c["name"]] = req.get("cpu", "NONE")
                    dev_replicas[c["name"]] = doc["spec"].get("replicas")
        self.assertEqual(set(trio), set(dev_overlay),
                         "the rendered dev overlay must contain all three containers")
        for name in trio:
            self.assertEqual("NONE", dev_overlay[name],
                             "%s must reserve NO cpu in the rendered dev overlay" % name)
            self.assertEqual(1, dev_replicas[name],
                             "%s must still be enabled (replicas 1) on dev" % name)

        # Production and es4 keep their reservations. Checked on the production RENDER too, so a stray
        # cpu: null reaching the production overlay would fail here.
        prod = cpu_requests([
            "k8s/services/dealer-ledger/overlays/production/manifest.yaml",
            "k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml",
        ])
        es4 = cpu_requests([
            "k8s/es4/services/dealer-ledger.yaml",
            "k8s/es4/services/dealer-ledger-calibration.yaml",
        ])
        # `experiment` is the fourth environment whose overlays define these same containers. It is not a
        # target of this change and is asserted here for exactly that reason: Codex (r2) pointed out that
        # leaving it neither asserted nor exempted is how a dev-only change quietly becomes a four-
        # environment one. Its declared state is identical to production and es4 - replicas 0 with the
        # 2 / 100m / 100m reservations - so it is checked by the same loop rather than given its own.
        experiment = cpu_requests([
            "k8s/services/dealer-ledger/overlays/experiment/manifest.yaml",
            "k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml",
        ])
        expected = {"dealer-ledger": "2",
                    "dealer-ledger-calibration-scorer": "100m",
                    "dealer-ledger-calibration-accumulator": "100m"}
        for label, got in (("production", prod), ("es4", es4), ("experiment", experiment)):
            self.assertEqual(set(trio), set(got), "%s must define all three containers" % label)
            for name, want in expected.items():
                cpu, replicas = got[name]
                self.assertEqual(want, str(cpu),
                                 "%s must KEEP its %s cpu reservation in %s - real hardware, where the "
                                 "reservation is what guarantees it" % (name, want, label))
                self.assertEqual(0, replicas,
                                 "%s must stay inert (replicas 0) in %s - this dev enable must not leak "
                                 "into any other environment" % (name, label))

        prod_render = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s" / "overlays" / "production")],
                                     capture_output=True, text=True)
        self.assertEqual(0, prod_render.returncode,
                         "the production overlay must render: %s" % prod_render.stderr[-800:])
        seen = 0
        for doc in yaml.safe_load_all(prod_render.stdout):
            if not doc or doc.get("kind") != "Deployment":
                continue
            for c in doc["spec"]["template"]["spec"]["containers"]:
                if c["name"] in trio:
                    seen += 1
                    req = ((c.get("resources") or {}).get("requests") or {})
                    self.assertEqual(expected[c["name"]], str(req.get("cpu", "NONE")),
                                     "%s must keep its cpu reservation in the PRODUCTION render" % c["name"])
                    self.assertEqual(0, doc["spec"].get("replicas"),
                                     "%s must stay inert in production" % c["name"])
        self.assertEqual(3, seen, "the production render must contain all three containers")


if __name__ == "__main__":
    unittest.main()
