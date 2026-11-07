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


if __name__ == "__main__":
    unittest.main()
