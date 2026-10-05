import pathlib
import os
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


# The §10 closed set of DEPLOYMENT ENVIRONMENT names, as PayloadV2.FrameContext.ENVIRONMENTS declares it.
# A frame records the environment that produced it, so this set is part of the data contract, not a label.
ZERODTE_ENVIRONMENTS = ("dev", "production", "es4")


class VixOptionInteligenceStageBEnvironmentTest(unittest.TestCase):
    """The operand Stage-B's frames engine REQUIRES, and the v1 research writer already reads.

    Stage-B reads ZERODTE_RESEARCH_ENVIRONMENT -- not a new ZERO_DTE_ENVIRONMENT beside it -- because two
    names for one fact let a deployment set them differently, and the frames and the research would then
    disagree about which environment produced the evidence. The name travels into every frame's payload, so a
    wrong value mislabels evidence that is otherwise correct; StageBSettings therefore requires it and holds
    it to the closed set, and a STAGE_B boot without it is refused at configuration parsing.

    It is INERT for the running v1 runtime (the research writer only requires it with the shadow model on),
    which is why setting it is safe ahead of any switchover and why nothing here depends on the mode.
    """

    # WHERE EACH ENVIRONMENT IS DECLARED. dev and production are generated slices of the monolithic
    # overlays; es4 is a DIRECT deployment, applied by deploy-all and deploy-service and held in
    # validate-es4-render.sh's KNOWN_STALE, so the renderer never overwrites it and that file is where
    # an es4 operand has to be declared. Codex r11 MAJOR: this test covered only the two overlays and
    # claimed "both live environments are covered" while es4 -- a third live deployment of the same
    # image -- had no ZERODTE_RESEARCH_ENVIRONMENT at all, inline or via its envFrom ConfigMaps.
    DECLARATIONS = {
        "dev": "k8s/services/vix-option-inteligence/overlays/dev/manifest.yaml",
        "production": "k8s/services/vix-option-inteligence/overlays/production/manifest.yaml",
        "es4": "k8s/es4/services/vix-option-inteligence.yaml",
    }

    def overlay(self, env):
        return (ROOT / f"k8s/services/vix-option-inteligence/overlays/{env}/manifest.yaml").read_text()

    def declaration(self, env):
        return (ROOT / self.DECLARATIONS[env]).read_text()

    def env_value(self, text, name):
        """The value of an env entry in a rendered manifest, or None."""
        lines = [l.strip() for l in text.split("\n")]
        for i, line in enumerate(lines):
            if line == f"- name: {name}":
                for nxt in lines[i + 1:]:
                    if nxt.startswith("value:"):
                        return nxt.split("value:", 1)[1].strip().strip('"').strip("'")
                    if nxt.startswith("- name:"):
                        return None
                return None
        return None

    def test_every_registered_environment_overlay_sets_its_own_name(self):
        overlays = sorted(p.name for p in (ROOT / "k8s/services/vix-option-inteligence/overlays").iterdir() if p.is_dir())
        self.assertTrue(overlays, "the service has no overlays, so this test would prove nothing")
        checked = []
        for env in overlays:
            if env not in ZERODTE_ENVIRONMENTS:
                # NOT silently skipped: an overlay whose name is outside the closed set cannot carry a lawful
                # value, so Stage-B can never be enabled there. Said out loud rather than passed over.
                self.assertIsNone(
                    self.env_value(self.overlay(env), "ZERODTE_RESEARCH_ENVIRONMENT"),
                    f"overlay '{env}' is not one of {ZERODTE_ENVIRONMENTS}, so it must not claim a Stage-B environment",
                )
                continue
            value = self.env_value(self.overlay(env), "ZERODTE_RESEARCH_ENVIRONMENT")
            self.assertEqual(
                env, value,
                f"overlay '{env}' must set ZERODTE_RESEARCH_ENVIRONMENT to '{env}' (it is {value!r}); "
                "Stage-B requires it and a wrong value mislabels every frame it produces",
            )
            checked.append(env)
        self.assertEqual(["dev", "production"], checked, "both overlay environments are covered")

    def test_every_name_in_the_closed_set_has_a_manifest_that_declares_it(self):
        # The set is the DATA CONTRACT (PayloadV2.FrameContext.ENVIRONMENTS). A name in it with no
        # declaring manifest is a deployment that cannot boot Stage-B, and es4 was exactly that.
        self.assertEqual(
            sorted(ZERODTE_ENVIRONMENTS), sorted(self.DECLARATIONS),
            "every environment in the closed set needs a manifest that declares its own name here",
        )
        for env in ZERODTE_ENVIRONMENTS:
            path = ROOT / self.DECLARATIONS[env]
            self.assertTrue(path.is_file(), f"{path} is missing, so '{env}' declares nothing")
            value = self.env_value(self.declaration(env), "ZERODTE_RESEARCH_ENVIRONMENT")
            self.assertEqual(
                env, value,
                f"{self.DECLARATIONS[env]} must set ZERODTE_RESEARCH_ENVIRONMENT to '{env}' (it is {value!r}); "
                "Stage-B requires it, refuses a value outside the closed set, and a wrong value mislabels "
                "every frame and research row that deployment produces",
            )

    def test_lifting_the_es4_hold_is_a_TWO_part_change_and_es4_is_declared_for_both_states(self):
        """es4 has two states, and the operand must be declared for each.

        HELD (today): "vix-option-inteligence" is absent from the renderer's SERVICES list -- it fell out in
        c12e87c -- and the name is quarantined in validate-es4-render.sh's KNOWN_STALE. The renderer iterates
        SERVICES only, so it does not produce this manifest, and k8s/es4/services/vix-option-inteligence.yaml
        is es4's source of authority; the closed-set test above is what holds that file to 'es4'.

        RENDERED: whoever lifts the hold must do BOTH halves -- add the name to SERVICES and drop it from
        KNOWN_STALE. Codex r12 MAJOR: this test and the manifest header previously said emptying KNOWN_STALE
        made the renderer take over, which is false; dropping it from KNOWN_STALE alone only makes the
        committed file unaccounted-for. validate-es4-render.sh refuses each half on its own, so the two-part
        change is enforced there. What is enforced NOWHERE else is that es4 keeps its own environment name
        through the transition, which is what this test is for: the ES_ENV _override is INERT TODAY, and it
        exists so a future re-render cannot inherit ZERODTE_RESEARCH_ENVIRONMENT=production from the overlay
        it derives from -- a relabelling of every es4 frame and research row, not a boot failure, so nothing
        else would catch it.
        """
        renderer = (ROOT / "scripts/es4/render_es4_manifests.py").read_text()
        validator = (ROOT / "scripts/ci/validate-es4-render.sh").read_text()

        services = renderer.split("SERVICES = [", 1)[1].split("\n]", 1)[0]
        in_services = '"vix-option-inteligence"' in services
        known_stale = validator.split("KNOWN_STALE = {", 1)[1].split("}", 1)[0]
        in_known_stale = '"vix-option-inteligence"' in known_stale

        # The hold is CONSISTENT: held means absent from SERVICES and quarantined. Half a transition is a
        # state in which nobody can say which file is the authority.
        self.assertEqual(
            not in_services, in_known_stale,
            "es4's vix-option-inteligence hold is half-lifted: it is "
            + ("in" if in_services else "absent from") + " the renderer's SERVICES and "
            + ("in" if in_known_stale else "not in") + " KNOWN_STALE. Lifting the hold is BOTH edits -- add it "
            "to SERVICES and drop it from KNOWN_STALE -- and then reconcile the capacity divergence that is "
            "why the hold exists (the production CPU request, 100m -> 500m)",
        )

        block = renderer.split('"vix-option-inteligence": [', 1)
        self.assertEqual(2, len(block), "the renderer no longer has a vix-option-inteligence ES_ENV block")
        block = block[1].split("],", 1)[0]
        self.assertIn(
            '{"name": "ZERODTE_RESEARCH_ENVIRONMENT", "value": "es4", "_override": True}', block,
            "the es4 ES_ENV must OVERRIDE the environment it derives from production. It is inert while the "
            "name is out of SERVICES; it is here so that whoever lifts the hold cannot silently relabel every "
            "es4 frame and research row as having come from production",
        )

    def test_the_value_is_never_overridden_a_second_time_in_the_same_overlay(self):
        # One authoritative value per overlay: a second entry would make the effective one depend on
        # ordering, and Kubernetes takes the LAST, so a stale first entry would read as correct.
        for env in ZERODTE_ENVIRONMENTS:
            text = self.declaration(env)
            self.assertEqual(
                1, text.count("- name: ZERODTE_RESEARCH_ENVIRONMENT"),
                f"{self.DECLARATIONS[env]} declares ZERODTE_RESEARCH_ENVIRONMENT more than once",
            )

    def test_no_second_name_is_invented_for_the_same_fact(self):
        for env in ZERODTE_ENVIRONMENTS:
            self.assertNotIn(
                "ZERO_DTE_ENVIRONMENT\n", self.declaration(env),
                f"{self.DECLARATIONS[env]} invents a second name for the environment; "
                "Stage-B reads ZERODTE_RESEARCH_ENVIRONMENT",
            )

    def test_the_mode_operand_is_absent_so_the_image_rolls_dark(self):
        # §14's dark cutover: V1 is the DEFAULT, so an overlay that does not mention the mode runs v1 and
        # rolling the image is not a cutover. An overlay that sets STAGE_B has made a decision, and this test
        # is where that decision becomes visible rather than arriving with an image.
        for env in ZERODTE_ENVIRONMENTS:
            self.assertIsNone(
                self.env_value(self.declaration(env), "ZERO_DTE_RUNTIME_MODE"),
                f"{self.DECLARATIONS[env]} selects a Stage-B runtime mode; that is a switchover, not a deploy",
            )


class VixOptionInteligenceDeployTest(unittest.TestCase):
    def test_live_service_is_registered_for_dev_and_prod(self):
        registry = (ROOT / "services.yaml").read_text()
        self.assertIn("name: vix-option-inteligence", registry)
        self.assertIn("envs: [dev, production]", registry)
        deployment = (ROOT / "k8s/base/vix-option-inteligence-deployment.yaml").read_text()
        self.assertIn("ZERO_DTE_INTELLIGENCE_ENABLED", deployment)
        self.assertIn('value: "true"', deployment)
        self.assertIn("ZERO_DTE_MIN_DIRECTION_HOLD_MS", deployment)
        self.assertIn('value: "120000"', deployment)

    def test_current_topic_is_explicit_and_compacted(self):
        topics = (ROOT / "scripts/kafka/topics.env").read_text()
        deployment = (ROOT / "k8s/base/vix-option-inteligence-deployment.yaml").read_text()
        self.assertIn("options.spx.vix-option-inteligence-service.current:32", topics)
        compacted = topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1]
        self.assertIn("options.spx.vix-option-inteligence-service.current", compacted)
        # The active Kafka contract carries the exact service identity; the legacy 0DTE topic is
        # intentionally absent from all producer/consumer configuration.
        reconciler = (ROOT / "scripts/kafka/ensure-vix-option-inteligence-topic.sh").read_text()
        self.assertIn("PARTITIONS=32", reconciler)
        self.assertIn("cleanup.policy=compact", reconciler)
        service_job = (ROOT / "Jenkinsfile.service-deploy").read_text()
        self.assertIn("ensure-vix-option-inteligence-topic.sh", service_job)
        # The monolithic deploy job must apply the identical Kafka contract (reconcile +
        # zero-orphan prune), not only the per-service job.
        monolith_job = (ROOT / "Jenkinsfile").read_text()
        self.assertIn("ensure-vix-option-inteligence-topic.sh", monolith_job)
        gateway = (ROOT / "k8s/base/feed-gateway-deployment.yaml").read_text()
        self.assertIn("KAFKA_VIX_OPTION_INTELIGENCE_TOPIC", gateway)
        self.assertIn("options.spx.vix-option-inteligence-service.current", gateway)
        self.assertNotIn("options.spx.0dte.intelligence.current", topics + deployment + gateway)

    def test_es4_uses_es_symbol_and_mirrors_vix(self):
        manifest = (ROOT / "k8s/es4/services/vix-option-inteligence.yaml").read_text()
        self.assertIn("name: ZERO_DTE_SYMBOL", manifest)
        self.assertIn("value: ES", manifest)
        mm2 = (ROOT / "infra/es4/mm2/mm2.properties").read_text()
        # This mirror is VIX-ONLY since DBP-R32 (2026-07-26). It used to also carry
        # underlying.es.trades prod->es4, but the ES-futures subscription moved to es4, so es4
        # produces es.underlying.es.trades natively and the traffic now flows the OTHER way
        # (es4 -> prod, over the renaming bridge — MM2 cannot rename a topic).
        # ⭐Assert the EFFECTIVE topic list, not the file text. A whole-file assertNotIn is wrong
        # here: this file legitimately mentions underlying.es.trades in the comment explaining WHY
        # it was removed, so a blunt check fails on its own documentation.
        topic_lines = [
            ln.strip()
            for ln in mm2.splitlines()
            if ln.strip().startswith("es->es4.topics") and not ln.strip().startswith("#")
        ]
        self.assertEqual(
            ["es->es4.topics = underlying.vix.price"],
            [ln for ln in topic_lines if ".exclude" not in ln],
            "the prod->es4 mirror must carry VIX only",
        )
        # Pin the LOOP-SAFETY property: both directions live for one logical topic is exactly what
        # produced the 2026-07-24 `es.es.es...` runaway. es4 now PRODUCES ES trades and the bridge
        # carries them es4->prod, so re-adding them here would close the cycle.
        self.assertNotIn(
            "underlying.es.trades",
            " ".join(ln for ln in topic_lines if ".exclude" not in ln),
            "MM2 must not mirror ES trades prod->es4",
        )
        bootstrap = (ROOT / "scripts/es4/bootstrap-es4.sh").read_text()
        self.assertIn("docker compose up -d --force-recreate mm2", bootstrap)
        # Topic definitions moved to the SSOT (scripts/kafka/topics.env, applied by
        # scripts/kafka/apply-topics.sh); create-es-topics.sh now only SELECTS the es4 set and
        # supplies the broker + CLI shim. This assertion still named the old inline location and
        # had been failing on main — assert the SSOT, and assert the delegation separately, so the
        # test tracks where the truth actually lives.
        topics_env = (ROOT / "scripts/kafka/topics.env").read_text()
        self.assertIn("es.underlying.vix.price", topics_env)
        self.assertIn("es.options.spx.vix-option-inteligence-service.current", topics_env)
        topic_script = (ROOT / "scripts/es4/create-es-topics.sh").read_text()
        self.assertIn("TOPIC_SET=es4", topic_script)
        self.assertIn("apply-topics.sh", topic_script)

    def test_all_three_jenkins_paths_include_service(self):
        service_job = (ROOT / "Jenkinsfile.service-deploy").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        self.assertIn("'vix-option-inteligence'", service_job)
        self.assertIn("'vix-option-inteligence'", es_job)

    def test_both_callers_invoke_the_single_prune_implementation(self):
        # Zero-orphan rule: ONE implementation (the lib) with fail-closed discovery,
        # fail-loud deletes, and terminal verification; both production callers source it
        # and supply broker-appropriate wrappers.
        lib = (ROOT / "scripts/kafka/prune-retired-zero-dte-identity.lib.sh").read_text()
        self.assertIn('prefix="zero-dte-intelligence-service-v1"', lib)
        self.assertIn("refusing to prune (fail-closed)", lib)
        self.assertIn("FATAL: failed to delete retired consumer group", lib)
        self.assertIn("FATAL: retired groups still present after delete", lib)
        self.assertIn("while IFS= read -r g", lib)
        reconciler = (ROOT / "scripts/kafka/ensure-vix-option-inteligence-topic.sh").read_text()
        self.assertIn("prune-retired-zero-dte-identity.lib.sh", reconciler)
        self.assertIn(
            'prune_retired_zero_dte_identity "${TOPIC_PREFIX:-}options.spx.0dte.intelligence.current"',
            reconciler)
        es4 = (ROOT / "scripts/es4/create-es-topics.sh").read_text()
        self.assertIn("prune-retired-zero-dte-identity.lib.sh", es4)
        self.assertIn('prune_retired_zero_dte_identity "es.options.spx.0dte.intelligence.current"', es4)
        # The docker-exec now lives in the PATH SHIM, not inline in create-es-topics.sh — the es4
        # host has no Kafka CLI, so every call is proxied through scripts/es4/kafka-cli-shim.
        # This assertion used to look for it inline and was therefore describing the old shape.
        self.assertIn("prune_kg()", es4)
        self.assertIn("SHIM_DIR", es4)
        shim = ROOT / "scripts/es4/kafka-cli-shim/kafka-consumer-groups"
        self.assertTrue(shim.exists(), "prune_kg() calls kafka-consumer-groups; the shim must exist")
        self.assertTrue(os.access(shim, os.X_OK), "the shim must be executable or PATH lookup fails")
        self.assertIn("docker exec -i", shim.read_text())
        self.assertIn("kafka-consumer-groups", shim.read_text())

    def test_rename_removes_legacy_workload_only_after_replacement(self):
        scoped = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        monolith = (ROOT / "scripts/deploy/apply.sh").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        for deployment_path in (scoped, monolith, es_job):
            self.assertIn("delete deployment zero-dte-intelligence-service --ignore-not-found", deployment_path)
            self.assertIn("delete service zero-dte-intelligence-service --ignore-not-found", deployment_path)


if __name__ == "__main__":
    unittest.main()
