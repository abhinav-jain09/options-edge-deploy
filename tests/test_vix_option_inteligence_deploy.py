import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


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
        self.assertIn("docker exec es4-kafka kafka-consumer-groups", es4)

    def test_rename_removes_legacy_workload_only_after_replacement(self):
        scoped = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        monolith = (ROOT / "scripts/deploy/apply.sh").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        for deployment_path in (scoped, monolith, es_job):
            self.assertIn("delete deployment zero-dte-intelligence-service --ignore-not-found", deployment_path)
            self.assertIn("delete service zero-dte-intelligence-service --ignore-not-found", deployment_path)


if __name__ == "__main__":
    unittest.main()
