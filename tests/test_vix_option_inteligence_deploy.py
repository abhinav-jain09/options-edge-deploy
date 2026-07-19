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
        gateway = (ROOT / "k8s/base/feed-gateway-deployment.yaml").read_text()
        self.assertIn("KAFKA_VIX_OPTION_INTELIGENCE_TOPIC", gateway)
        self.assertIn("options.spx.vix-option-inteligence-service.current", gateway)
        self.assertNotIn("options.spx.0dte.intelligence.current", topics + deployment + gateway)

    def test_es4_uses_es_symbol_and_mirrors_vix(self):
        manifest = (ROOT / "k8s/es4/services/vix-option-inteligence.yaml").read_text()
        self.assertIn("name: ZERO_DTE_SYMBOL", manifest)
        self.assertIn("value: ES", manifest)
        mm2 = (ROOT / "infra/es4/mm2/mm2.properties").read_text()
        self.assertIn("underlying.es.trades,underlying.vix.price", mm2)
        bootstrap = (ROOT / "scripts/es4/bootstrap-es4.sh").read_text()
        self.assertIn("docker compose up -d --force-recreate mm2", bootstrap)
        topic_script = (ROOT / "scripts/es4/create-es-topics.sh").read_text()
        self.assertIn("es.underlying.vix.price", topic_script)
        self.assertIn("es.options.spx.vix-option-inteligence-service.current", topic_script)

    def test_all_three_jenkins_paths_include_service(self):
        service_job = (ROOT / "Jenkinsfile.service-deploy").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        self.assertIn("'vix-option-inteligence'", service_job)
        self.assertIn("'vix-option-inteligence'", es_job)

    def test_rename_removes_legacy_workload_only_after_replacement(self):
        scoped = (ROOT / "scripts/deploy/service-deploy.sh").read_text()
        monolith = (ROOT / "scripts/deploy/apply.sh").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        for deployment_path in (scoped, monolith, es_job):
            self.assertIn("delete deployment zero-dte-intelligence-service --ignore-not-found", deployment_path)
            self.assertIn("delete service zero-dte-intelligence-service --ignore-not-found", deployment_path)


if __name__ == "__main__":
    unittest.main()
