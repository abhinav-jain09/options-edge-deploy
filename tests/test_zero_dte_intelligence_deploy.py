import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class ZeroDteIntelligenceDeployTest(unittest.TestCase):
    def test_live_service_is_registered_for_dev_and_prod(self):
        registry = (ROOT / "services.yaml").read_text()
        self.assertIn("name: zero-dte-intelligence", registry)
        self.assertIn("envs: [dev, production]", registry)
        deployment = (ROOT / "k8s/base/zero-dte-intelligence-deployment.yaml").read_text()
        self.assertIn("ZERO_DTE_INTELLIGENCE_ENABLED", deployment)
        self.assertIn('value: "true"', deployment)
        self.assertIn("ZERO_DTE_MIN_DIRECTION_HOLD_MS", deployment)
        self.assertIn('value: "120000"', deployment)

    def test_current_topic_is_explicit_and_compacted(self):
        topics = (ROOT / "scripts/kafka/topics.env").read_text()
        self.assertIn("options.spx.0dte.intelligence.current:4", topics)
        compacted = topics.split("OPTIONS_EDGE_COMPACTED_TOPICS=", 1)[1]
        self.assertIn("options.spx.0dte.intelligence.current", compacted)

    def test_es4_uses_es_symbol_and_mirrors_vix(self):
        manifest = (ROOT / "k8s/es4/services/zero-dte-intelligence.yaml").read_text()
        self.assertIn("name: ZERO_DTE_SYMBOL", manifest)
        self.assertIn("value: ES", manifest)
        mm2 = (ROOT / "infra/es4/mm2/mm2.properties").read_text()
        self.assertIn("underlying.es.trades,underlying.vix.price", mm2)
        topic_script = (ROOT / "scripts/es4/create-es-topics.sh").read_text()
        self.assertIn("es.underlying.vix.price", topic_script)
        self.assertIn("es.options.spx.0dte.intelligence.current", topic_script)

    def test_all_three_jenkins_paths_include_service(self):
        service_job = (ROOT / "Jenkinsfile.service-deploy").read_text()
        es_job = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        self.assertIn("'zero-dte-intelligence'", service_job)
        self.assertIn("'zero-dte-intelligence'", es_job)


if __name__ == "__main__":
    unittest.main()
