from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class HpsfOpsArtifactsTest(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text()

    def test_hpsf_manifests_are_in_kustomization(self) -> None:
        kustomization = self.read("k8s/base/kustomization.yaml")
        for manifest in [
            "hpsf-stage-a-deployment.yaml",
            "hpsf-stage-b-deployment.yaml",
            "hpsf-postgres-writer-deployment.yaml",
            "hpsf-postgres-writer-service.yaml",
        ]:
            self.assertIn(manifest, kustomization)

    def test_hpsf_config_uses_databento_rf1_and_signal_only(self) -> None:
        config = self.read("k8s/base/configmap.yaml")
        required = [
            "HPSF_MODE: LIVE_SIGNAL_ONLY",
            "HPSF_STREAMS_REPLICATION_FACTOR: \"1\"",
            "HPSF_STREAMS_NUM_STANDBY_REPLICAS: \"0\"",
            "HPSF_STREAMS_STATE_DIR: /home/options-edge/data/kafka-streams/hpsf",
            "HPSF_STATE_STORE_TYPE: memory",
            "HPSF_TOPIC_OPRA_TCBBO: options.opra.tcbbo",
            "HPSF_TOPIC_UNDERLYING_ES_TRADES: underlying.es.trades",
            "HPSF_TOPIC_UNDERLYING_SPX_PRICE: underlying.spx.price",
            "HPSF_TOPIC_SIGNAL: options.hpsf.signal",
            "HPSF_TOPIC_LATEST_SIGNAL: options.hpsf.latest-signal",
            "HPSF_ORDER_PLACEMENT_ENABLED: \"false\"",
            "ORDER_PLACEMENT_ENABLED: \"false\"",
        ]
        for item in required:
            self.assertIn(item, config)
        self.assertNotIn("replication.factor=3", config)
        self.assertNotIn("min.insync.replicas=2", config)
        self.assertNotIn("num.standby.replicas=1", config)

    def test_stage_a_and_writer_manifest_health_and_state(self) -> None:
        stage_a = self.read("k8s/base/hpsf-stage-a-deployment.yaml")
        writer = self.read("k8s/base/hpsf-postgres-writer-deployment.yaml")
        self.assertIn("APP_MARKET_DATA_SOURCE", stage_a)
        self.assertIn("DATABENTO", stage_a)
        self.assertIn("HPSF_TOPOLOGY_ENABLED", stage_a)
        self.assertIn("value: \"true\"", stage_a)
        self.assertIn("/home/options-edge/data/kafka-streams/hpsf/stage-a", stage_a)
        self.assertIn("strike-current-state-store", self.read("k8s/base/configmap.yaml"))
        self.assertIn("strike-bucket-state-store", self.read("k8s/base/configmap.yaml"))
        self.assertIn("/health/live", writer)
        self.assertIn("/health/ready", writer)
        self.assertIn("options.hpsf.writer-dlq", writer)

    def test_stage_b_starts_real_stage_b_topology_without_duplicate_stage_a(self) -> None:
        stage_b = self.read("k8s/base/hpsf-stage-b-deployment.yaml")
        self.assertIn("HPSF_STAGE_ROLE", stage_b)
        self.assertIn("STAGE_B", stage_b)
        self.assertIn("HPSF_STAGE_A_ENABLED", stage_b)
        self.assertIn("HPSF_STAGE_B_ENABLED", stage_b)
        self.assertIn("HPSF_TOPOLOGY_ENABLED", stage_b)
        self.assertIn("value: \"true\"", stage_b)

    def test_hpsf_smoke_script_is_hpsf_only_and_parses(self) -> None:
        script = ROOT / "scripts/smoke/check-hpsf-deployment.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        text = script.read_text()
        self.assertIn("options.hpsf.latest-signal", text)
        self.assertIn("verify-hpsf-topics.sh", text)
        self.assertIn("hpsf-stage-a-service", text)
        self.assertIn("hpsf-postgres-writer-service", text)
        self.assertIn("print_k8s_diagnostics", text)
        self.assertIn("describe pods", text)
        self.assertNotIn("ibkr-feed-service", text)
        self.assertNotIn("options.ibkr.raw", text)

    def test_stage_b_runtime_smoke_produces_fixture_and_requires_fresh_outputs(self) -> None:
        script = ROOT / "scripts/smoke/check-hpsf-stage-b-runtime.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        text = script.read_text()
        self.assertIn("options.hpsf.strike-flow", text)
        self.assertIn("options.hpsf.signal", text)
        self.assertIn("options.hpsf.latest-signal", text)
        self.assertIn("underlying.es.trades", text)
        self.assertIn("underlying.spx.price", text)
        self.assertIn("read_expected_record", text)
        self.assertIn("wait_for_stage_b_log_contains", text)
        self.assertIn("--from-beginning", text)
        self.assertIn("expected_eval_id", text)
        self.assertIn("Recent hpsf-stage-b-service logs", text)
        self.assertIn("--bootstrap-server", text)
        self.assertIn("Stage B runtime smoke passed", text)
        self.assertNotIn("kafka-run-class", text)
        self.assertNotIn("ibkr-feed-service", text)
        self.assertNotIn("options.ibkr.raw", text)

    def test_stage_b_internal_topic_reset_is_scoped_to_repartition_topics(self) -> None:
        script = ROOT / "scripts/kafka/reset-hpsf-stage-b-internal-topics.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        text = script.read_text()
        self.assertIn("options-edge-hpsf-stage-b-v2-1", text)
        self.assertIn("-repartition", text)
        self.assertIn("Refusing to delete unexpected topic name", text)
        self.assertNotIn("options.hpsf.signal", text)
        self.assertNotIn("options.hpsf.latest-signal", text)
        self.assertNotIn("options.ibkr.raw", text)

    def test_monitoring_rules_and_scrape_cover_hpsf(self) -> None:
        rules = self.read("scripts/monitoring/hpsf-alert-rules.yaml")
        scrape = self.read("scripts/monitoring/apply-prometheus-scrapes.sh")
        for alert in [
            "HpsfKafkaDiskAbove75Percent",
            "HpsfOpraLagTooHigh",
            "HpsfUnderlyingLagTooHigh",
            "HpsfEvaluationDurationTooHigh",
            "HpsfDlqIncreasingFast",
            "HpsfWriterDlqNonZero",
            "HpsfPostgresWriterUnhealthy",
            "HpsfChangelogRestoreSlow",
            "HpsfStaleDataGateActiveTooLong",
            "HpsfOldBucketKeysNotDeleted",
        ]:
            self.assertIn(alert, rules)
        self.assertIn("hpsf-postgres-writer-service", scrape)

    def test_runbook_documents_market_hours_replay_and_rf1_warning(self) -> None:
        runbook = self.read("docs/hpsf-v2-runbook.md")
        for text in [
            "09:30 to 16:00 America/New_York",
            "09:30 to 13:00 America/New_York",
            "HPSF must never place orders",
            "HPSF_MODE=SHADOW",
            "HPSF_MODE=REPLAY",
            "options.hpsf.latest-signal",
            "RF=1 has no broker-failure durability",
            "Do not involve IBKR feed services",
        ]:
            self.assertIn(text, runbook)

    def test_jenkins_ops_pipeline_references_required_gates(self) -> None:
        pipeline = self.read("Jenkinsfile.hpsf-ops")
        for text in [
            "Validate Ops Files",
            "Kustomize Render",
            "Topic Verification",
            "Deploy HPSF Manifests",
            "Remote HPSF Smoke",
            "Rollback Info",
            "scripts/smoke/check-hpsf-deployment.sh",
            "scripts/kafka/verify-hpsf-topics.sh",
            "kubectl apply -f k8s/base/hpsf-stage-a-deployment.yaml",
            "options-edge-hpsf-stage-b-v2-1-smoke-${BUILD_NUMBER:-manual}",
            "set env deployment/hpsf-stage-b-service HPSF_STREAMS_APPLICATION_ID",
            'kubectl -n "$NAMESPACE" rollout restart deployment/hpsf-stage-b-service',
        ]:
            self.assertIn(text, pipeline)

    def test_main_deploy_pipeline_rolls_out_hpsf_services(self) -> None:
        pipeline = self.read("Jenkinsfile")
        for text in [
            "choices: ['DATABENTO', 'IBKR']",
            "MARKET_DATA_SOURCE = \"${params.MARKET_DATA_SOURCE ?: 'DATABENTO'}\"",
            "market_data_source=\"${MARKET_DATA_SOURCE:-DATABENTO}\"",
        ]:
            self.assertIn(text, pipeline)
        for text in [
            "HPSF_PROCESSING_IMAGE",
            "HPSF_POSTGRES_WRITER_IMAGE",
            "scripts/kafka/create-hpsf-topics.sh",
            "scripts/kafka/verify-hpsf-topics.sh",
            "stage('Reset HPSF Stage B Internal Topics')",
            "scale deployment/hpsf-stage-b-service --replicas=0",
            "scripts/kafka/reset-hpsf-stage-b-internal-topics.sh",
            "set image deployment/hpsf-stage-a-service",
            "set image deployment/hpsf-stage-b-service",
            "set image deployment/hpsf-postgres-writer-service",
            "rollout restart deployment/hpsf-stage-a-service",
            "rollout restart deployment/hpsf-stage-b-service",
            "rollout status deployment/hpsf-stage-b-service",
            "stage('HPSF Smoke')",
            "scripts/smoke/check-hpsf-deployment.sh",
            "scripts/smoke/check-hpsf-stage-b-runtime.sh",
        ]:
            self.assertIn(text, pipeline)

    def test_main_smoke_skips_live_ui_checks_outside_market_hours(self) -> None:
        script = self.read("scripts/smoke/check-k8s-services.sh")
        for text in [
            "LIVE_UI_MARKET_HOURS_ZONE",
            "America/New_York",
            "LIVE_UI_MARKET_OPEN",
            "09:30",
            "LIVE_UI_MARKET_CLOSE",
            "16:00",
            "SKIPPED - outside market hours",
            "A skipped live-market check is not proof that live streams work",
            "request_selected_contract",
            "check_integration_test",
        ]:
            self.assertIn(text, script)
        self.assertIn('metrics="$(curl -fsS "http://127.0.0.1:${local_port}/metrics")"', script)
        self.assertNotIn("curl -fsS \"http://127.0.0.1:${local_port}/metrics\" | grep -q", script)


if __name__ == "__main__":
    unittest.main()
