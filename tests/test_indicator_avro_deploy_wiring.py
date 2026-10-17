"""The Jenkins wiring of the indicator Avro cutover (INDICATOR-AVRO-CONVERSION-PLAN.md sections 4-6): the schema
registration runs before the indicator topic barrier and the rollout, the cutover scales the producer down and resets
its state before recreating the topics, es4's clean reset re-imports the fixed ids after the registry is healthy and
before any topic or app, and the mirror install asserts schema parity before copying from the source's offset 0."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def stages(text: str) -> list[str]:
    return re.findall(r"stage\('([^']+)'\)", text)


class IndicatorAvroDeployWiring(unittest.TestCase):
    def test_service_deploy_registers_then_recreates_then_verifies_then_deploys(self):
        t = (ROOT / "Jenkinsfile.service-deploy").read_text()
        s = stages(t)
        order = [s.index("Register indicator schemas"), s.index("Recreate indicator topics (Avro cutover)"),
                 s.index("Verify indicator topics"), s.index("Deploy (service-scoped)")]
        self.assertEqual(order, sorted(order), s)
        self.assertIn("booleanParam(name: 'REGISTER_INDICATOR_SCHEMAS'", t)
        self.assertIn("booleanParam(name: 'RECREATE_INDICATOR_TOPICS'", t)
        reg = t[t.index("stage('Register indicator schemas')"):t.index("stage('Recreate indicator topics (Avro cutover)')")]
        self.assertIn("ensure-indicator-schemas.sh", reg)
        self.assertIn("CONTRACTS_PERMITTED_SHA", reg)
        self.assertIn("merge-base --is-ancestor", reg, "the contracts commit must be on main")
        self.assertIn("jsonpath='{.data.SCHEMA_REGISTRY_URL}'", reg, "the registry comes from the deployed configuration")
        self.assertNotIn("192.168.100.252:8082", reg, "no registry literal")
        rec = t[t.index("stage('Recreate indicator topics (Avro cutover)')"):t.index("stage('Verify indicator topics')")]
        scale = rec.index("scale deployment indicator-service --replicas=0")
        reset = rec.index("reset-indicator-state.sh")
        recreate = rec.index("ensure-indicator-topics.sh --recreate")
        self.assertLess(scale, reset)
        self.assertLess(reset, recreate, "offsets to 0 BEFORE the topics go")
        self.assertIn("params.SERVICE == 'indicator-service'", rec)

    def test_es4_deploy_fetches_schemas_registers_on_every_topic_creating_path_and_recreates_on_request(self):
        t = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        s = stages(t)
        self.assertLess(s.index("Fetch indicator schemas (contracts)"), s.index("Create topics"))
        self.assertLess(s.index("Fetch indicator schemas (contracts)"), s.index("Clean reset (atomic Kafka + Schema Registry wipe)"))
        fetch = t[t.index("stage('Fetch indicator schemas (contracts)')"):t.index("stage('Create topics')")]
        self.assertIn("schemas-registry/indicators/", fetch)
        self.assertIn("merge-base --is-ancestor", fetch)
        create = t[t.index("stage('Create topics')"):t.index("stage('Clean reset")]
        self.assertLess(create.index("ensure-indicator-schemas.sh"), create.index("create-es-topics.sh"))
        deploy = t[t.index("stage('Deploy to es4')"):t.index("stage('Activate es-feed")]
        self.assertIn("ensure-indicator-schemas.sh", deploy)
        self.assertLess(deploy.index("reset-indicator-state.sh"), deploy.index("ensure-indicator-topics.sh --recreate"))
        self.assertIn("RECREATE_ENVIRONMENT=es4", deploy)
        self.assertIn("INDICATOR_INPUT_TOPIC=es.underlying.es.trades", deploy)

    def test_es4_clean_reset_imports_the_fixed_ids_after_the_registry_is_healthy_and_before_topics(self):
        t = (ROOT / "scripts/es4/cleanup-es4.sh").read_text()
        health = t.index("waiting for kafka + schema-registry health")
        empty = t.index("/subjects")
        register = t.index("ensure-indicator-schemas.sh")
        topics = t.index("create-es-topics.sh")
        self.assertLess(health, empty)
        self.assertLess(empty, register)
        self.assertLess(register, topics)
        self.assertIn("did not restart on the empty store", t)

    def test_mirror_install_asserts_parity_then_copies_from_the_source_start(self):
        t = (ROOT / "Jenkinsfile.es-indicator-mirror").read_text()
        gen = t[t.index("stage('Generate config + match target topic')"):t.index("stage('Install launchd service')")]
        parity = gen.index("assert-indicator-schema-parity.sh")
        group = gen.index("--delete --group")
        reset = gen.index("auto.offset.reset=earliest")
        self.assertLess(parity, group)
        self.assertLess(group, reset)
        self.assertNotIn("auto.offset.reset=latest", t)
        verify = t[t.index("stage('Verify (running + zero pollution)')"):]
        self.assertIn("target end offsets equal the source", verify)
        self.assertIn("decode-newest-indicator-record.sh", verify)

    def test_archive_unit_ships_the_avro_reader_and_the_contracts_jar(self):
        t = (ROOT / "Jenkinsfile.archive-scripts-deploy").read_text()
        unit = re.search(r'UNIT\s*=\s*"([^"]*)"', t).group(1).split()
        self.assertIn("IndicatorArchiveReader.java", unit)
        self.assertIn("options-edge-contracts.jar", unit)
        self.assertIn("scripts/ops/archive/IndicatorArchiveReader.java", t)
        self.assertIn("contracts.version", t)
        self.assertRegex((ROOT / "scripts/ops/archive/contracts.version").read_text().strip(), r"^\d+\.\d+\.\d+$")
        arch = (ROOT / "scripts/ops/archive/oe-archive-kafka.sh").read_text()
        self.assertIn('OE_INDICATOR_AVRO_TOPICS="${OE_INDICATOR_AVRO_TOPICS:-options.indicators.bars es.options.indicators.bars}"', arch)
        self.assertLess(arch.index("if is_indicator_avro_topic \"$topic\"; then"), arch.index('kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \\\n           --topic "$topic" --partition "$part" --offset "$from"'))

    def test_the_scripts_exist_and_are_executable(self):
        for p in ["scripts/kafka/ensure-indicator-schemas.sh", "scripts/kafka/assert-indicator-schema-parity.sh",
                  "scripts/kafka/reset-indicator-state.sh", "scripts/kafka/decode-newest-indicator-record.sh",
                  "scripts/kafka/topic-shape.lib.sh"]:
            self.assertTrue((ROOT / p).exists(), p)
        import os
        for p in ["scripts/kafka/ensure-indicator-schemas.sh", "scripts/kafka/assert-indicator-schema-parity.sh",
                  "scripts/kafka/reset-indicator-state.sh", "scripts/kafka/decode-newest-indicator-record.sh"]:
            self.assertTrue(os.access(ROOT / p, os.X_OK), p)

    def test_apply_topics_sources_the_shared_shape_library(self):
        t = (ROOT / "scripts/kafka/apply-topics.sh").read_text()
        self.assertIn('source "$SCRIPT_DIR/topic-shape.lib.sh"', t)
        self.assertIn("topic_shape_resolve_declarations", t)
        for fn in ["create_topic()", "alter_topic_config()", "topic_cleanup_policy()", "wait_for_topic_absent()"]:
            self.assertNotIn(fn, t, fn + " lives in the library now")
        lib = (ROOT / "scripts/kafka/topic-shape.lib.sh").read_text()
        for fn in ["create_topic()", "alter_topic_config()", "topic_cleanup_policy()", "wait_for_topic_absent()",
                   "topic_shape_resolve_declarations()"]:
            self.assertIn(fn, lib, fn)


if __name__ == "__main__":
    unittest.main()
