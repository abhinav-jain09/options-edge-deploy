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
        self.assertIn("git -C .jenkins-tmp/contracts checkout main", reg)
        self.assertIn("PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .jenkins-tmp/contracts --ref main", reg, "the contracts commit must be on main: the guard proves it")
        self.assertIn("jsonpath='{.data.SCHEMA_REGISTRY_URL}'", reg, "the registry comes from the deployed configuration")
        self.assertNotIn("192.168.100.252:8082", reg, "no registry literal")
        rec = t[t.index("stage('Recreate indicator topics (Avro cutover)')"):t.index("stage('Verify indicator topics')")]
        scale = rec.index("scale deployment indicator-service --replicas=0")
        reset = rec.index("reset-indicator-state.sh")
        recreate = rec.index("ensure-indicator-topics.sh --recreate")
        self.assertLess(scale, reset)
        self.assertLess(reset, recreate, "offsets to 0 BEFORE the topics go")
        self.assertIn("params.SERVICE == 'indicator-service'", rec)

    def test_a_destructive_recreate_requires_the_schema_registration_in_the_same_run(self):
        sd = (ROOT / "Jenkinsfile.service-deploy").read_text()
        rec = sd[sd.index("stage('Recreate indicator topics (Avro cutover)')"):sd.index("stage('Verify indicator topics')")]
        self.assertIn("if (!params.REGISTER_INDICATOR_SCHEMAS)", rec)
        self.assertLess(rec.index("error(\"RECREATE_INDICATOR_TOPICS=true requires REGISTER_INDICATOR_SCHEMAS=true"), rec.index("reset-indicator-state.sh"))
        es4 = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        dep = es4[es4.index("stage('Deploy to es4')"):]
        self.assertLess(dep.index('[ "${RECREATE_INDICATOR_TOPICS:-false}" = true ] && [ "${REGISTER_INDICATOR_SCHEMAS:-false}" != true ]'),
                        dep.index("ensure-indicator-schemas.sh"))
        self.assertLess(dep.index("ensure-indicator-schemas.sh"), dep.index("reset-indicator-state.sh"))

    def test_es4_deploy_fetches_schemas_registers_on_every_topic_creating_path_and_recreates_on_request(self):
        t = (ROOT / "Jenkinsfile.es4-deploy").read_text()
        s = stages(t)
        self.assertLess(s.index("Fetch indicator schemas (contracts)"), s.index("Create topics"))
        self.assertLess(s.index("Fetch indicator schemas (contracts)"), s.index("Clean reset (atomic Kafka + Schema Registry wipe)"))
        fetch = t[t.index("stage('Fetch indicator schemas (contracts)')"):t.index("stage('Create topics')")]
        self.assertIn("schemas-registry/indicators/", fetch)
        # the contracts source is a nested acquisition bound by its own guard (on main at CONTRACTS_PERMITTED_SHA),
        # and the ship is a dedicated rsync right after a verify of that tree
        self.assertIn("git -C .jenkins-tmp/contracts checkout main", fetch)
        self.assertIn("PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .jenkins-tmp/contracts --ref main", fetch)
        self.assertLess(fetch.index("verify-permitted-tree.sh --dir .jenkins-tmp/contracts'"), fetch.index("sh 'rsync -az --delete .jenkins-tmp/contracts/schemas-registry/indicators/"))
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
        delete_block = gen[group:reset]
        self.assertNotIn("|| true", delete_block, "the delete is not swallowed")
        self.assertIn("grep -q 'does not exist'", delete_block, "an absent group is the only accepted failure")
        self.assertIn("still listed", delete_block, "the group is polled to absence before MM1 starts")
        decode = (ROOT / "scripts/kafka/decode-newest-indicator-record.sh").read_text()
        self.assertIn("kafka-avro-console-consumer", decode, "the gate deserializes the record, not just its frame")
        self.assertIn("does not deserialize", decode)
        self.assertIn('PartitionCount:', decode, "the partition count comes from broker metadata")
        self.assertLess(decode.index("every partition must be gated"), decode.index("while IFS=: read -r t p end"))
        verify = t[t.index("stage('Verify (running + zero pollution)')"):]
        self.assertIn("target end offsets equal the source", verify)
        # EOS-aware parity (2026-10-18): the es4 producer is transactional, so the gate counts committed source
        # records (read_committed, offsets printed) against the target end offset instead of comparing end offsets,
        # which include one commit marker per transaction on the source only.
        self.assertIn("--isolation-level read_committed", verify)
        self.assertIn("print.offset=true", verify)
        self.assertIn("auto.offset.reset=none", verify)
        self.assertIn("Processed a total of", verify)
        self.assertNotIn('[ "$SEND" = "$TEND" ] && break', verify)
        self.assertIn("decode-newest-indicator-record.sh", verify)

    def test_archive_unit_ships_the_avro_reader_and_builds_the_contracts_jar_from_the_permitted_commit(self):
        t = (ROOT / "Jenkinsfile.archive-scripts-deploy").read_text()
        unit = re.search(r'UNIT\s*=\s*"([^"]*)"', t).group(1).split()
        self.assertIn("IndicatorArchiveReader.java", unit)
        self.assertNotIn("options-edge-contracts.jar", unit, "the jar is not a committed file: it is the SIDECAR")
        self.assertIn('SIDECAR     = "options-edge-contracts.jar canonical-archive-reader.jar"', t)
        self.assertIn("scripts/ops/archive/IndicatorArchiveReader.java", t)
        # the jar is BUILT from the permitted contracts commit: nested acquisition, its guard as the next statement,
        # a verify immediately before the package effect, a verify (target allowed) immediately before the ship
        stage = t[t.index("git clone git@github.com:abhinav-jain09/options-edge-contracts.git .jenkins-tmp/contracts"):]
        order = ["git -C .jenkins-tmp/contracts checkout main",
                 "PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .jenkins-tmp/contracts --ref main",
                 "latest-main",
                 "PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:?}\" bash scripts/jenkins/verify-permitted-tree.sh --dir .jenkins-tmp/contracts'",
                 "sh 'mvn -B -q -ntp -f .jenkins-tmp/contracts/pom.xml -DskipTests package'",
                 "verify-permitted-tree.sh --dir .jenkins-tmp/contracts --allow-ignored target'",
                 "sh 'scp -o BatchMode=yes .jenkins-tmp/contracts/target/options-edge-contracts-archive.jar \"${PROD_HOST}:${STAGE_DIR}/options-edge-contracts.jar\"'",
                 "sh 'scp -o BatchMode=yes scripts/ops/archive/oe-archive-kafka.sh"]
        at = -1
        for token in order:
            nxt = stage.index(token)
            self.assertGreater(nxt, at, token)
            at = nxt
        self.assertEqual((ROOT / "scripts/ops/archive/contracts.version").read_text().strip(), "latest-main", "the guarded main POM determines the coordinate")
        self.assertIn("UNIT='$UNIT $SIDECAR'", t, "the host installs, records and verifies the jar with the unit")
        self.assertNotIn(".m2/repository", t, "never a cached jar")
        self.assertLess(t.index("stage('Build canonical archive helper')"), t.index("stage('Check the host has not drifted')"))
        self.assertIn('ARCHIVE_CONTRACTS_VERSION = readFile(', t)
        self.assertIn('canonical-archive-reader.jar', t)
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
