# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class Es4CleanupFullResetTest(unittest.TestCase):
    def setUp(self):
        self.script = (ROOT / "scripts" / "es4" / "cleanup-es4.sh").read_text()
        self.jenkins = (ROOT / "Jenkinsfile.es4-deploy").read_text()

    def test_all_deployments_are_stopped_and_restored(self):
        self.assertNotIn('SCALE_EXCLUDE="es-web"', self.script)
        self.assertIn("reconcile-cleanup-state.awk", self.script)
        self.assertIn("restoring es4 Deployments to captured replica counts", self.script)

    def test_kafka_state_and_logs_are_wiped(self):
        self.assertIn("sudo -n find '$KAFKA_DATA'", self.script)
        self.assertIn("{.spec.local.path}", self.script)
        self.assertIn("/var/lib/rancher/k3s/storage/*", self.script)
        self.assertIn("/var/log/containers", self.script)
        self.assertIn("/var/log/pods", self.script)

    def test_topic_snapshot_keeps_contracts_not_streams_state_or_mm2_recursion(self):
        self.assertIn('TOPIC_STATE="$ES4_HOME/.es4-cleanup.topics"', self.script)
        self.assertIn("t ~ /^es\\./", self.script)
        self.assertIn("t !~ /^es\\.es\\./", self.script)
        self.assertIn("t !~ /-changelog($|-)/", self.script)
        self.assertIn("t !~ /-repartition($|-)/", self.script)
        self.assertIn("--describe --entity-type topics", self.script)

    def test_destructive_sequence_is_ordered(self):
        markers = [
            'log "scaling es4 Deployments to 0',
            'log "docker compose down',
            'log "wiping Kafka data volume CONTENTS',
            'log "wiping persistent Kafka Streams state stores',
            'log "deleting options-edge Kubernetes service logs',
            'log "docker compose up -d kafka schema-registry postgres redis',
            'log "recreating every snapshotted application topic',
            'log "docker compose up -d (start mm2',
            'log "restoring es4 Deployments to captured replica counts',
        ]
        positions = [self.script.index(marker) for marker in markers]
        self.assertEqual(positions, sorted(positions))

    def test_jenkins_syncs_complete_script_unit_and_invokes_cleanup(self):
        sync = 'rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/'
        self.assertEqual(self.jenkins.count(sync), 3)
        self.assertIn("bash /home/es4/repo/scripts/es4/cleanup-es4.sh", self.jenkins)


if __name__ == "__main__":
    unittest.main()
