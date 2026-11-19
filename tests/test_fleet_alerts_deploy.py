# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""The fleet alerts are wired the way FLEET-ALERT-DISCORD-REQUIREMENT says, and routed to the right room.

Routing is the requirement most likely to rot silently and the one whose failure sends a PRODUCTION page to
the dev group, or a dev page to the production group. It cannot be proved by running the job, so it is
asserted against the Jenkinsfile's own text here (R7c). Credential EXISTENCE cannot be proved from this
repository at all; what is proved is that the ids bound are the two ids other jobs already bind.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
JF = REPO / "Jenkinsfile.fleet-watch"
SCOPE = REPO / "scripts/ci/jenkins-permitted-sha-scope.txt"
WATCH = REPO / "scripts/watch"

PROD_ID = "offhours-discord-webhook"
DEV_ID = "offhours-discord-webhook-dev"
NEW_CHECKS = {"crash-loop": "paging", "kafka-lag": "paging", "partition-balance": "observation-only",
              "readiness": "paging", "dlq-growth": "paging", "oom": "paging", "host-load": "paging",
              "topic-freshness": "paging", "kafka-broker": "paging", "restart-budget": "paging"}


class Routing(unittest.TestCase):
    def setUp(self):
        self.jf = JF.read_text()

    def test_the_routing_helper_sends_production_to_the_prod_group(self):
        # The whole point: a production crash loop must not land in the dev room.
        m = re.search(r"def webhookFor\(String profile\) \{(.+?)\n\}", self.jf, re.S)
        self.assertIsNotNone(m, "Jenkinsfile.fleet-watch must carry a webhookFor(profile) helper")
        body = m.group(1)
        self.assertIn(PROD_ID, body)
        self.assertIn(DEV_ID, body)
        self.assertRegex(body, r"profile\s*==\s*'production'\s*\?\s*'%s'\s*:\s*'%s'" % (PROD_ID, DEV_ID),
                         "production must select %r and everything else %r" % (PROD_ID, DEV_ID))

    def test_the_dev_id_is_not_the_prod_id(self):
        # A copy-paste that bound the same id twice would route production into the dev room and pass every
        # other assertion here.
        self.assertNotEqual(PROD_ID, DEV_ID)
        self.assertTrue(DEV_ID.startswith(PROD_ID), "the dev id is the prod id plus a suffix; if that ever "
                                                    "stops being true, this test's substring checks must change")

    def test_es4_is_not_routed_to_production(self):
        # Owner decision: es4 -> the dev group. The helper's ternary covers it, and this says so by name so
        # a future edit that adds an es4 branch has to face this assertion.
        m = re.search(r"def webhookFor\(String profile\) \{(.+?)\n\}", self.jf, re.S)
        body = m.group(1)
        self.assertNotRegex(body, r"es4[^\n]*%s" % PROD_ID,
                            "es4 must not select the production webhook")

    def test_every_new_stage_binds_a_webhook_credential(self):
        for check in NEW_CHECKS:
            stage = self._stage_for(check)
            self.assertIn("withCredentials", stage,
                          "the %s stage must bind a webhook credential" % check)
            self.assertIn("webhookFor(profile)", stage,
                          "the %s stage must route through webhookFor, not a hardcoded id" % check)
            self.assertIn("DISCORD_WEBHOOK_URL", stage)

    def test_no_stage_hardcodes_a_credential_id(self):
        # A hardcoded id is how one stage ends up pointing at the wrong room while the helper looks right.
        for check in NEW_CHECKS:
            stage = self._stage_for(check)
            self.assertNotIn("'%s'" % PROD_ID, stage)
            self.assertNotIn("'%s'" % DEV_ID, stage)

    def test_each_check_runs_in_its_declared_mode(self):
        for check, mode in NEW_CHECKS.items():
            stage = self._stage_for(check)
            self.assertIn("FLEET_MODE=%s" % mode, stage,
                          "%s must run in %s mode (R4i puts partition-balance in observation-only until "
                          "its thresholds are calibrated)" % (check, mode))

    def test_partition_balance_is_the_only_observation_only_check(self):
        paging = [c for c, m in NEW_CHECKS.items() if m == "paging"]
        self.assertEqual({"crash-loop", "kafka-lag", "readiness", "dlq-growth", "oom", "host-load", "topic-freshness", "kafka-broker", "restart-budget"}, set(paging),
                         "every check pages from day one except partition balance, which waits for §5a")

    def test_readiness_and_dead_letters_no_longer_run_unwrapped_in_the_jenkinsfile(self):
        # They used to run `python3 scripts/watch/fleet-readiness.py` / `dlq-growth.py` straight from the stage: a red build and NOTHING in
        # Discord. A stage that calls the script itself would silently bring that back.
        for script in ("fleet-readiness.py", "dlq-growth.py"):
            for line in self.jf.splitlines():
                code = line.split("//", 1)[0]
                if script in code:
                    self.assertIn("fleet-alert-deliver.sh", code,
                                  "%s must run under fleet-alert-deliver.sh, not directly: %s" % (script, line.strip()))

    def test_the_host_load_check_has_the_node_permission_it_needs_and_asks_for_no_more(self):
        rbac = (REPO / "k8s/security/jenkins-fleet-watch-node-reader.yaml").read_text()
        self.assertIn("ClusterRole", rbac)
        self.assertRegex(rbac, r'resources: \["nodes"\]\s+verbs: \["get", "list"\]')
        code = "\n".join(l.split("#", 1)[0] for l in rbac.splitlines())
        # nodes/proxy reaches the kubelet API, including exec into any pod on the node
        self.assertNotIn("nodes/proxy", code)
        self.assertNotRegex(code, r'verbs:.*"(create|update|patch|delete|\*)"', "this grant is read-only")

    def test_paging_is_one_page_per_incident_and_the_deliverer_hands_over_a_state_dir(self):
        deliver = (WATCH / "fleet-alert-deliver.sh").read_text()
        self.assertIn('--state-dir "$STATE_DIR"', deliver, "without a state directory every run would page again")
        self.assertIn('dirname "$WORKSPACE")/.oe-watch', deliver,
                      "in Jenkins the incidents live beside the job workspaces, where the baselines already are")
        composer = (WATCH / "fleet-alert.py").read_text()
        for const in ("REMIND_MIN = 30", "ESCALATE_MIN = 120", "SUMMARY_MIN = 24 * 60", "GRACE_MIN = 30"):
            self.assertIn(const, composer)

    def test_an_observation_only_finding_does_not_fail_the_build(self):
        deliver = (WATCH / "fleet-alert-deliver.sh").read_text()
        self.assertIn('[ "$FLEET_MODE" = observation-only ] && [ "$rc" = 1 ]', deliver)
        code = "\n".join(l.split("#", 1)[0] for l in deliver.splitlines())
        self.assertNotRegex(code, r'exit "\$rc"\s*$', "no path may re-raise the raw status after the verdict is set")
        self.assertEqual(3, code.count('exit "$verdict"'))

    def test_delivery_goes_through_the_one_implementation(self):
        # R1a: oe-alert.sh is the single delivery path. A second curl to a webhook is the defect its own
        # header warns about, and Jenkinsfile.morning-autostart's private discord() is that defect.
        for check in NEW_CHECKS:
            stage = self._stage_for(check)
            self.assertIn("fleet-alert-deliver.sh", stage)
            self.assertNotIn("curl", stage, "no stage may POST to a webhook itself")
        deliver = (WATCH / "fleet-alert-deliver.sh").read_text()
        self.assertIn("oe-alert.sh", deliver)
        self.assertIn("alert ", deliver)
        code = "\n".join(l.split("#", 1)[0] for l in deliver.splitlines())
        self.assertNotIn("curl", code, "the deliverer must not carry its own curl")

    def test_no_stage_can_exit_before_the_deliverer_runs(self):
        # Codex implementation review MAJOR-1: `mkdir -p "$STATE" || exit 2` ran in the Jenkins shell BEFORE
        # fleet-alert-deliver.sh, so a state-directory failure sent NO "could not check" message - the one
        # state that must always reach Discord. Everything the check needs is set up INSIDE the wrapped
        # command now, so nothing in the stage may exit on its own.
        for check in NEW_CHECKS:
            stage = self._stage_for(check)
            self.assertNotIn("|| exit 2", stage,
                             "%s must not exit before fleet-alert-deliver.sh runs" % check)
            self.assertNotIn("mkdir -p", stage,
                             "%s must not prepare state outside the wrapped command" % check)
            body = stage[stage.index("script:"):]
            wrapped = {"crash-loop": "crash-loop-run.sh",
                       "kafka-lag": "stateful-check-run.sh kafka-lag.py",
                       "partition-balance": "stateful-check-run.sh partition-balance.py",
                       "readiness": "readiness-run.sh",
                       "dlq-growth": "stateful-check-run.sh dlq-growth.py",
                       "oom": "oom-run.sh",
                       "host-load": "host-load-run.sh",
                       "topic-freshness": "stateful-check-run.sh topic-freshness.py",
                       "kafka-broker": "stateful-check-run.sh kafka-broker.py",
                       "restart-budget": "restart-budget-run.sh"}[check]
            self.assertIn(wrapped, body)
            self.assertLess(body.index("fleet-alert-deliver.sh"), body.index(wrapped),
                            "the deliverer must WRAP the check script, not follow it")

    def test_the_stateful_checks_go_through_the_wrapper(self):
        for check in ("kafka-lag", "partition-balance"):
            self.assertIn("stateful-check-run.sh", self._stage_for(check))
        self.assertIn("crash-loop-run.sh", self._stage_for("crash-loop"))
        for name in ("stateful-check-run.sh", "crash-loop-run.sh", "readiness-run.sh", "oom-run.sh", "host-load-run.sh"):
            text = (WATCH / name).read_text()
            self.assertIn("ERROR:", text, "%s must emit an ERROR: reason the composer can read" % name)
            self.assertIn("exit 2", text, "%s must fail closed as exit 2" % name)

    def test_every_new_stage_is_wrapped_in_catchError(self):
        # So one red check cannot hide another's verdict - the rule the existing two stages already follow.
        for check in NEW_CHECKS:
            self.assertIn("catchError", self._stage_for(check))

    def test_the_scope_file_no_longer_claims_the_job_changes_nothing(self):
        # R6b. The job stays OUT of the permitted-commit guard because it has no DEPLOYMENT effect, but the
        # stated reason may not be left saying it changes nothing now that it POSTs to Discord.
        rows = [l for l in SCOPE.read_text().splitlines() if l.startswith("Jenkinsfile.fleet-watch")]
        self.assertEqual(1, len(rows), "fleet-watch must appear exactly once in the scope file")
        row = rows[0]
        self.assertRegex(row, r"\|\s*out\s*\|", "fleet-watch stays out of the permitted-commit guard")
        self.assertIn("Discord", row,
                      "the reason must name the Discord POST, or it describes a job that no longer exists")

    def test_the_jenkinsfile_header_qualifies_its_read_only_claim(self):
        self.assertIn("Discord", self.jf.split("pipeline {")[0],
                      "the header must say the job delivers to Discord")
        header = self.jf.split("pipeline {")[0]
        for claim in re.findall(r"changes nothing[^\n]*", header):
            self.assertIn("Discord", claim + header[header.index(claim):header.index(claim) + 400],
                          "a bare 'changes nothing' claim must be qualified by the Discord POST")

    # ---- the restart budget (owner rule 2026-11-19: a deployment restarts 5 times, not more) ----------------------------------------------------
    def test_restart_budget_runs_for_every_profile_and_validates_its_mode(self):
        body = self._stage_for("restart-budget")
        self.assertIn("watchedProfiles(params.ENVS, PROFILES)", body)
        self.assertNotIn("findAll", body, "the budget applies to production, dev AND es4 (production only pages for its mandatory set)")
        self.assertIn("RESTART_BUDGET_MODE=${params.RESTART_BUDGET_MODE}", body, "the stage must pass the chosen mode to the wrapper")
        self.assertRegex(body, r"RESTART_BUDGET_MODE in \['enforce', 'dry-run'\]", "restart-from-stage can skip the parameter check")
        self.assertRegex(self.jf, r"choice\(name: 'RESTART_BUDGET_MODE', choices: \['enforce', 'dry-run'\]",
                         "enforce first: it is the default, dry-run is the opt-out")

    def test_the_scope_file_names_the_one_cluster_write(self):
        row = [l for l in SCOPE.read_text().splitlines() if l.startswith("Jenkinsfile.fleet-watch")][0]
        self.assertIn("stop-only", row, "the scope row must say the job now has one cluster write, and that it only stops")
        self.assertIn("Restart budget", row)
        self.assertNotIn("READ-ONLY AGAINST EVERY CLUSTER", row, "that sentence is no longer true")
        self.assertIn("scales it to 0", row)

    def test_the_budget_can_only_stop_never_raise(self):
        src = (WATCH / "restart-budget.py").read_text()
        replicas = re.findall(r'--replicas=(\S+?)["\')]', src)
        self.assertTrue(replicas, "the check must scale something")
        self.assertEqual({"0"}, set(replicas), "restart-budget.py may only ever scale to 0: %s" % replicas)
        self.assertNotRegex(src, r'"(delete|apply|replace|patch|rollout|set)"', "the budget annotates and scales, nothing else")

    def test_the_budget_reads_the_same_mandatory_file_as_the_ci_guard(self):
        src = (WATCH / "restart-budget.py").read_text()
        self.assertIn("prod-mandatory-services.txt", src)
        self.assertTrue((REPO / "config/prod-mandatory-services.txt").is_file())

    def test_the_budget_ranks_both_of_its_classes_as_urgent(self):
        rank = (WATCH / "fleet-alert.py").read_text()
        for cls in ("RESTART_BUDGET", "RESTART_BUDGET_HELD"):
            self.assertRegex(rank, r'"%s": \(5, ' % cls, "%s must be rank 5 or its page is buried" % cls)

    def test_the_checks_exist_and_are_executable_python(self):
        for name in ("crash-loop.py", "kafka-lag.py", "partition-balance.py", "fleet-alert.py", "oom.py", "host-load.py", "restart-budget.py"):
            p = WATCH / name
            self.assertTrue(p.is_file(), "%s is missing" % name)
            compile(p.read_text(), str(p), "exec")

    def test_every_new_check_has_a_negative_control_suite(self):
        for name in ("crash-loop", "kafka-lag", "partition-balance", "fleet-alert", "oom", "host-load", "run-wrappers", "topic-freshness", "kafka-broker", "restart-budget"):
            self.assertTrue((WATCH / ("%s-test.py" % name)).is_file(),
                            "%s has no negative controls" % name)
        self.assertTrue((WATCH / "fleet-alert-deliver-test.sh").is_file(),
                        "the delivery path has no controlled test")

    def _stage_for(self, check):
        names = {"crash-loop": "Crash loops", "kafka-lag": "Consumer lag",
                 "partition-balance": "Partition balance", "readiness": "Readiness", "dlq-growth": "Dead letters",
                 "oom": "Memory kills", "host-load": "Host load",
                 "topic-freshness": "Topic freshness", "kafka-broker": "Kafka broker",
                 "restart-budget": "Restart budget"}
        label = names[check]
        start = self.jf.index("stage('%s')" % label)
        nxt = self.jf.find("    stage('", start + 10)
        return self.jf[start: nxt if nxt > 0 else len(self.jf)]


if __name__ == "__main__":
    unittest.main()
