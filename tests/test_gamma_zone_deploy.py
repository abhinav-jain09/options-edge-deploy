# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring of gamma-zone-service (phase 1: it measures the zone and publishes; it emits no signal and never trades).

Two dev-only topics, options.spx.gamma-zone.{current,log}, their first-deploy barrier, the reset lists, and the service itself: DEV ONLY, single replica, strategy Recreate, no volume, no probes
(it serves no HTTP); the services.yaml entry, the dev overlay resource and image remap, the generated slice, the image wiring of every deploy script, and the SERVICE choice and barrier arm of the
service deploy.

The assertion this module exists for is THE LOG IS NEVER COMPACTED. Its key is the session, so a cleanup.policy of compact would keep one record per session and discard exactly the intraday
history the topic exists to accumulate - which is the whole reason the service was built. A single wrong policy on that topic silently destroys the corpus, so it is asserted in the declaration, in
the barrier, and in the barrier's refusal of a compacted topic it finds already there.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fake_kafka_cli.py"
BARRIER = ROOT / "scripts/kafka/ensure-gamma-zone-topics.sh"
SVC = "gamma-zone"
BASE = f"k8s/services/{SVC}/base/{SVC}-deployment.yaml"
SLICE = f"k8s/services/{SVC}/overlays/dev/manifest.yaml"
CURRENT = "options.spx.gamma-zone.current"
LOG = "options.spx.gamma-zone.log"
TOPICS = (CURRENT, LOG)
LOG_BYTES = "1073741824"          # 1 GiB; the arithmetic is in the topics.env block
INPUT_TOPIC = "es.futures.footprint.bars"


def read(rel: str) -> str:
    return (ROOT / rel).read_text()


def declarations(variables, environment=None):
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    if environment is not None:
        env["ENVIRONMENT"] = environment
    script = f'source "{ROOT}/scripts/kafka/topics.env"; ' + "; ".join(f'echo "{v}=${{{v}:-}}"' for v in variables)
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout
    return {line.split("=", 1)[0]: line.split("=", 1)[1].split() for line in out.splitlines()}


def shaped(policy, retention, bytes_="-1", parts=1, isr=1, maxbytes="1048576"):
    return {"partitions": parts, "rf": 1, "end": 0,
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_,
                        "max.message.bytes": maxbytes, "min.insync.replicas": str(isr)}}


def run_barrier(topics, environment="dev", extra_env=None):
    tmp = Path(tempfile.mkdtemp())
    bin_dir = tmp / "bin"
    bin_dir.mkdir()
    for n in ("kafka-topics", "kafka-configs"):
        shutil.copy(FAKE, bin_dir / n)
        (bin_dir / n).chmod(0o755)
    state, calls = tmp / "state.json", tmp / "calls"
    state.write_text(json.dumps({"topics": topics}))
    calls.touch()
    env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
    env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls),
               KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1")
    if environment is not None:
        env["ENVIRONMENT"] = environment
    if extra_env:
        env.update(extra_env)
    r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())


def right_topics():
    return {CURRENT: shaped("compact", "-1", "-1"), LOG: shaped("delete", "-1", LOG_BYTES)}


class Topics(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES",
            "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS", "OPTIONS_EDGE_COMPACTED_TOPICS",
            "OPTIONS_EDGE_ES4_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS",
            "OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"]

    def test_both_topics_are_declared_once_at_one_partition_exact(self):
        d = declarations(self.VARS)
        for t in TOPICS:
            # Every declaration of this topic, whatever its partition count. Counting only ":1"
            # would have passed an extra "topic:2" sitting beside it, which is the shape the
            # EXACT_PARTITION list exists to prevent and which apply-topics would then try to
            # reconcile by RECREATING the topic.
            declared = [x for x in d["OPTIONS_EDGE_TOPICS"] if x.split(":")[0] == t]
            self.assertEqual([f"{t}:1"], declared, f"{t} must be declared exactly once, at 1 partition")
            self.assertEqual(d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"].count(t), 1, t)
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])

    def test_the_corpus_log_can_never_be_delete_and_recreated(self):
        d = declarations(self.VARS + ["OPTIONS_EDGE_NEVER_RECREATE_TOPICS"])
        # Reset-preserved, dev-clean-preserved and premarket-preserved do NOT close
        # apply-topics.sh's delete-and-recreate arm, which a global
        # KAFKA_RECREATE_MISMATCHED_TOPICS=true reaches on any partition mismatch and which
        # discards EVERY record. Three preservation lists and the fourth left open is preservation
        # on paper.
        self.assertIn(LOG, d["OPTIONS_EDGE_NEVER_RECREATE_TOPICS"])
        # `current` is deliberately NOT listed: the next folded bar refills it.
        self.assertNotIn(CURRENT, d["OPTIONS_EDGE_NEVER_RECREATE_TOPICS"])

    def resolved_policy(self, topic):
        """What apply-topics would actually SET on this topic, through the repo's own resolver."""
        # CLEANUP_POLICY is apply-topics.sh's own default for an unlisted topic, taken from the same
        # KAFKA_TOPIC_CLEANUP_POLICY the deployed ConfigMap sets. Resolving without it would read an
        # empty policy and prove nothing.
        script = (f'source "{ROOT}/scripts/kafka/topics.env"; '
                  f'source "{ROOT}/scripts/kafka/topic-shape.lib.sh"; '
                  'CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"; '
                  f'topic_cleanup_policy "{topic}"; echo; topic_retention_ms "{topic}"')
        env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
        env["ENVIRONMENT"] = "dev"
        out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True,
                             env=env).stdout.split()
        return out[0], out[1]

    def test_the_log_RESOLVES_to_delete_and_appears_in_no_compaction_list(self):
        # The one assertion this module exists for. The key is the session, so compaction would keep
        # one record per session and discard the intraday history the topic exists to accumulate.
        #
        # Asserted through the repo's OWN resolver, not only by absence from the compaction lists: a
        # change to the default policy could compact this topic while every absence check stayed
        # green (Codex round 2). This is what apply-topics would actually set.
        policy, retention_ms = self.resolved_policy(LOG)
        self.assertEqual("delete", policy, "the corpus must never be compacted")
        self.assertEqual("-1", retention_ms, "the corpus keeps every record; only the byte cap removes one")
        # and `current` resolves to compact, so the resolver is genuinely being consulted
        self.assertIn("compact", self.resolved_policy(CURRENT)[0])

        d = declarations(self.VARS)
        self.assertNotIn(LOG, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(LOG, d["OPTIONS_EDGE_COMPACTED_TOPICS"])
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        self.assertEqual(bytes_[LOG], LOG_BYTES)

    def test_the_current_topic_is_pure_compact(self):
        d = declarations(self.VARS)
        self.assertIn(CURRENT, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(CURRENT, d["OPTIONS_EDGE_COMPACTED_TOPICS"])
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        self.assertEqual((retention[CURRENT], bytes_[CURRENT]), ("-1", "-1"))

    def test_the_log_survives_every_reset_and_the_current_topic_is_rebuildable(self):
        d = declarations(self.VARS)
        # An emission cannot be re-derived once the footprint tape has aged out of its own window,
        # and a dev clean would otherwise destroy the corpus on a service whose only purpose is to
        # accumulate one.
        self.assertIn(LOG, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(LOG, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"])
        self.assertNotIn(LOG, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        # the next folded bar refills `current`
        self.assertIn(CURRENT, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertNotIn(CURRENT, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertNotIn(CURRENT, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"])

    def test_production_declares_neither_topic_in_ANY_variable(self):
        # EVERY OPTIONS_EDGE_* variable the file defines, enumerated by the shell rather than by a
        # hand-written list: a hand-written list cannot cover a variable added later, which is
        # exactly where a production leak would hide.
        env = {k: v for k, v in os.environ.items() if k != "ENVIRONMENT"}
        env["ENVIRONMENT"] = "production"
        script = (f'source "{ROOT}/scripts/kafka/topics.env"; '
                  'for v in $(compgen -v OPTIONS_EDGE_); do printf "%s=%s\n" "$v" "${!v}"; done')
        out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True,
                             env=env).stdout
        checked = 0
        for line in out.splitlines():
            name, _, value = line.partition("=")
            checked += 1
            for t in TOPICS:
                self.assertNotIn(t, value, f"{t} leaked into {name} on production")
        self.assertGreater(checked, 20, "the enumeration found almost no variables; it is not working")

    def test_the_input_topic_is_not_redeclared_here(self):
        # It belongs to es-cvd-service. A second declaration of someone else's topic is how a shape
        # gets two owners that disagree.
        block = read("scripts/kafka/topics.env").split("--- gamma-zone-service outputs")[1]
        self.assertNotIn(f"{INPUT_TOPIC}:", block)


class Barrier(unittest.TestCase):
    def test_it_creates_both_topics_at_the_declared_shapes_on_an_empty_cluster(self):
        rc, out, _calls, state = run_barrier({})
        self.assertEqual(0, rc, out)
        self.assertEqual(shaped("compact", "-1", "-1")["configs"], state["topics"][CURRENT]["configs"])
        self.assertEqual(shaped("delete", "-1", LOG_BYTES)["configs"], state["topics"][LOG]["configs"])
        self.assertEqual(1, state["topics"][LOG]["partitions"])

    def test_it_passes_and_alters_nothing_when_both_topics_are_already_right(self):
        before = right_topics()
        rc, out, calls, state = run_barrier(json.loads(json.dumps(before)))
        self.assertEqual(0, rc, out)
        self.assertEqual(before, state["topics"], "an existing correct topic is never altered")
        self.assertFalse([c for c in calls if "--alter" in c or "--create" in c], calls)

    def test_it_REFUSES_a_log_it_finds_compacted(self):
        topics = right_topics()
        topics[LOG] = shaped("compact", "-1", LOG_BYTES)
        rc, out, calls, _state = run_barrier(topics)
        self.assertNotEqual(0, rc, out)
        self.assertIn(LOG, out)
        # refused, never repaired: altering a cleanup.policy in place is not this script's business,
        # and the operator has to see that the corpus topic was wrong
        self.assertFalse([c for c in calls if "--alter" in c], calls)

    def test_it_REFUSES_a_log_whose_byte_cap_is_not_the_declared_one(self):
        topics = right_topics()
        topics[LOG] = shaped("delete", "-1", "268435456")
        rc, out, _calls, _state = run_barrier(topics)
        self.assertNotEqual(0, rc, out)
        self.assertIn("retention.bytes", out)

    def test_it_REFUSES_a_log_with_a_finite_time_retention(self):
        topics = right_topics()
        topics[LOG] = shaped("delete", "86400000", LOG_BYTES)
        rc, out, _calls, _state = run_barrier(topics)
        self.assertNotEqual(0, rc, out)

    def test_it_REFUSES_a_current_topic_it_finds_on_delete(self):
        topics = right_topics()
        topics[CURRENT] = shaped("delete", "-1", "-1")
        rc, out, _calls, _state = run_barrier(topics)
        self.assertNotEqual(0, rc, out)

    def test_it_REFUSES_a_RENAMED_topic_because_nothing_preserves_that_name(self):
        for variable in ("KAFKA_GAMMA_ZONE_CURRENT_TOPIC", "KAFKA_GAMMA_ZONE_LOG_TOPIC"):
            rc, out, calls, _state = run_barrier(right_topics(), extra_env={variable: "somewhere.else"})
            self.assertNotEqual(0, rc, f"{variable}: {out}")
            self.assertIn(variable, out)
            self.assertEqual([], calls, "it refuses before it touches the cluster")
        # and the declared names themselves are accepted when passed explicitly
        rc, out, _calls, _state = run_barrier(right_topics(), extra_env={
            "KAFKA_GAMMA_ZONE_CURRENT_TOPIC": CURRENT, "KAFKA_GAMMA_ZONE_LOG_TOPIC": LOG})
        self.assertEqual(0, rc, out)

    def test_it_REFUSES_to_run_outside_dev(self):
        for environment in ("production", "es4", "", None):
            rc, out, calls, _state = run_barrier(right_topics(), environment=environment)
            self.assertNotEqual(0, rc, f"{environment!r}: {out}")
            self.assertIn("DEV ONLY", out)
            self.assertEqual([], calls, "it refuses before it touches the cluster")


class Service(unittest.TestCase):
    def test_services_yaml_declares_it_dev_only_single_deployment(self):
        entry = next(s for s in yaml.safe_load(read("services.yaml"))["services"] if s["name"] == SVC)
        self.assertEqual(["dev"], entry["envs"])
        self.assertEqual("options-edge-gamma-zone", entry["image"])
        self.assertEqual(["gamma-zone-service"], entry["deployments"])
        self.assertTrue(entry["sliceGenerated"])
        self.assertEqual("standalone", entry["managedBy"])

    def test_one_replica_recreate_and_no_state_volume(self):
        spec = yaml.safe_load(read(BASE))["spec"]
        # One writer owns both outputs: the log is a totally ordered record of what the service said
        # and when, and both topics are keyed by session. Recreate fences the predecessor rather than
        # running two writers side by side.
        self.assertEqual(1, spec["replicas"])
        self.assertEqual("Recreate", spec["strategy"]["type"])
        pod = spec["template"]["spec"]
        self.assertNotIn("volumes", pod, "the session fold is rebuilt from the tape, not stored")
        container, = pod["containers"]
        self.assertNotIn("volumeMounts", container)

    def test_no_probes_and_no_ports_because_it_serves_no_http(self):
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        for key in ("ports", "readinessProbe", "livenessProbe"):
            self.assertNotIn(key, container, f"{key} would promise an endpoint this service has not got")

    def test_the_measured_knobs_are_on_the_pod_with_their_measured_values(self):
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        env = {e["name"]: e["value"] for e in container["env"]}
        # Measured against the live dev tape: the producer stamps a versioned contract identity, and
        # the plain "ES" this was first written with matches NOTHING on that topic -- the first live
        # run folded 0 bars and refused 0, which looked exactly like a service with no input.
        self.assertEqual("ES.v.0", env["GAMMA_ZONE_SYMBOL"])
        self.assertEqual("1m", env["GAMMA_ZONE_TIMEFRAME"])
        self.assertEqual("500", env["GAMMA_ZONE_REVERSAL_CENTS"], "5.00 index points, on closes")
        self.assertEqual("50000", env["GAMMA_ZONE_MIN_VOLUME"])
        # 20 hours, so the seek lands before any session's first bar and the roll is OBSERVED rather
        # than assumed. A shorter lookback would publish sessionStartObserved=false all day.
        self.assertEqual("72000000", env["GAMMA_ZONE_LOOKBACK_MS"])
        self.assertEqual("60000", env["GAMMA_ZONE_HEARTBEAT_MS"])

    def test_no_cpu_limit_so_the_fold_is_never_throttled(self):
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        self.assertNotIn("cpu", container["resources"]["limits"])
        self.assertIn("memory", container["resources"]["limits"])

    def test_the_generated_slice_matches_the_dev_overlay_render(self):
        rendered = yaml.safe_load(read(SLICE))
        self.assertEqual("gamma-zone-service", rendered["metadata"]["name"])
        self.assertEqual(1, rendered["spec"]["replicas"])
        container, = rendered["spec"]["template"]["spec"]["containers"]
        # the dev image remap in k8s/overlays/dev is what the slice mirrors
        self.assertEqual("host.docker.internal:5001/options-edge-gamma-zone:dev", container["image"])

    def test_the_dev_overlay_carries_the_resource_and_the_image_remap(self):
        overlay = read("k8s/overlays/dev/kustomization.yaml")
        self.assertIn("- ../../services/gamma-zone/base", overlay)
        self.assertIn("192.168.100.252:5000/options-edge-gamma-zone", overlay)
        self.assertIn("host.docker.internal:5001/options-edge-gamma-zone", overlay)

    def test_the_image_is_wired_through_every_deploy_script(self):
        self.assertIn("GAMMA_ZONE_IMAGE", read("scripts/deploy/apply.sh"))
        self.assertIn("GAMMA_ZONE_IMAGE", read("scripts/deploy/image-lock.sh"))
        self.assertIn("GAMMA_ZONE_IMAGE=", read("scripts/deploy/resolve-images.sh"))
        self.assertIn('("gamma-zone-service", "gamma-zone"): "GAMMA_ZONE_IMAGE"',
                      read("scripts/deploy/verify-running-images.sh"))
        self.assertIn("gamma-zone-service: host.docker.internal:5001/options-edge-gamma-zone:dev",
                      read("image-tags/dev.yaml"))

    def test_the_service_deploy_offers_it_and_runs_its_barrier(self):
        jenkinsfile = read("Jenkinsfile.service-deploy")
        self.assertIn("'gamma-zone'", jenkinsfile)
        self.assertIn("gamma-zone) scripts/kafka/ensure-gamma-zone-topics.sh ;;", jenkinsfile)
        # and the barrier arm is gated on the permitted-SHA guard, like every other one
        arm = re.search(r"^.*\['line-acceptance'.*'gamma-zone'\].*$", jenkinsfile, re.M).group(0)
        self.assertIn("PERMITTED_SHA_GUARD == 'PASSED'", arm)
        self.assertIn("DEPLOY_WORKSPACE_PERMITTED == 'PASSED'", arm)
        self.assertIn("!params.DEPLOY_DRY_RUN", arm)

    def test_nothing_references_it_outside_dev(self):
        for rel in ("k8s/base/kustomization.yaml", "k8s/overlays/production/kustomization.yaml",
                    "k8s/overlays/experiment/kustomization.yaml", "image-tags/production.yaml"):
            self.assertNotIn("gamma-zone", read(rel), rel)
        self.assertFalse(list((ROOT / f"k8s/services/{SVC}/overlays").glob("production")))
        self.assertFalse(list((ROOT / f"k8s/services/{SVC}/overlays").glob("experiment")))


if __name__ == "__main__":
    unittest.main()
