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
# The session's whole volume profile, so "how many contracts traded between these two strikes" can be
# answered at all. Shaped exactly like CURRENT -- pure compact, -1/-1, one partition -- and
# deliberately NOT like the corpus log, whose byte cap is what gives the record about 87 sessions.
PROFILE = "options.spx.gamma-zone.profile"
TOPICS = (CURRENT, LOG, PROFILE)
# The two that the next folded bar rewrites. Everything true of CURRENT's reset class is true of
# PROFILE's, and this pair is what the rebuildable assertions iterate.
REBUILDABLE = (CURRENT, PROFILE)
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
    return {CURRENT: shaped("compact", "-1", "-1"),
            LOG: shaped("delete", "-1", LOG_BYTES),
            PROFILE: shaped("compact", "-1", "-1")}


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

    def test_the_PROFILE_is_pure_compact_like_current_and_NOT_like_the_log(self):
        """It is a latest-value-per-session view, not a corpus. Giving it the log's shape would put
        a record per folded bar into a byte-capped topic for no reason; giving the log this shape
        would discard the history the service exists to accumulate."""
        d = declarations(self.VARS)
        self.assertIn(PROFILE, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertIn(f"{PROFILE}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"])
        self.assertIn(f"{PROFILE}=-1", d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])
        # No byte cap: a compacted latest-value topic has one record per session to keep.
        self.assertNotIn(f"{PROFILE}={LOG_BYTES}", d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"])

    def test_the_PROFILE_is_preserved_by_NOTHING_because_it_is_rebuilt(self):
        """Deliberately not in any preservation list. The service re-folds the whole session from a
        20-hour lookback on every start, so the next folded bar rewrites it. Preserving it would
        pin a stale copy of something that regenerates in seconds."""
        d = declarations(self.VARS + ["OPTIONS_EDGE_NEVER_RECREATE_TOPICS",
                                      "OPTIONS_EDGE_RESET_PRESERVED_TOPICS",
                                      "OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS",
                                      "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        self.assertIn(PROFILE, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        for preserved in ("OPTIONS_EDGE_NEVER_RECREATE_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS",
                          "OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"):
            self.assertNotIn(PROFILE, d[preserved], preserved)

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
    def test_it_creates_ALL_THREE_topics_at_the_declared_shapes_on_an_empty_cluster(self):
        rc, out, _calls, state = run_barrier({})
        self.assertEqual(0, rc, out)
        self.assertEqual(shaped("compact", "-1", "-1")["configs"], state["topics"][CURRENT]["configs"])
        self.assertEqual(shaped("delete", "-1", LOG_BYTES)["configs"], state["topics"][LOG]["configs"])
        # The profile takes CURRENT's shape, not the log's: a byte cap on a latest-value-per-session
        # topic would be meaningless, and the log's shape would make it a second corpus.
        self.assertEqual(shaped("compact", "-1", "-1")["configs"], state["topics"][PROFILE]["configs"])
        self.assertEqual(1, state["topics"][LOG]["partitions"])
        self.assertEqual(1, state["topics"][PROFILE]["partitions"])

    def test_it_passes_and_alters_nothing_when_both_topics_are_already_right(self):
        before = right_topics()
        rc, out, calls, state = run_barrier(json.loads(json.dumps(before)))
        self.assertEqual(0, rc, out)
        self.assertEqual(before, state["topics"], "an existing correct topic is never altered")
        self.assertFalse([c for c in calls if "--alter" in c or "--create" in c], calls)

    def test_it_REFUSES_a_profile_it_finds_on_delete(self):
        """A profile left on delete would age its own records out, so the strike-range query would
        silently start answering about part of a session."""
        topics = right_topics()
        topics[PROFILE] = shaped("delete", "-1", "-1")
        rc, out, calls, _state = run_barrier(topics)
        self.assertNotEqual(0, rc, out)
        self.assertFalse([c for c in calls if "--alter" in c], "it refuses; it never repairs")

    def test_it_REFUSES_a_renamed_profile_topic(self):
        """The barrier only ever ensures the DECLARED name. An override would create a topic with
        gamma-zone's shape under a name topics.env does not declare."""
        rc, out, calls, _state = run_barrier(right_topics(),
                                             extra_env={"KAFKA_GAMMA_ZONE_PROFILE_TOPIC": "somewhere.else"})
        self.assertNotEqual(0, rc, out)
        self.assertIn("somewhere.else", out)
        self.assertFalse([c for c in calls if "--create" in c or "--alter" in c], calls)

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


class ManifestBinding(unittest.TestCase):
    """The image label and the manifest must agree EXACTLY, in BOTH directions."""

    BARS_SUBJECT = "es.futures.footprint.bars-value"
    BARS_ID = 2034

    def entry(self):
        manifest = json.loads(read("schema-manifest/manifest.json"))
        return next(e for e in manifest["entries"] if e.get("subject") == self.BARS_SUBJECT)

    def test_the_service_is_bound_as_a_consumer_of_the_footprint_bars_subject(self):
        # The service reads the tape through CanonicalAvroKafka, so it declares that subject in its
        # own schema-requirements.json, which becomes the image label. service-deploy compares the
        # label against THIS manifest and refuses on a binding the manifest does not govern --
        # AFTER it has already built the image. A declared input with no manifest binding fails the
        # deploy just as surely as an undeclared one.
        binding = next((c for c in self.entry()["consumers"] if c["service"] == SVC), None)
        self.assertIsNotNone(binding, f"{SVC} is not a declared consumer of {self.BARS_SUBJECT}")
        self.assertEqual([self.BARS_ID], binding["accepts"])
        self.assertEqual(["dev"], binding["environments"], "the service renders on dev alone")

    def test_the_service_produces_no_avro_subject_anywhere_in_the_manifest(self):
        # Its two outputs are JSON on the wire, so they are not subjects at all. A producer binding
        # here would promise a subject that does not exist.
        manifest = json.loads(read("schema-manifest/manifest.json"))
        for e in manifest["entries"]:
            for producer in e.get("producers", []):
                self.assertNotEqual(SVC, producer.get("service"),
                                    f"{SVC} must produce no Avro subject, but is bound to "
                                    f"{e.get('subject')}")


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

    def test_the_input_topic_is_named_in_full_on_the_pod(self):
        """The topic the whole service reads, which its code default does NOT resolve to.

        The code default is the bare "futures.footprint.bars" and TOPIC_PREFIX is empty on dev, so
        without this the service resolves a topic that does not exist. It did exactly that on the
        first real rollout and refused to start: "input topic has no partitions:
        futures.footprint.bars". It must match the topic the manifest binding is about, and the name
        every other reader of this tape uses.
        """
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        env = {e["name"]: e["value"] for e in container["env"]}
        self.assertEqual("es.futures.footprint.bars", env["KAFKA_ES_FOOTPRINT_BARS_TOPIC"])
        # the same name the feed-gateway base manifest uses, so there is one spelling in the fleet
        self.assertIn("value: es.futures.footprint.bars", read("k8s/base/feed-gateway-deployment.yaml"))
        # and it is the topic the manifest subject this service declares is carried on
        entry = next(e for e in json.loads(read("schema-manifest/manifest.json"))["entries"]
                     if e.get("subject") == "es.futures.footprint.bars-value")
        self.assertIn("es.futures.footprint.bars",
                      [t["name"] for t in entry.get("topics", [])])

    def test_the_measured_knobs_are_on_the_pod_with_their_measured_values(self):
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        env = {e["name"]: e["value"] for e in container["env"]}
        # Measured against the live dev tape: the producer stamps a versioned contract identity, and
        # the plain "ES" this was first written with matches NOTHING on that topic -- the first live
        # run folded 0 bars and refused 0, which looked exactly like a service with no input.
        self.assertEqual("ES.v.0", env["GAMMA_ZONE_SYMBOL"])
        self.assertEqual("1m", env["GAMMA_ZONE_TIMEFRAME"])
        # The reversal threshold SCALES with the session's own close range. All three numbers
        # are pinned: a floor alone would let a silent ceiling or fraction change the rule,
        # and the fraction is the one measured over four live sessions (ReversalRule).
        self.assertEqual("500", env["GAMMA_ZONE_REVERSAL_MIN_CENTS"],
                         "floor 5.00 points: the fixed threshold this service shipped with")
        self.assertEqual("5000", env["GAMMA_ZONE_REVERSAL_MAX_CENTS"],
                         "ceiling 50.00 points: too coarse fails silently")
        self.assertEqual("1250", env["GAMMA_ZONE_REVERSAL_RANGE_BPS"],
                         "one eighth of the session close range")
        # The constant it replaced must be gone from the pod's OWN env, not left beside the rule
        # where an operator would read it as still in force. The service no longer reads the name.
        self.assertNotIn("GAMMA_ZONE_REVERSAL_CENTS", env)
        self.assertEqual("50000", env["GAMMA_ZONE_MIN_VOLUME"])
        # 20 hours, so the seek lands before any session's first bar and the roll is OBSERVED rather
        # than assumed. A shorter lookback would publish sessionStartObserved=false all day.
        self.assertEqual("72000000", env["GAMMA_ZONE_LOOKBACK_MS"])
        self.assertEqual("60000", env["GAMMA_ZONE_HEARTBEAT_MS"])

    # This test file names every one of these variables, so a search that counted its own
    # literals would be answering a question about itself. Both searches below exclude it.
    SELF = "tests/test_gamma_zone_deploy.py"

    def _tracked_files_naming(self, token):
        found = subprocess.run(["git", "grep", "-l", token, "--", ".", ":!" + self.SELF],
                               cwd=str(ROOT), capture_output=True, text=True)
        self.assertIn(found.returncode, (0, 1), found.stderr)
        return sorted(line for line in found.stdout.splitlines() if line.strip())

    def test_the_retired_reversal_variable_is_nowhere_in_THIS_REPO_at_all(self):
        """The retired name must not reach the pod by ANY path this repo controls.

        An earlier version of this test walked k8s/infra for the literal and pinned the two
        envFrom source names, and called that "not injected through envFrom". It was not: this
        repo also GENERATES options-edge-runtime-secrets (Jenkinsfile, Jenkinsfile.secrets-sync),
        so the name could have been added there while the test stayed green, and a literal walk of
        one directory is neither as broad as the claim nor as precise as a rendered-ConfigMap
        assertion (Codex round 2).

        A tracked-file search is strictly stronger than both: it covers k8s/infra, every overlay,
        both secret generators, every script and every manifest at once. Its one hole is a key
        assembled at runtime from pieces, which nothing in this repo does to any env name -- said
        here rather than left for someone to assume away.
        """
        self.assertEqual([], self._tracked_files_naming("GAMMA_ZONE_REVERSAL_CENTS"),
                         "the retired GAMMA_ZONE_REVERSAL_CENTS is still named in: "
                         + ", ".join(self._tracked_files_naming("GAMMA_ZONE_REVERSAL_CENTS")))

    def test_the_three_that_replaced_it_are_really_ON_the_pod_not_just_in_this_test(self):
        """PARSED, not grepped, and in BOTH the base and the render the deploy actually applies.

        Two earlier drafts of this check were wrong in the same direction. The first searched the
        whole repo including this file, so its own literals satisfied it and renaming a variable
        throughout the manifests left it green. The second excluded this file but was still a text
        search, and both manifests NAME all three variables in their comments -- so a rule deleted
        from env but described in a comment would have passed (Codex round 3). Reading the
        container's env is the only form of this check that cannot be satisfied by prose.
        """
        expected = {"GAMMA_ZONE_REVERSAL_MIN_CENTS": "500",
                    "GAMMA_ZONE_REVERSAL_MAX_CENTS": "5000",
                    "GAMMA_ZONE_REVERSAL_RANGE_BPS": "1250"}
        for path in (BASE, SLICE):
            container, = yaml.safe_load(read(path))["spec"]["template"]["spec"]["containers"]
            env = {e["name"]: e.get("value") for e in container["env"]}
            for name, value in expected.items():
                self.assertIn(name, env, name + " is not in the container env of " + path)
                self.assertEqual(value, env[name], name + " differs in " + path)
            # The render the deploy applies must not carry the retired one either, whatever the
            # base says: the slice is generated, and a generator is a place a value can reappear.
            self.assertNotIn("GAMMA_ZONE_REVERSAL_CENTS", env, path)

    def test_the_envFrom_sources_are_pinned_so_a_NEW_one_is_a_reviewed_diff(self):
        """What this does and does not establish.

        It does NOT prove the retired variable is absent -- the test above does that. It pins the
        two objects this Deployment imports, so adding a third becomes a diff somebody has to
        justify rather than a silent widening of what reaches the container.

        Note also that an injected key could not have OVERRIDDEN an explicit container.env entry
        of the same name: Kubernetes gives container.env precedence over envFrom. The risk is
        injection where the pod names nothing, which is exactly the retired variable's case.
        """
        container, = yaml.safe_load(read(BASE))["spec"]["template"]["spec"]["containers"]
        sources = [e.get("configMapRef", e.get("secretRef", {})).get("name")
                   for e in container.get("envFrom", [])]
        self.assertEqual(["options-edge-config", "options-edge-runtime-secrets"], sources)

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
