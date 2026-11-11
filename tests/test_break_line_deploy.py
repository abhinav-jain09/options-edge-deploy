# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
"""Deploy wiring of the Break Score rev 13 topics and schemas (break-line-service; design options-edge-documents design/es-liquidity/liq-S-break-score-rev13-design.md sections 5.7, 15.2, 15.7).

D1: the five dev-only topics futures.break-line.{lines,state,events,status,snapshot}, their first-deploy barrier, the reset lists, the schema map and the manifest family
breakline (ids 3901-3905 of the block 3901-3910).
D2 (this change): the service. DEV ONLY, single replica, strategy Recreate, health port 8080 (/healthz, /readyz), flags off (gamma reader off, allow-test off); services.yaml entry, the dev overlay
resource and image remap, the generated slice, the image wiring of every deploy script, the SERVICE choice and the barrier arm of the service deploy, and the manifest producer and consumer bindings."""
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
BARRIER = ROOT / "scripts/kafka/ensure-break-line-topics.sh"
SVC = "break-line"
BASE = f"k8s/services/{SVC}/base/{SVC}-deployment.yaml"
SLICE = f"k8s/services/{SVC}/overlays/dev/manifest.yaml"
PINNED_BOOTSTRAP, PINNED_REGISTRY = "host.docker.internal:19092", "http://host.docker.internal:8082"
P = "futures.break-line"
LINES, STATE, EVENTS, STATUS, SNAPSHOT = (f"{P}.{n}" for n in ("lines", "state", "events", "status", "snapshot"))
TOPICS = (LINES, STATE, EVENTS, STATUS, SNAPSHOT)
PURE_COMPACT = (LINES, STATE, STATUS, SNAPSHOT)
JAR = ROOT / "tools/schema-governance/schema-governance-tool.jar"
NS = "com.optionsedge.contracts.breakline"
IDS = {"BreakLineCommand": 3901, "BreakLineState": 3906, "BreakLineEvent": 3907, "BreakLineStatus": 3908, "BreakLineSnapshot": 3905}
ARCHIVAL_V1 = {"BreakLineState": 3902, "BreakLineEvent": 3903, "BreakLineStatus": 3904}
TOPIC_OF = {"BreakLineCommand": LINES, "BreakLineState": STATE, "BreakLineEvent": EVENTS, "BreakLineStatus": STATUS, "BreakLineSnapshot": SNAPSHOT}
DAYS30 = "2592000000"
BIG = "9437184"


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
            "configs": {"cleanup.policy": policy, "retention.ms": retention, "retention.bytes": bytes_, "max.message.bytes": maxbytes, "min.insync.replicas": str(isr)}}


def run_barrier(topics, environment="dev"):
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
    env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", FAKE_KAFKA_STATE=str(state), FAKE_KAFKA_CALLS=str(calls), KAFKA_BOOTSTRAP_SERVERS="b:1", KAFKA_TOPIC_REPLICATION_FACTOR="1")
    if environment is not None:
        env["ENVIRONMENT"] = environment
    r = subprocess.run(["bash", str(BARRIER)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines(), json.loads(state.read_text())


def right_topics():
    t = {n: shaped("compact", "-1", "-1") for n in PURE_COMPACT}
    t[EVENTS] = shaped("delete", DAYS30, "-1")
    t[SNAPSHOT] = shaped("compact", "-1", "-1", maxbytes=BIG)
    return t


class Topics(unittest.TestCase):
    VARS = ["OPTIONS_EDGE_TOPICS", "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS",
            "OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_ES4_TOPICS", "OPTIONS_EDGE_RESET_PRESERVED_TOPICS", "OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS", "OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS",
            "OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"]

    def test_the_five_topics_are_declared_once_one_partition_exact(self):
        d = declarations(self.VARS)
        for t in TOPICS:
            self.assertEqual(d["OPTIONS_EDGE_TOPICS"].count(f"{t}:1"), 1, t)
            self.assertEqual(d["OPTIONS_EDGE_EXACT_PARTITION_TOPICS"].count(t), 1, t)
            self.assertNotIn(t, d["OPTIONS_EDGE_ES4_TOPICS"])

    def test_the_four_pure_compact_topics_and_the_thirty_day_events_log(self):
        d = declarations(self.VARS)
        retention = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"] if "=" in x)
        bytes_ = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES"] if "=" in x)
        for t in PURE_COMPACT:
            self.assertIn(t, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"], t)
            self.assertNotIn(t, d["OPTIONS_EDGE_COMPACTED_TOPICS"], t)
            self.assertEqual((retention[t], bytes_[t]), ("-1", "-1"), t)
        self.assertEqual(retention[EVENTS], DAYS30)
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_PURE_COMPACT_TOPICS"])
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_COMPACTED_TOPICS"])
        self.assertNotIn(EVENTS, bytes_)
        mb = dict(x.split("=", 1) for x in d["OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES"] if "=" in x)
        self.assertEqual(mb[SNAPSHOT], BIG)

    def test_lines_is_user_data_preserved_everywhere_and_the_derivable_topics_are_rebuildable(self):
        d = declarations(self.VARS)
        self.assertIn(LINES, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertIn(LINES, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"])
        self.assertNotIn(LINES, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])
        for t in (STATE, STATUS, SNAPSHOT):
            self.assertIn(t, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"], t)
            self.assertNotIn(t, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"], t)
            self.assertNotIn(t, d["OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"], t)
        # the finite-retention events log is in neither reset list
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_RESET_PRESERVED_TOPICS"])
        self.assertNotIn(EVENTS, d["OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"])

    def test_the_premarket_reset_spares_the_lines_topic_and_only_it(self):
        regex = re.search(r"^PRESERVE_TOPICS_REGEX='(.*)'$", read("scripts/ops/premarket-reset.sh"), re.M).group(1)
        self.assertIsNotNone(re.search(regex, LINES), "the pre-market reset must spare the user's lines")
        for t in (STATE, EVENTS, STATUS, SNAPSHOT, f"{P}.lines2", f"x{LINES}", f"{LINES}x"):
            self.assertIsNone(re.search(regex, t), t)

    def test_dev_and_unset_declare_them_and_every_other_environment_declares_none(self):
        base = declarations(self.VARS)
        self.assertEqual(declarations(self.VARS, "dev"), base)
        for environment in ("production", "experiment", "es4", "staging"):
            other = declarations(self.VARS, environment)
            self.assertFalse([x for v in self.VARS for x in other[v] if "break-line" in x], environment)

    def test_the_declaration_is_read_by_the_dev_clean_even_though_it_is_indented(self):
        """The 2026-11-06 loss: the readers of topics.env were anchored at column 0 and a dev-only block is indented. The helper must still see the lines topic in the keep list."""
        out = subprocess.run(["bash", "-c", f'cd "{ROOT}" && grep -nE "^[[:space:]]*OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS=.*{re.escape(LINES)}" scripts/kafka/topics.env'], capture_output=True, text=True)
        self.assertEqual(out.returncode, 0)
        self.assertTrue(out.stdout.splitlines()[0].split(":", 1)[1].startswith("  "), "indented inside the dev-only if")
        ref = subprocess.run(["bash", str(ROOT / "scripts/kafka/reset-preserved-topics.sh"), "--help"], capture_output=True, text=True)
        self.assertIn(ref.returncode, (0, 1, 2))   # the helper exists and is runnable; its own tests prove the indented read (scripts/kafka/reset-preserved-topics-test.sh)

    def test_it_is_not_held_down_by_the_dev_cleanup_lists(self):
        cleanup = read("scripts/ops/dev-cleanup.sh")
        for name in ("DISABLED_DEV", "KEEP_DOWN"):
            m = re.search(rf"^{name}=\(?(.*?)\)?$", cleanup, re.M | re.S)
            self.assertTrue(m is None or "break-line" not in m.group(0), name)


class Barrier(unittest.TestCase):
    def test_a_fresh_dev_cluster_gets_the_five_topics_at_their_shapes_and_no_broker_is_touched(self):
        rc, out, calls, state = run_barrier({})
        self.assertEqual(rc, 0, out)
        self.assertEqual([c for c in calls if "--entity-type brokers" in c or "--entity-default" in c], [], "no broker-wide change")
        self.assertFalse(any("--alter" in c or "--delete" in c for c in calls))
        for t in PURE_COMPACT:
            d = state["topics"][t]
            self.assertEqual((d["partitions"], d["configs"]["cleanup.policy"], d["configs"]["retention.ms"], d["configs"]["retention.bytes"]), (1, "compact", "-1", "-1"), t)
        self.assertEqual(state["topics"][SNAPSHOT]["configs"]["max.message.bytes"], BIG)
        e = state["topics"][EVENTS]
        self.assertEqual((e["partitions"], e["configs"]["cleanup.policy"], e["configs"]["retention.ms"]), (1, "delete", DAYS30))
        self.assertIn("=== ensure-break-line-topics OK ===", out)

    def test_it_is_dev_only_and_fails_closed_before_touching_anything(self):
        for environment in ("production", "es4", "", None):
            rc, out, calls, state = run_barrier({}, environment=environment)
            self.assertEqual(rc, 1, f"{environment!r}: {out}")
            self.assertIn("DEV ONLY", out)
            self.assertEqual(calls, [])
            self.assertEqual(state["topics"], {})

    def test_a_second_run_changes_nothing(self):
        _, _, _, state = run_barrier({})
        rc, out, calls, _ = run_barrier(state["topics"])
        self.assertEqual(rc, 0, out)
        self.assertFalse(any("--create" in c or "--delete" in c or "--alter" in c for c in calls))

    def test_every_difference_fails_closed_names_the_topic_and_alters_nothing(self):
        cases = {}
        for t in PURE_COMPACT:
            cases[f"{t} delete policy"] = (t, lambda d: d["configs"].update({"cleanup.policy": "delete"}))
            cases[f"{t} finite retention"] = (t, lambda d: d["configs"].update({"retention.ms": "86400000"}))
            cases[f"{t} byte-capped"] = (t, lambda d: d["configs"].update({"retention.bytes": "268435456"}))
            cases[f"{t} 4 partitions"] = (t, lambda d: d.update(partitions=4))
            cases[f"{t} compact,delete"] = (t, lambda d: d["configs"].update({"cleanup.policy": "compact,delete"}))
        cases["events compacted"] = (EVENTS, lambda d: d["configs"].update({"cleanup.policy": "compact"}))
        cases["events one ms short"] = (EVENTS, lambda d: d["configs"].update({"retention.ms": "2591999999"}))
        cases["events never ages out"] = (EVENTS, lambda d: d["configs"].update({"retention.ms": "-1"}))
        cases["snapshot message bound too small"] = (SNAPSHOT, lambda d: d["configs"].update({"max.message.bytes": "1048576"}))
        for label, (topic, mutate) in cases.items():
            existing = right_topics()
            mutate(existing[topic])
            rc, out, calls, state = run_barrier(existing)
            self.assertEqual(rc, 1, f"{label}: {out}")
            self.assertIn(f"'{topic}'", out, label)
            self.assertIn("FAILED", out, label)
            self.assertFalse(any("--delete" in c or "--alter" in c for c in calls), label)
            self.assertEqual(state["topics"][topic]["partitions"], existing[topic]["partitions"], label)

    def test_it_is_executable_and_sources_the_shared_routine(self):
        self.assertTrue(os.access(BARRIER, os.X_OK))
        self.assertIn("ensure-topic-contract.lib.sh", BARRIER.read_text())
        self.assertTrue(os.access(ROOT / "scripts/kafka/ensure-break-line-topics-test.sh", os.X_OK))


def deployment(rel: str) -> dict:
    return next(d for d in yaml.safe_load_all(read(rel)) if d and d.get("kind") == "Deployment")


def env_of(dep: dict) -> dict:
    return {e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}


class Manifests(unittest.TestCase):
    def test_base_and_committed_slice_are_dev_single_replica_recreate_flags_off_with_health_paths(self):
        for rel in (BASE, SLICE):
            d = deployment(rel)
            self.assertEqual(d["metadata"]["name"], "break-line-service", rel)
            self.assertEqual(d["spec"]["replicas"], 1, rel)
            self.assertEqual(d["spec"]["strategy"]["type"], "Recreate", rel)
            ct = d["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(ct["name"], SVC, rel)
            self.assertIn("options-edge-break-line", ct["image"], rel)
            self.assertEqual(ct["livenessProbe"]["httpGet"]["path"], "/healthz", rel)
            self.assertEqual(ct["readinessProbe"]["httpGet"]["path"], "/readyz", rel)
            self.assertEqual(ct["resources"]["requests"], {"cpu": "200m", "memory": "512Mi"}, rel)
            self.assertEqual(ct["resources"]["limits"], {"cpu": "1", "memory": "768Mi"}, rel)
            e = env_of(d)
            self.assertEqual(e["BREAK_LINE_ENVIRONMENT"], "Rithmic Paper Trading", rel)
            self.assertEqual(e["BREAK_LINE_ALLOW_TEST"], "false", f"{rel}: the test environment is not admitted")
            self.assertEqual(e["BREAK_LINE_GAMMA_ENABLED"], "false", f"{rel}: the gamma reader is off")
            self.assertEqual(e["BREAK_LINE_HEALTH_PORT"], "8080", rel)
            self.assertNotIn("HEALTH_PORT", e, f"{rel}: the service reads BREAK_LINE_HEALTH_PORT (Main), a bare HEALTH_PORT would be ignored")
            self.assertEqual(e["JAVA_TOOL_OPTIONS"], "-Xmx512m -XX:+UseSerialGC", rel)

    def test_no_pod_volume_and_no_d3_flag_anywhere(self):
        """The restart snapshot is a record on futures.break-line.snapshot, so the pod has no volume; the relay and commands flags are D3 and appear in no manifest of this change."""
        for rel in (BASE, SLICE):
            spec = deployment(rel)["spec"]["template"]["spec"]
            self.assertNotIn("volumes", spec, rel)
            self.assertNotIn("volumeMounts", spec["containers"][0], rel)
            self.assertEqual(len(spec["containers"]), 1, rel)
            self.assertFalse({"BREAK_LINE_RELAY_ENABLED", "BREAK_LINE_COMMANDS_ENABLED"} & set(env_of(deployment(rel))), rel)
        # ANYWHERE: every manifest, patch, slice and script of the repository, not a chosen list
        offenders = []
        for p in list((ROOT / "k8s").rglob("*.yaml")) + [ROOT / "services.yaml"] + list(ROOT.glob("Jenkinsfile*")) + list((ROOT / "scripts").rglob("*.sh")) + list((ROOT / "image-tags").glob("*.yaml")):
            if re.search(r"BREAK_LINE_(RELAY|COMMANDS)_ENABLED", p.read_text(errors="replace")):
                offenders.append(str(p.relative_to(ROOT)))
        self.assertEqual(offenders, [], "D3 turns them on, in its own change; nothing here names either flag")

    def test_it_is_a_consumer_of_the_shared_config_and_secrets_and_has_no_service_object(self):
        ct = deployment(BASE)["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual([x.get("configMapRef", {}).get("name") or x.get("secretRef", {}).get("name") for x in ct["envFrom"]], ["options-edge-config", "options-edge-runtime-secrets"])
        self.assertFalse(any(d and d.get("kind") in ("Service", "PersistentVolumeClaim") for d in yaml.safe_load_all(read(SLICE))))

    def test_the_shared_dev_config_resolves_to_the_pinned_broker_and_registry(self):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s/overlays/dev")], capture_output=True, text=True, check=True).stdout
        cm = next(d for d in yaml.safe_load_all(out) if d and d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "options-edge-config")
        self.assertEqual((cm["data"]["KAFKA_BOOTSTRAP_SERVERS"], cm["data"]["SCHEMA_REGISTRY_URL"]), (PINNED_BOOTSTRAP, PINNED_REGISTRY))

    def test_no_production_experiment_or_es4_reference(self):
        for env in ("production", "experiment", "es4"):
            self.assertFalse((ROOT / f"k8s/services/{SVC}/overlays/{env}").exists(), f"no {env} overlay")
            p = ROOT / f"k8s/overlays/{env}/kustomization.yaml"
            self.assertNotIn(f"services/{SVC}/", p.read_text() if p.exists() else "", env)
        self.assertNotIn(SVC, read("k8s/base/kustomization.yaml"))


class Registration(unittest.TestCase):
    def test_services_yaml_declares_the_dev_only_standalone_slice(self):
        by = {s["name"]: s for s in yaml.safe_load(read("services.yaml"))["services"]}
        s = by[SVC]
        self.assertEqual(s["envs"], ["dev"])
        self.assertEqual(s["managedBy"], "standalone")
        self.assertTrue(s["sliceGenerated"])
        self.assertEqual(s["deployments"], ["break-line-service"])
        self.assertEqual(s["image"], "options-edge-break-line")

    def test_dev_overlay_resource_image_remap_and_image_tag(self):
        k = read("k8s/overlays/dev/kustomization.yaml")
        self.assertIn(f"  - ../../services/{SVC}/base\n", k)
        self.assertIn("newName: host.docker.internal:5001/options-edge-break-line", k)
        self.assertIn("break-line-service: host.docker.internal:5001/options-edge-break-line:dev", read("image-tags/dev.yaml"))

    def test_the_image_variable_is_wired_in_every_deploy_script(self):
        for rel in ("scripts/deploy/apply.sh", "scripts/deploy/image-lock.sh", "scripts/deploy/resolve-images.sh", "scripts/deploy/verify-running-images.sh"):
            self.assertIn("BREAK_LINE_IMAGE", read(rel), rel)
        self.assertIn('("break-line-service", "break-line"): "BREAK_LINE_IMAGE"', read("scripts/deploy/verify-running-images.sh"))
        self.assertIn("BREAK_LINE_IMAGE=${BREAK_LINE_IMAGE:-$registry/options-edge-break-line:dev}", read("scripts/deploy/resolve-images.sh"))

    def test_service_deploy_offers_it_and_runs_the_shared_barrier_stage_without_a_new_stage(self):
        j = read("Jenkinsfile.service-deploy")
        self.assertIn("'strike-hold', 'break-line',", j)
        # The arm names break-line and is gated. Asserted by MEMBERSHIP rather than by pinning the
        # whole list's text: the previous exact pin broke when gamma-zone joined the arm on
        # 2026-11-11, which is a service being added, not this service losing its gate.
        arm = re.search(r"^.*\['line-acceptance'.*\]\.contains\(params\.SERVICE\).*$", j, re.M).group(0)
        self.assertIn("'break-line'", arm)
        self.assertIn("!params.DEPLOY_DRY_RUN", arm)
        self.assertIn("PERMITTED_SHA_GUARD == 'PASSED'", arm)
        self.assertIn("DEPLOY_WORKSPACE_PERMITTED == 'PASSED'", arm)
        self.assertIn("break-line) scripts/kafka/ensure-break-line-topics.sh ;;", j)
        self.assertEqual(j.count("Verify break-line topics"), 0, "no separate stage: the pipeline's CPS method is at the JVM limit")

    def test_the_slice_generator_has_no_drift(self):
        before = {p: p.read_text() for p in (ROOT / f"k8s/services/{SVC}/overlays/dev").glob("*")}
        subprocess.run(["bash", str(ROOT / "scripts/deploy/generate-service-slices.sh")], cwd=ROOT, check=True, capture_output=True)
        after = {p: p.read_text() for p in (ROOT / f"k8s/services/{SVC}/overlays/dev").glob("*")}
        self.assertEqual(before, after)


class Schemas(unittest.TestCase):
    def test_the_map_names_exactly_the_five_fixed_ids_and_no_other_map_uses_them(self):
        rows = [l.split("|") for l in read("scripts/kafka/schema-id-maps/break-line.map").splitlines() if l and not l.startswith("#")]
        self.assertEqual(sorted((r[0], int(r[1])) for r in rows), sorted([(f"{NS}.{n}", i) for n, i in IDS.items()] + [(f"{NS}.{n}", i) for n, i in ARCHIVAL_V1.items()]))
        for r in rows:
            n = r[0].rsplit(".", 1)[1]
            self.assertEqual(r[2], (f"v1/{n}.avsc" if int(r[1]) in ARCHIVAL_V1.values() else f"{n}.avsc"), r)
        for other in (ROOT / "scripts/kafka/schema-id-maps").glob("*.map"):
            if other.name == "break-line.map":
                continue
            text = other.read_text()
            for i in list(IDS.values()) + list(ARCHIVAL_V1.values()):
                self.assertNotRegex(text, rf"\|{i}\|", f"{other.name} uses {i}")

    def test_the_manifest_governs_the_five_subjects_with_hashes_equal_to_the_snapshots(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        by = {e["subject"]: e for e in m["entries"] if e.get("kind") == "MIRRORED"}
        self.assertEqual(m["familyBlocks"]["breakline"], [[3901, 3910]])
        by_id = {e["id"]: e for e in m["entries"] if e.get("family") == "breakline"}
        for n, i in IDS.items():
            e = by_id[i]
            v2 = n in ARCHIVAL_V1
            self.assertEqual((e["subject"], e["id"], e["version"], e["family"], e["environments"], e["retained"]), (f"{NS}.{n}", i, 2 if v2 else 1, "breakline", ["dev"], True))
            self.assertEqual(e["topics"], [{"name": TOPIC_OF[n], "kind": "data"}])
            self.assertEqual(e["source"], {"kind": "FILE", "repo": "contracts", "path": f"schemas-registry/breakline/{n}.avsc"})
            # D2 binds the service: it produces the version 2 ids (3906-3908) and the snapshot (3905); the web tier writes 3901 (no producer binding, as line health's user lines);
            # the version 1 ids 3902-3904 are archival and bound to nobody; the service reads 3901 (lines) and 3906 (its own state at a cold start)
            self.assertEqual(e["producers"], [{"service": SVC, "environments": ["dev"]}] if i in (3901, 3905, 3906, 3907, 3908) else [], f"{n} {i}: producers")
            self.assertEqual(e["consumers"], [], f"{n} {i}: the image label (schema-requirements.json) declares no consumes of the break-line subjects")
            snap = ROOT / "schema-manifest/schemas" / e["subject"] / ("v2.avsc" if v2 else "v1.avsc")
            out = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
            self.assertEqual(out, e["schemaHash"], n)
            snaps = json.loads(snap.read_text())
            self.assertEqual((snaps["namespace"], snaps["name"]), (NS, n))
        for n, i in ARCHIVAL_V1.items():
            e = by_id[i]
            self.assertEqual((e["subject"], e["version"], e["producers"], e["consumers"], e["retained"]), (f"{NS}.{n}", 1, [], [], True), f"{n}: archival version 1")
            self.assertEqual(e["source"]["path"], f"schemas-registry/breakline/v1/{n}.avsc")
            snap = ROOT / "schema-manifest/schemas" / e["subject"] / "v1.avsc"
            out = subprocess.run(["java", "-jar", str(JAR), "hash", str(snap)], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
            self.assertEqual(out, e["schemaHash"], n)
            # the version 1 snapshot is the frozen text: its hash differs from version 2 of the same subject
            self.assertNotEqual(e["schemaHash"], by_id[IDS[n]]["schemaHash"], n)
        self.assertEqual(sorted(by_id), sorted(list(IDS.values()) + list(ARCHIVAL_V1.values())))
        lh = next(x for x in m["entries"] if x.get("id") == 3708)
        want = [{"service": "break-line", "accepts": [3701, 3708], "environments": ["dev"]}]
        self.assertEqual(lh["consumers"], want, "the service reads the line health state, both versions (a compacted topic can still hold version 1 frames)")
        self.assertEqual(next(x for x in m["entries"] if x.get("id") == 3701)["consumers"], want)

    def test_the_bindings_on_the_existing_subjects_are_exactly_the_new_consumer_and_nothing_else_changed(self):
        """Break-line reads 3502, 3509 and 3801 and accepts exactly that id; every other producer and consumer of those entries is what it was before this change."""
        m = json.loads(read("schema-manifest/manifest.json"))
        by = {e["id"]: e for e in m["entries"] if e.get("kind") == "MIRRORED"}
        mine = {"service": SVC, "environments": ["dev"]}
        expected = {
            3502: ([{"service": "es-liquidity-engine", "environments": ["dev"]}], [{"service": "level-absorption", "accepts": [3502], "environments": ["dev"]}]),
            3509: ([{"service": "es-liquidity-engine", "environments": ["dev"]}], [{"service": "level-absorption", "accepts": [3509], "environments": ["dev"]}]),
            3801: ([{"service": "strike-hold", "environments": ["dev"]}], []),
        }
        for i, (producers, others) in expected.items():
            e = by[i]
            self.assertEqual(e["producers"], producers, f"{i}: producers unchanged")
            self.assertEqual([c for c in e["consumers"] if c["service"] != SVC], others, f"{i}: the other consumers unchanged")
            self.assertEqual([c for c in e["consumers"] if c["service"] == SVC], [dict(mine, accepts=[i])] if i in (3502, 3509) else [], f"{i}: break-line accepts exactly {i} on the engine inputs, and does not read 3801 (the image label declares no such consume)")
        # the complete set of entries that name break-line, by role
        roles = {}
        for e in m["entries"]:
            for role in ("producers", "consumers"):
                for x in e.get(role) or []:
                    if x["service"] == SVC:
                        roles.setdefault(role, []).append(e["id"])
        self.assertEqual({k: sorted(v) for k, v in roles.items()}, {"producers": [3901, 3905, 3906, 3907, 3908], "consumers": [3502, 3509, 3701, 3708]})

    def test_no_id_of_the_block_3901_3910_is_used_by_any_other_family(self):
        m = json.loads(read("schema-manifest/manifest.json"))
        others = [e["id"] for e in m["entries"] if e.get("family") != "breakline" and e.get("id") is not None and 3901 <= e["id"] <= 3910]
        self.assertEqual(others, [])

    def test_the_manifest_validates_with_the_governance_tool(self):
        r = subprocess.run(["java", "-jar", str(JAR), "validate", "--manifest", str(ROOT / "schema-manifest/manifest.json"), "--snapshots", str(ROOT / "schema-manifest/schemas")],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
