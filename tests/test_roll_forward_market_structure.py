"""scripts/kafka/roll-forward-market-structure.sh (W0 step 4 and step 6) against a stateful stand-in for the Kafka CLIs and kubectl
(tests/fake_ms_cluster.py): every gate refuses BEFORE the first delete and names itself; the happy path deletes and recreates exactly the five W0
topics, one at a time, at the declared shape with new ids and end offset 0, and never names the input, tools or geometry topic; a part-way failure
is resumable without re-wiping what is already recreated; --verify is read-only and fails on a pre-cutover id or a JSON value."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/roll-forward-market-structure.sh"
FAKE = ROOT / "tests/fake_ms_cluster.py"
P = "options.spx.market-structure."
NAMES = {"ui": P + "ui", "sequence": P + "sequence", "dlq": P + "dlq", "patterns": P + "patterns", "recovery": P + "recovery"}
UNTOUCHED = ["options.indicators.bars", P + "tools", P + "geometry"]
DIGEST = "sha256:" + "ab" * 32
GW_DIGEST = "sha256:" + "ef" * 32
def tid(prefix: str) -> str:
    """A Kafka topic id is 22 base64url characters."""
    return (prefix + "x" * 22)[:22]


PRE = {n: tid(n.capitalize() + "Pre") for n in ("ui", "sequence", "dlq", "patterns", "recovery")}
NOW_MS = 1_760_000_000_000
EVENING = "2 1705 2026-10-20 17:05:00 EDT"   # a Tuesday, after the close
CFG = {
    "ui": {"cleanup.policy": "delete", "retention.ms": "604800000"}, "sequence": {"cleanup.policy": "delete", "retention.ms": "172800000"},
    "dlq": {"cleanup.policy": "delete", "retention.ms": "604800000"}, "patterns": {"cleanup.policy": "delete", "retention.ms": "172800000"},
    "recovery": {"cleanup.policy": "compact", "retention.ms": "-1"},
}


def topic(tid: str, parts: int, configs: dict, end: int = 0) -> dict:
    base = {"max.message.bytes": "1048576", "min.insync.replicas": "1"}
    return {"id": tid, "parts": parts, "rf": 1, "configs": {**configs, **base}, "end": {str(p): end for p in range(parts)}, "first": {}}


def initial_state() -> dict:
    topics = {NAMES[n]: topic(PRE[n], 1, CFG[n], end=100) for n in NAMES}
    topics["options.indicators.bars"] = topic(tid("Bars"), 8, {"cleanup.policy": "delete"}, end=5000)
    topics[P + "tools"] = topic(tid("Tools"), 1, {"cleanup.policy": "delete", "retention.ms": "172800000"}, end=7)
    topics[P + "geometry"] = topic(tid("Geom"), 1, {"cleanup.policy": "delete", "retention.ms": "172800000"}, end=9)
    ready = {"image": "192.168.100.252:5000/options-edge-market-structure:prod", "name": "market-structure"}
    pod = {"metadata": {"name": "market-structure-service-abc", "namespace": "options-edge", "uid": "uid-1"}, "status": {"phase": "Running",
           "containerStatuses": [{"name": "market-structure", "ready": True, "restartCount": 0, "imageID": "192.168.100.252:5000/options-edge-market-structure@" + DIGEST}]},
           "spec": {"containers": [{"name": ready["name"], "image": ready["image"] + "@" + DIGEST}]}}
    gateway = {"metadata": {"name": "feed-gateway-service-xyz", "namespace": "options-edge"}, "status": {"phase": "Running",
               "containerStatuses": [{"name": "feed-gateway", "ready": True, "imageID": "192.168.100.252:5000/options-edge-feed-gateway@" + GW_DIGEST}]},
               "spec": {"containers": [{"name": "feed-gateway", "image": "192.168.100.252:5000/options-edge-feed-gateway:prod@" + GW_DIGEST}]}}
    return {
        "topics": topics, "service_state": "SESSION_ENDED", "running_pods": ["market-structure-service-abc"], "pods": {"items": [pod, gateway]},
        "metrics": {"records_consumed_total": 10, "projections_published_total": 20, "sequence_published_total": 30,
                    "rejections_published_total": 1, "pattern_sets_published_total": 40},
        "producers": {"own": 4001, "by_topic": {t: [[4001, NOW_MS - 60_000]] for t in NAMES.values()}}, "groups": [],
    }


class Cluster:
    def __init__(self, state: dict | None = None):
        self.tmp = Path(tempfile.mkdtemp())
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for n in ["kafka-topics.sh", "kafka-configs.sh", "kafka-get-offsets.sh", "kafka-consumer-groups.sh", "kafka-console-consumer.sh",
                  "kafka-transactions.sh", "kubectl"]:
            shutil.copy(FAKE, self.bin / n)
            (self.bin / n).chmod(0o755)
        self.state_path = self.tmp / "state.json"
        self.calls = self.tmp / "calls"
        self.calls.touch()
        self.state_path.write_text(json.dumps(state or initial_state()))

    @property
    def state(self) -> dict:
        return json.loads(self.state_path.read_text())

    def patch(self, fn):
        s = self.state
        fn(s)
        self.state_path.write_text(json.dumps(s))

    def run(self, phase="--recreate", cluster="dev", bs="localhost:19092", pre=None, digest=DIGEST, gateway_digest=GW_DIGEST, attested=True, extra=(), now=EVENING):
        pre = pre or ",".join(f"{n}={PRE[n]}" for n in NAMES)
        args = ["bash", str(SCRIPT), phase, "--cluster", cluster, "--pre-cutover-ids", pre]
        if phase == "--recreate":
            if digest:
                args += ["--expect-image-digest", digest]
            if gateway_digest:
                args += ["--expect-gateway-digest", gateway_digest]
            if attested:
                args += ["--producers-attested"]
        args += list(extra) + [bs, str(self.bin)]
        env = dict(os.environ, MS_CLUSTER_STATE=str(self.state_path), MS_CLUSTER_CALLS=str(self.calls), KUBECTL=str(self.bin / "kubectl"),
                   MS_W0_NOW=now, MS_W0_NOW_MS=str(NOW_MS), MS_W0_QUIESCE_SECONDS="0", MS_W0_SETTLE_SLEEP="0", KAFKA_BIN_DIR="")
        r = subprocess.run(args, capture_output=True, text=True, env=env)
        return r.returncode, r.stdout + r.stderr

    def deletes(self) -> list[str]:
        return [c.split("--topic ")[1].split()[0] for c in self.calls.read_text().splitlines() if c.startswith("kafka-topics") and " --delete " in c + " "]


class HappyPath(unittest.TestCase):
    def test_the_five_topics_are_recreated_empty_at_the_declared_shape_and_nothing_else_is_touched(self):
        c = Cluster()
        rc, out = c.run()
        self.assertEqual(rc, 0, out)
        self.assertEqual(sorted(c.deletes()), sorted(NAMES.values()))
        s = c.state["topics"]
        for n, t in NAMES.items():
            self.assertNotEqual(s[t]["id"], PRE[n], t)
            self.assertEqual(s[t]["parts"], 1)
            self.assertEqual(s[t]["end"], {"0": 0})
            for k, v in {**CFG[n], "max.message.bytes": "1048576"}.items():
                self.assertEqual(s[t]["configs"][k], v, f"{t} {k}")
        self.assertEqual(s[P + "recovery"]["configs"]["segment.bytes"], "16777216")
        self.assertEqual(s[P + "recovery"]["configs"]["max.compaction.lag.ms"], "300000")
        for t in UNTOUCHED:
            self.assertEqual(s[t]["id"], initial_state()["topics"][t]["id"], t)
        self.assertNotIn("options.indicators.bars", c.deletes())
        for g in ("G2 ok", "G3 ok", "G4 ok", "G5 ok", "G6 ok", "G7 ok", "G8 ok", "post-recreate: all five topics at end offset 0"):
            self.assertIn(g, out)

    def test_weekend_is_allowed_and_the_ensure_script_ran_after_the_recreate(self):
        c = Cluster()
        rc, out = c.run(now="6 1100 2026-10-17 11:00:00 EDT")
        self.assertEqual(rc, 0, out)
        calls = c.calls.read_text().splitlines()
        last_delete = max(i for i, x in enumerate(calls) if " --delete " in x + " ")
        self.assertTrue(any(x.startswith("kafka-configs") and "--describe" in x for x in calls[last_delete:]), "ensure-market-structure-topics.sh ran after the wipe")
        self.assertIn("ok: 'options.spx.market-structure.recovery'", out)


class EveryGateRefusesBeforeTheFirstDelete(unittest.TestCase):
    def refused(self, mutate=None, expect="", **kw):
        c = Cluster()
        if mutate:
            c.patch(mutate)
        rc, out = c.run(**kw)
        self.assertNotEqual(rc, 0, out)
        self.assertEqual(c.deletes(), [], out)
        self.assertIn(expect, out)
        for t, d in initial_state()["topics"].items():
            self.assertEqual(c.state["topics"][t]["id"], d["id"], f"{t} untouched")
        return out

    def test_g1_the_bootstrap_must_be_the_named_clusters_broker(self):
        self.refused(expect="is not the production cluster's broker", cluster="production", bs="localhost:19092")
        self.refused(expect="is not the dev cluster's broker", cluster="dev", bs="192.168.100.252:9092")

    def test_g2_inside_the_regular_session_or_before_the_close_on_a_trading_day(self):
        for now in ("2 1030 2026-10-20 10:30:00 EDT", "1 0930 2026-10-19 09:30:00 EDT", "5 1559 2026-10-16 15:59:59 EDT", "3 0715 2026-10-21 07:15:00 EDT"):
            self.refused(expect="G2", now=now)

    def test_g4_g9_without_a_cluster_scope_pod_list_the_namespaces_are_read_one_by_one(self):
        """Production: the deployer SA has no cluster-scope pod list. The same gates are judged from per-namespace reads; system namespaces may be unreadable, a tenant namespace may not."""
        c = Cluster()
        c.patch(lambda s: s.update(cluster_pods_forbidden=True, namespaces=["options-edge", "bleedingoptions", "kube-system", "loki", "default"], unreadable_namespaces=["kube-system", "loki", "default"]))
        rc, out = c.run(extra=["--gates-only"])
        self.assertEqual(rc, 0, out)
        self.assertIn("G4 ok", out)
        self.assertIn("G9 ok", out)
        self.refused(lambda s: s.update(cluster_pods_forbidden=True, namespaces=["options-edge", "bleedingoptions"], unreadable_namespaces=["bleedingoptions"]), "bleedingoptions")
        # a tolerated (system) namespace failing for any reason other than kubectl's own Forbidden (outage, auth, timeout) fails closed
        self.refused(lambda s: s.update(cluster_pods_forbidden=True, namespaces=["options-edge", "kube-system"], unreadable_namespaces=["kube-system"],
                                        unreadable_message="Unable to connect to the server: net/http: TLS handshake timeout"), "non-authorization error")
        self.refused(lambda s: (s.update(cluster_pods_forbidden=True, namespaces=["options-edge"]), s["pods"]["items"][1]["status"]["containerStatuses"][0].update(imageID="x@sha256:" + "cd" * 32)), "G9")
        # a JSON-era market-structure pod in ANOTHER namespace is still seen
        def stray(s):
            s.update(cluster_pods_forbidden=True, namespaces=["options-edge", "other"])
            pod = json.loads(json.dumps(s["pods"]["items"][0]))
            pod["metadata"].update(namespace="other", name="ms-stray")
            pod["status"]["containerStatuses"][0]["imageID"] = "x@sha256:" + "ee" * 32
            s["pods"]["items"].append(pod)
        self.refused(stray, "G4")

    def test_g9_the_gateway_must_be_the_expected_avro_gateway_and_running(self):
        rc, out = Cluster().run(gateway_digest="")
        self.assertNotEqual(rc, 0)
        self.assertIn("G9", out)
        self.refused(lambda s: s["pods"]["items"][1]["status"]["containerStatuses"][0].update(imageID="x@sha256:" + "cd" * 32), "G9")   # a JSON-era gateway
        self.refused(lambda s: s["pods"]["items"][1]["status"].update(phase="Pending"), "G9")                                              # no gateway serving
        self.refused(lambda s: s["pods"]["items"].pop(1), "G9")                                                                           # no gateway at all
        self.refused(lambda s: s["pods"]["items"][1]["status"]["containerStatuses"][0].update(ready=False), "G9")                        # Running but not ready

    def test_gates_only_evaluates_every_gate_and_deletes_nothing(self):
        c = Cluster()
        rc, out = c.run(extra=["--gates-only"])
        self.assertEqual(rc, 0, out)
        for g in ("G2 ok", "G3 ok", "G4 ok", "G9 ok", "G5 ok", "G6 ok", "G7 ok", "G8 ok", "--gates-only"):
            self.assertIn(g, out)
        self.assertEqual(c.deletes(), [])
        for t, d in initial_state()["topics"].items():
            self.assertEqual(c.state["topics"][t]["id"], d["id"], f"{t} untouched")
        self.refused(lambda s: s.update(service_state="LIVE"), "G3", extra=["--gates-only"])   # a rehearsal refuses like the real run

    def test_a_pod_with_two_matching_containers_counts_once(self):
        def sidecar(s):
            pod = s["pods"]["items"][0]
            pod["spec"]["containers"].append(dict(pod["spec"]["containers"][0], name="sidecar"))
            pod["status"]["containerStatuses"].append(dict(pod["status"]["containerStatuses"][0], name="sidecar"))
        c = Cluster()
        c.patch(sidecar)
        rc, out = c.run(extra=["--gates-only"])
        self.assertEqual(rc, 0, out)

    def test_gates_only_is_refused_for_verify(self):
        rc, out = Cluster().run(phase="--verify", extra=["--gates-only"])
        self.assertEqual(rc, 2, out)
        self.assertIn("--gates-only", out)

    def test_g3_the_service_must_report_session_ended(self):
        self.refused(lambda s: s.update(service_state="LIVE"), "G3")

    def test_g3_without_the_pod_proxy_a_ready_container_after_the_close_proves_session_ended(self):
        """Production: the deployer SA cannot get pods/proxy. The gate (and the quiescence read) must work from `kubectl get pod`: Ready passes, not Ready refuses,
        and the end-offset proof still refuses on movement."""
        c = Cluster()
        c.patch(lambda s: s.update(proxy_forbidden=True))
        rc, out = c.run(extra=["--gates-only"])
        self.assertEqual(rc, 0, out)
        self.assertIn("pod proxy not permitted", out)
        self.assertIn("uid=uid-1 restarts=0 ready=true", out)
        self.assertIn("G7 ok", out)
        self.assertEqual(c.deletes(), [])
        self.refused(lambda s: (s.update(proxy_forbidden=True), s["pods"]["items"][0]["status"]["containerStatuses"][0].update(ready=False)), "G3")
        self.refused(lambda s: s.update(proxy_forbidden=True), "16:05", now="3 1602 2026-10-21 16:02:00 EDT")   # inside the 5 minute margin after the close
        self.refused(lambda s: (s.update(proxy_forbidden=True), s["pods"]["items"][0]["metadata"].update(uid="")), "single market-structure")   # an identity without a UID proves nothing
        self.refused(lambda s: s.update(service_state="forbidden pods/proxy"), "G3")                          # a workload body that merely says "forbidden" never selects the fallback
        self.refused(lambda s: (s.update(proxy_forbidden=True), s.update(bars_moves_per_read=1)), "G7")   # the end-offset proof still refuses on movement

    def test_g4_a_second_market_structure_pod_or_a_foreign_digest_refuses(self):
        def other_digest(s):
            s["pods"]["items"][0]["status"]["containerStatuses"][0]["imageID"] = "x@sha256:" + "cd" * 32
        self.refused(other_digest, "G4")

        def lingering(s):
            extra = json.loads(json.dumps(s["pods"]["items"][0]))
            extra["metadata"].update(name="old-json-image", namespace="other")
            extra["status"]["phase"] = "Running"
            extra["status"]["containerStatuses"][0]["imageID"] = "x@sha256:" + "00" * 32     # the OLD JSON image, in another namespace
            s["pods"]["items"].append(extra)
        self.refused(lingering, "G4")

        def two_running(s):
            extra = json.loads(json.dumps(s["pods"]["items"][0]))
            extra["metadata"]["name"] = "second"
            s["pods"]["items"].append(extra)
        self.refused(two_running, "G4")
        self.refused(lambda s: s["pods"].update(items=[]), "G4")

    def test_g5_the_maintenance_stop_must_be_attested(self):
        c = Cluster()
        rc, out = c.run(attested=False)
        self.assertEqual(rc, 2, out)
        self.assertIn("--producers-attested", out)
        self.assertEqual(c.deletes(), [])
        rc, out = Cluster().run(digest="")
        self.assertEqual(rc, 2, out)

    def test_g6_a_foreign_producer_with_a_recent_write_refuses_but_a_stale_one_does_not(self):
        def foreign(s):
            s["producers"]["by_topic"][P + "dlq"].append([9999, NOW_MS - 30_000])
        out = self.refused(foreign, "G6")
        self.assertIn("9999", out)
        c = Cluster()
        c.patch(lambda s: s["producers"]["by_topic"][P + "dlq"].append([9999, NOW_MS - 86_400_000]))
        rc, out = c.run()
        self.assertEqual(rc, 0, out)

    def test_g7_movement_during_the_observation_refuses(self):
        self.refused(lambda s: s.update(metrics_move=True), "G7")
        self.refused(lambda s: s.update(bars_moves_per_read=True), "G7")

    def test_g8_a_pre_cutover_id_mismatch_on_any_topic_leaves_all_five_untouched(self):
        pre = ",".join(f"{n}={PRE[n] if n != 'patterns' else tid('Wrong')}" for n in NAMES)
        self.refused(expect="G8", pre=pre)

    def test_malformed_pre_cutover_ids_are_refused_as_usage(self):
        rc, out = Cluster().run(pre="ui=short,sequence=x")
        self.assertEqual(rc, 2, out)


class FailureAndResume(unittest.TestCase):
    def test_a_writer_alive_after_the_create_fails_closed(self):
        c = Cluster()
        c.patch(lambda s: s.update(writer_after_create=NAMES["sequence"]))
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("is not empty right after the recreate", out)

    def test_a_failed_create_is_resumed_without_wiping_what_is_already_recreated(self):
        c = Cluster()
        c.patch(lambda s: s.update(create_fails=NAMES["dlq"]))
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        s = c.state["topics"]
        self.assertNotIn(NAMES["dlq"], s)
        new_ids = {n: s[NAMES[n]]["id"] for n in ("ui", "sequence")}
        self.assertTrue(all(new_ids[n] != PRE[n] for n in new_ids))
        # a plain re-run is refused: ui and sequence no longer carry their pre-cutover ids and dlq is absent
        c.patch(lambda st: st.pop("create_fails"))
        before = len(c.deletes())
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertEqual(len(c.deletes()), before)
        # resume: say which topics were already recreated and allow the absent one
        done = ",".join(f"{n}={i}" for n, i in new_ids.items())
        rc, out = c.run(extra=["--already-recreated", done, "--topic-absent-after-failed-create"])
        self.assertEqual(rc, 0, out)
        s2 = c.state["topics"]
        for n, i in new_ids.items():
            self.assertEqual(s2[NAMES[n]]["id"], i, f"{n} was not re-wiped")
        resumed = c.deletes()[before:]
        self.assertEqual(sorted(resumed), sorted([NAMES["patterns"], NAMES["recovery"]]))
        self.assertNotIn(NAMES["dlq"], resumed)

    def test_stale_group_offsets_beyond_the_new_log_end_are_reported(self):
        c = Cluster()
        c.patch(lambda s: s.update(groups=[f"gateway {NAMES['ui']} 0 57 0 0"]))
        rc, out = c.run()
        self.assertEqual(rc, 0, out)
        self.assertIn("committed offset BEYOND the new log end", out)
        self.assertIn(f"gateway:{NAMES['ui']}@57", out)


class Verify(unittest.TestCase):
    def recreated(self) -> Cluster:
        c = Cluster()
        rc, out = c.run()
        assert rc == 0, out
        return c

    def test_verify_passes_on_avro_and_changes_nothing(self):
        c = self.recreated()
        c.patch(lambda s: [s["topics"][t].update(end={"0": 12}) for t in NAMES.values()])
        n_before = len(c.deletes())
        rc, out = c.run("--verify")
        self.assertEqual(rc, 0, out)
        self.assertIn("a SAMPLE", out)
        self.assertEqual(len(c.deletes()), n_before)

    def test_verify_fails_on_a_json_value(self):
        c = self.recreated()
        c.patch(lambda s: (s["topics"][NAMES["patterns"]].update(end={"0": 12}), s.update(wire={NAMES["patterns"]: "json"})))
        rc, out = c.run("--verify")
        self.assertEqual(rc, 1, out)
        self.assertIn("NON-AVRO", out)

    def test_verify_fails_on_a_topic_that_still_has_its_pre_cutover_id(self):
        c = Cluster()
        rc, out = c.run("--verify")
        self.assertEqual(rc, 1, out)
        self.assertIn("PRE-CUTOVER id", out)


class DeclarationsAgreeWithTopicsEnv(unittest.TestCase):
    """The shapes this script creates are the ones scripts/kafka/topics.env declares (one source of truth would be better; two that agree is the floor)."""

    def test_retention_and_policy_equal_topics_env(self):
        script = f'source "{ROOT}/scripts/kafka/topics.env"; echo "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"; echo "$OPTIONS_EDGE_PURE_COMPACT_TOPICS"'
        out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout.splitlines()
        retention = dict(x.split("=", 1) for x in out[0].split() if "=" in x)
        for n, t in NAMES.items():
            self.assertEqual(retention[t], CFG[n]["retention.ms"], t)
        self.assertIn(NAMES["recovery"], out[1].split())
        for n in ("ui", "sequence", "dlq", "patterns"):
            self.assertNotIn(NAMES[n], out[1].split())

    def test_the_script_never_names_the_input_the_tools_or_the_geometry_topic_in_a_destructive_call(self):
        text = SCRIPT.read_text()
        self.assertNotIn("market-structure.geometry", text)
        self.assertNotIn("market-structure.tools", text)
        self.assertEqual(len([ln for ln in text.splitlines() if 'kt --delete' in ln]), 1, "exactly one delete call, on the loop variable")


if __name__ == "__main__":
    unittest.main()
