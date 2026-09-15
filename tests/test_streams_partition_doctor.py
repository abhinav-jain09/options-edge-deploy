"""streams-partition-doctor.sh against a simulated cluster.

The fake kubectl/kafka-topics keep state in a JSON file. Each deployment has a Streams application id
and a list of internal topics Streams rejects ("bad" = topic, expected, actual). On every scale-up a new
pod starts and logs the FIRST bad topic that still exists with a count != expected (Streams reports one
per start) and stays not ready; once none remain, Streams "recreates" them at the expected count and the
app becomes ready. kafka-topics --topic is a regular expression, as in the real tool. The doctor must read
every number it uses from the logs and the broker.
"""
import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCTOR = ROOT / "scripts" / "kafka" / "streams-partition-doctor.sh"
BASH = "/bin/bash" if Path("/bin/bash").exists() else "bash"
UUID = "0f6cb959-9777-4abf-ac8b-f4d46d4d45d5"

FAKE = textwrap.dedent(
    r'''
    import json, os, re, sys
    state = sys.argv[1]; args = sys.argv[2:]
    S = os.path.join(state, "state.json")
    st = json.load(open(S))
    def save(): json.dump(st, open(S, "w"))
    def rejection(dep):
        for t, e, a in dep["bad"]:
            if t in st["topics"] and st["topics"][t] != e:
                return f"org.apache.kafka.streams.errors.StreamsException: Existing internal topic {t} has invalid partitions: expected: {e}; actual: {st['topics'][t]}. Use 'org.apache.kafka.tools.StreamsResetter' tool to clean up invalid topics before processing."
        return None
    def start(d):
        dep = st["deploys"][d]
        st["seq"] += 1
        pod = {"name": f"{d}-pod-{st['seq']}", "phase": "Running", "prev": [], "pending": [],
               "log": [(f"[{dep['app']}-{st['uuid']}-StreamThread-1] INFO stream-thread [{dep['app']}-{st['uuid']}-StreamThread-1] State transition from CREATED to STARTING"
                        if dep.get("client_id_set") else
                        f"[main] INFO stream-client [{dep['app']}-{st['uuid']}] State transition from CREATED to REBALANCING")]}
        r = rejection(dep)
        if r and dep.get("late_reject"):
            pod["pending"].append(r); dep["ready"] = dep["replicas"]
        elif r:
            pod["log"].append(r); dep["ready"] = dep["replicas"] if dep.get("ready_while_broken") else 0
        else:
            for t, e, a in dep["bad"]:
                st["topics"].setdefault(t, e)
            pod["log"].append(f"stream-client [{dep['app']}-{st['uuid']}] State transition from REBALANCING to RUNNING")
            dep["ready"] = dep["replicas"]
        dep["pods"] = [p for p in dep["pods"] if p["phase"] in ("Failed", "Succeeded")] + [pod]
    if args[0] == "topics":
        args = args[1:]
        st["calls"].append(["kafka-topics"] + args)
        if ("--list" in args and st.get("list_fails")) or ("--describe" in args and st.get("describe_fails")):
            save(); sys.exit(1)
        if "--list" in args:
            print("\n".join(sorted(st["topics"])))
        else:
            rx = re.compile(args[args.index("--topic") + 1])
            hits = sorted(t for t in st["topics"] if rx.fullmatch(t))
            if "--delete" in args and not st.get("delete_noop"):
                for t in hits: st["topics"].pop(t)
            elif "--describe" in args:
                for t in hits: print(f"Topic: {t}\tTopicId: x\tPartitionCount: {st['topics'][t]}\tReplicationFactor: 1\tConfigs: ")
        save(); sys.exit(0)
    st["calls"].append(["kubectl"] + args)
    if args[:2] == ["get", "deploy"] and args[2] == "-o":
        print("\n".join(n for n, v in st["deploys"].items() if v["replicas"] > 0))
    elif args[:2] == ["get", "deploy"]:
        d, fmt = args[2], args[4]
        dep = st["deploys"][d]
        if "go-template" in fmt: print(f"app={d},", end="")
        elif "spec.replicas" in fmt: print(dep["replicas"], end="")
        elif "readyReplicas" in fmt:
            print(dep["ready"] or "", end="")
            for p in dep["pods"]:
                p["log"] += p["pending"]; p["pending"] = []
    elif args[:2] == ["get", "pods"]:
        d = args[3].split("=", 1)[1]
        dep = st["deploys"][d]
        if dep["replicas"] == 0 and dep.get("pods_api_errors", 0) > 0:
            dep["pods_api_errors"] -= 1
            st["pods_listed"].append([d, "API-ERROR"]); save(); sys.exit(1)
        if dep["replicas"] == 0 and dep.get("terminating", 0) > 0:
            dep["terminating"] -= 1
        elif dep["replicas"] == 0 and not dep.get("never_terminates"):
            dep["pods"] = [p for p in dep["pods"] if p["phase"] in ("Failed", "Succeeded")]
        st["pods_listed"].append([d, [p["name"] for p in dep["pods"] if p["phase"] not in ("Failed", "Succeeded")]])
        for p in dep["pods"]: print(p["name"], p["phase"])
    elif args[0] == "logs":
        for dep in st["deploys"].values():
            for p in dep["pods"]:
                if p["name"] == args[1]:
                    if p["name"] in st.get("unreadable", []) or p["phase"] in ("Failed", "Succeeded"): sys.exit(1)
                    if "--previous" in args:
                        if not p["prev"]: sys.exit(1)
                        print("\n".join(p["prev"]))
                    else:
                        print("\n".join(p["log"]))
    elif args[0] == "scale":
        d = args[1].split("/", 1)[1]; n = int(args[2].split("=", 1)[1])
        dep = st["deploys"][d]
        if n == 0 and st.get("slow_scale_down"):
            import time; time.sleep(st["slow_scale_down"])
        was = dep["replicas"]; dep["replicas"] = n
        if n == 0: dep["ready"] = 0
        elif was == 0 or not [p for p in dep["pods"] if p["phase"] == "Running"]: start(d)
    save()
    '''
)


class StreamsPartitionDoctorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        (self.dir / "fake.py").write_text(FAKE)

    def tearDown(self):
        self.tmp.cleanup()

    def cluster(self, deploys, topics, **extra):
        st = {"deploys": {}, "topics": topics, "calls": [], "seq": 0, "uuid": UUID, "pods_listed": [], **extra}
        for name, spec in deploys.items():
            st["deploys"][name] = {"replicas": 0, "ready": 0, "pods": [], "app": spec.get("app", f"{name}-dev"),
                                   **{k: v for k, v in spec.items() if k not in ("app", "replicas")}}
            st["deploys"][name].setdefault("bad", [])
        self.save(st)
        for name, spec in deploys.items():
            self.fake("scale", f"deploy/{name}", f"--replicas={spec.get('replicas', 1)}")
        st = self.state()
        st["calls"] = []
        st["pods_listed"] = []
        self.save(st)

    def save(self, st):
        (self.dir / "state.json").write_text(json.dumps(st))

    def fake(self, *args):
        subprocess.run(["python3", str(self.dir / "fake.py"), str(self.dir), *args], check=True, capture_output=True)

    def state(self):
        return json.loads((self.dir / "state.json").read_text())

    def run_doctor(self, *args, **env):
        fake = f"python3 {self.dir / 'fake.py'} {self.dir}"
        e = dict(os.environ, KUBECTL=fake, KAFKA_TOPICS=f"{fake} topics", DOCTOR_POLL_SECONDS="0",
                 DOCTOR_READY_GRACE_SECONDS="0", DOCTOR_READY_TIMEOUT="3", **env)
        return subprocess.run([BASH, str(DOCTOR), *args], env=e, text=True, capture_output=True, timeout=120)

    def calls(self, kind):
        return [c for c in self.state()["calls"] if c[0] == kind]

    def deletes(self):
        return [c[c.index("--topic") + 1] for c in self.calls("kafka-topics") if "--delete" in c]

    def scales(self, d):
        return [c[3] for c in self.calls("kubectl") if c[:3] == ["kubectl", "scale", f"deploy/{d}"]]

    def test_healthy_apps_are_left_alone(self):
        self.cluster({"gex": {}, "align": {}}, {"src": 32})
        r = self.run_doctor("--repair")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("OK gex", r.stdout)
        self.assertEqual(self.scales("gex") + self.scales("align"), [])
        self.assertEqual(self.deletes(), [])

    def test_repairs_every_rejected_internal_topic_one_per_start(self):
        bad = [["align-dev-es-gex-align-rekey-repartition", 4, 1], ["align-dev-books-changelog", 4, 1]]
        self.cluster({"align": {"bad": bad}, "other": {}},
                     {"align-dev-es-gex-align-rekey-repartition": 1, "align-dev-books-changelog": 1, "user-topic": 4})
        r = self.run_doctor("--repair")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("REPAIRED align", r.stdout)
        st = self.state()
        self.assertEqual(st["deploys"]["align"]["ready"], 1)
        self.assertEqual(st["topics"]["align-dev-es-gex-align-rekey-repartition"], 4)
        self.assertEqual(st["topics"]["align-dev-books-changelog"], 4)
        self.assertEqual(sorted(self.deletes()), sorted(t for t, _, _ in bad))          # each exactly once
        self.assertEqual(st["topics"]["user-topic"], 4)
        self.assertEqual(self.scales("other"), [])

    def test_scales_to_zero_before_deleting_and_restores_replicas(self):
        self.cluster({"svc": {"replicas": 2, "bad": [["svc-dev-x-repartition", 32, 1]]}}, {"svc-dev-x-repartition": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        calls = self.state()["calls"]
        i_zero = next(i for i, c in enumerate(calls) if c[:4] == ["kubectl", "scale", "deploy/svc", "--replicas=0"])
        i_del = next(i for i, c in enumerate(calls) if "--delete" in c)
        self.assertLess(i_zero, i_del)
        self.assertEqual(self.scales("svc"), ["--replicas=0", "--replicas=2"])

    def test_no_delete_while_a_pod_is_still_terminating(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]], "terminating": 3}}, {"svc-dev-x-changelog": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        st = self.state()
        i_del = next(i for i, c in enumerate(st["calls"]) if "--delete" in c)
        n_listings = len([c for c in st["calls"][:i_del] if c[:3] == ["kubectl", "get", "pods"]])
        before = st["pods_listed"][:n_listings]
        self.assertTrue([p for p in before if p[1]], "the fake must have shown live pods after the scale-down")
        self.assertEqual(before[-1][1], [], "the listing right before the delete must show no live pod")

    def test_pods_that_never_go_away_abort_without_delete_and_restore_replicas(self):
        self.cluster({"svc": {"replicas": 3, "bad": [["svc-dev-x-changelog", 4, 1]], "never_terminates": True}},
                     {"svc-dev-x-changelog": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 4, r.stdout)
        self.assertIn("FAILED svc: pods still present", r.stdout)
        self.assertEqual(self.deletes(), [])
        self.assertEqual(self.state()["deploys"]["svc"]["replicas"], 3)

    def test_failed_evicted_pods_neither_block_nor_count_as_unreadable(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]]}}, {"svc-dev-x-changelog": 1})
        st = self.state()
        st["deploys"]["svc"]["pods"].insert(0, {"name": "svc-evicted", "phase": "Failed", "log": [], "prev": [], "pending": []})
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("REPAIRED svc", r.stdout)

    def test_stale_rejection_for_an_already_correct_topic_deletes_nothing(self):
        # The previous container logged the rejection; the topic was fixed since; the app runs now.
        self.cluster({"svc": {}}, {"svc-dev-store-changelog": 4})
        st = self.state()
        st["deploys"]["svc"]["pods"][-1]["prev"] = [
            f"stream-client [svc-dev-{UUID}] State transition from CREATED to REBALANCING",
            "StreamsException: Existing internal topic svc-dev-store-changelog has invalid partitions: expected: 4; actual: 1."]
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("OK svc", r.stdout)
        self.assertEqual(self.deletes(), [])
        self.assertEqual(self.scales("svc"), [])

    def test_rejection_only_in_the_previous_crashed_container_is_repaired(self):
        self.cluster({"svc": {}}, {"svc-dev-x-repartition": 1})
        st = self.state()
        pod = st["deploys"]["svc"]["pods"][-1]
        pod["prev"] = [f"stream-client [svc-dev-{UUID}] State transition from CREATED to REBALANCING",
                       "Existing internal topic svc-dev-x-repartition has invalid partitions: expected: 8; actual: 1"]
        pod["log"] = ["starting"]
        st["deploys"]["svc"]["bad"] = [["svc-dev-x-repartition", 8, 1]]
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertEqual(self.deletes(), ["svc-dev-x-repartition"])

    def test_report_only_without_repair_flag(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]]}}, {"svc-dev-x-changelog": 1})
        r = self.run_doctor()
        self.assertEqual(r.returncode, 4)
        self.assertIn("MISMATCH svc: svc-dev-x-changelog has 1 partition(s), Streams expects 4", r.stdout)
        self.assertEqual(self.deletes(), [])

    def test_never_touches_a_non_internal_or_another_apps_topic(self):
        self.cluster({"svc": {"bad": [["options.opra.tcbbo", 32, 1]]},
                      "two": {"bad": [["other-app-x-changelog", 4, 1]]}},
                     {"options.opra.tcbbo": 1, "other-app-x-changelog": 1})
        r = self.run_doctor("--repair", "svc", "two")
        self.assertEqual(r.returncode, 3, r.stdout)
        self.assertIn("REFUSED svc", r.stdout)
        self.assertIn("REFUSED two", r.stdout)
        self.assertEqual(self.deletes(), [])
        self.assertEqual(self.scales("svc") + self.scales("two"), [])          # refused BEFORE any scale

    def test_topic_names_are_regex_escaped(self):
        self.cluster({"svc": {"app": "a.b", "bad": [["a.b-x-changelog", 4, 1]]}}, {"a.b-x-changelog": 1, "a-b-x-changelog": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("a-b-x-changelog", self.state()["topics"])

    def test_copartition_error_is_reported_not_repaired(self):
        self.cluster({"svc": {}}, {"a": 4, "b": 32})
        st = self.state()
        st["deploys"]["svc"]["pods"][-1]["log"].append(
            "org.apache.kafka.streams.errors.TopologyException: Invalid topology: Following topics do not have the same number of partitions: [{a=4}, {b=32}]")
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 3, r.stdout)
        self.assertIn("UNREPAIRABLE svc", r.stdout)
        self.assertEqual(self.deletes(), [])

    def test_ready_then_rejecting_is_not_called_healed(self):
        # READY is reported before Streams logs the rejection (REPLACE_THREAD apps); the grace re-check
        # must catch it, and with a delete that cannot take effect the result is FAILED, never REPAIRED.
        self.cluster({"svc": {"bad": [["svc-dev-x-repartition", 4, 1]], "late_reject": True}}, {"svc-dev-x-repartition": 1},
                     delete_noop=True)
        st = self.state()
        pod = st["deploys"]["svc"]["pods"][-1]
        pod["log"] += pod["pending"]
        pod["pending"] = []
        self.save(st)
        r = self.run_doctor("--repair", "svc", DOCTOR_MAX_ROUNDS="2")
        self.assertEqual(r.returncode, 4, r.stdout)
        self.assertIn("FAILED svc", r.stdout)
        self.assertNotIn("REPAIRED svc", r.stdout)

    def test_most_serious_outcome_wins_the_exit_code(self):
        # an app needing a declaration fix (3) must not be masked by a later failed repair (4)
        self.cluster({"a1": {"bad": [["options.x", 4, 1]]}, "b2": {"bad": [["b2-dev-x-changelog", 4, 1]], "never_terminates": True}},
                     {"options.x": 1, "b2-dev-x-changelog": 1})
        r = self.run_doctor("--repair", "a1", "b2")
        self.assertIn("REFUSED a1", r.stdout)
        self.assertIn("FAILED b2", r.stdout)
        self.assertEqual(r.returncode, 3, r.stdout)


    def test_broker_describe_failure_is_unknown_not_repaired_not_ok(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]]}}, {"svc-dev-x-changelog": 1})
        st = self.state(); st["describe_fails"] = True; self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 5, r.stdout)
        self.assertIn("INCONCLUSIVE svc", r.stdout)
        self.assertEqual(self.deletes(), [])
        self.assertEqual(self.scales("svc"), [])

    def test_pod_api_error_during_the_wait_is_not_taken_as_pods_gone(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]], "pods_api_errors": 2, "terminating": 2}},
                     {"svc-dev-x-changelog": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        st = self.state()
        i_del = next(i for i, c in enumerate(st["calls"]) if "--delete" in c)
        n = len([c for c in st["calls"][:i_del] if c[:3] == ["kubectl", "get", "pods"]])
        self.assertIn([ "svc", "API-ERROR"], st["pods_listed"][:n])
        self.assertEqual(st["pods_listed"][n - 1][1], [])

    def test_interrupted_through_a_pipe_still_restores_replicas(self):
        # dev-cleanup pipes the doctor through sed, oe-boot-bringup through tee: a SIGTERM kills the
        # reader too, and the restore must not die writing its log line first.
        self.cluster({"svc": {"replicas": 2, "bad": [["svc-dev-x-changelog", 4, 1]], "never_terminates": True}},
                     {"svc-dev-x-changelog": 1})
        fake = f"python3 {self.dir / 'fake.py'} {self.dir}"
        env = dict(os.environ, KUBECTL=fake, KAFKA_TOPICS=f"{fake} topics", DOCTOR_POLL_SECONDS="1",
                   DOCTOR_READY_GRACE_SECONDS="0", DOCTOR_READY_TIMEOUT="3")
        proc = subprocess.Popen([BASH, "-c", f'"{BASH}" "{DOCTOR}" --repair svc | cat'], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
        import time, signal
        for _ in range(300):
            if self.state()["deploys"]["svc"]["replicas"] == 0:
                break
            time.sleep(0.1)
        self.assertEqual(self.state()["deploys"]["svc"]["replicas"], 0, "doctor never scaled down")
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=60)
        for _ in range(100):
            if self.state()["deploys"]["svc"]["replicas"] == 2:
                break
            time.sleep(0.1)
        self.assertEqual(self.state()["deploys"]["svc"]["replicas"], 2)

    def test_aborted_app_is_restored_before_the_next_app_is_examined(self):
        self.cluster({"a1": {"replicas": 2, "bad": [["a1-dev-x-changelog", 4, 1]], "never_terminates": True}, "b2": {}},
                     {"a1-dev-x-changelog": 1})
        r = self.run_doctor("--repair", "a1", "b2")
        self.assertEqual(r.returncode, 4, r.stdout)
        calls = self.state()["calls"]
        i_restore = next(i for i, c in enumerate(calls) if c[:4] == ["kubectl", "scale", "deploy/a1", "--replicas=2"])
        i_b2 = next(i for i, c in enumerate(calls) if c[:2] == ["kubectl", "get"] and "b2" in c)
        self.assertLess(i_restore, i_b2)

    def test_old_copartition_line_in_a_crashed_container_does_not_block_a_repair(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-changelog", 4, 1]]}}, {"svc-dev-x-changelog": 1})
        st = self.state()
        st["deploys"]["svc"]["pods"][-1]["prev"] = [
            "TopologyException: Invalid topology: Following topics do not have the same number of partitions: [{a=4}, {b=32}]"]
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("REPAIRED svc", r.stdout)

    def test_app_with_client_id_is_identified_by_its_stream_thread_prefix(self):
        self.cluster({"svc": {"bad": [["svc-dev-x-repartition", 4, 1]], "client_id_set": True}}, {"svc-dev-x-repartition": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertEqual(self.deletes(), ["svc-dev-x-repartition"])

    def test_app_prefixed_topic_that_is_not_internal_is_refused(self):
        self.cluster({"svc": {"bad": [["svc-dev-output", 4, 1]]}}, {"svc-dev-output": 1})
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 3, r.stdout)
        self.assertIn("REFUSED svc", r.stdout)
        self.assertEqual(self.deletes(), [])

    def test_unreadable_log_is_inconclusive_not_ok(self):
        self.cluster({"svc": {}}, {"a": 4})
        st = self.state()
        st["unreadable"] = [st["deploys"]["svc"]["pods"][-1]["name"]]
        self.save(st)
        r = self.run_doctor("--repair", "svc")
        self.assertEqual(r.returncode, 5, r.stdout)
        self.assertIn("INCONCLUSIVE svc", r.stdout)

    def test_cluster_unreachable_is_unknown_not_ok(self):
        r = subprocess.run([BASH, str(DOCTOR), "--repair"], text=True, capture_output=True, timeout=60,
                           env=dict(os.environ, KUBECTL="false", KAFKA_TOPICS="false"))
        self.assertEqual(r.returncode, 5, r.stdout)


class DoctorWiringTest(unittest.TestCase):
    """Every bring-up path runs the doctor with --repair after the apps are up."""

    def body(self, path, header):
        text = (ROOT / path).read_text()
        start = text.index(header)
        return text[start:text.index("\n}\n", start)]

    def test_dev_full_and_overnight_start_run_the_doctor_last(self):
        start = self.body("scripts/ops/dev-cleanup.sh", "do_start() {")
        self.assertGreater(start.index("run_partition_doctor"), start.index("apply_internal_topic_configs"))
        self.assertGreater(start.index("run_partition_doctor"), start.index("scale deploy/"))
        overnight = self.body("scripts/ops/dev-cleanup.sh", "do_start_overnight() {")
        self.assertGreater(overnight.index("run_partition_doctor $OVERNIGHT_SET"), overnight.index("scale deploy/"))
        helper = self.body("scripts/ops/dev-cleanup.sh", "run_partition_doctor() {")
        self.assertIn("origin/main:scripts/kafka/streams-partition-doctor.sh", helper)
        self.assertIn("--repair", helper)
        self.assertIn('KUBECTL_SCALE="$K"', helper)

    def test_readiness_waits_stop_when_the_not_ready_set_is_stable(self):
        dev = self.body("scripts/ops/dev-cleanup.sh", "run_partition_doctor() {")
        self.assertIn("DOCTOR_STABLE_SECONDS", dev)
        self.assertNotIn("for d in $deploys; do", dev, "one get deploy per pass, not kubectl per app")
        prod = (ROOT / "scripts/ops/oe-boot-bringup.sh").read_text()
        self.assertIn("DOCTOR_STABLE_SECONDS", prod)

    def test_prod_boot_bringup_runs_the_doctor_after_wave_two(self):
        text = (ROOT / "scripts/ops/oe-boot-bringup.sh").read_text()
        self.assertGreater(text.index('bash "$DOCTOR" --repair'), text.index('scale_up "$REST"'))
        self.assertLess(text.index('bash "$DOCTOR" --repair'), text.index('log "=== boot bring-up done ==="'))
        self.assertIn('KUBECTL_SCALE="$KUBECTL $SA"', text)

    def test_es4_reset_repairs_then_rescans_before_declaring_wedged_topologies(self):
        text = (ROOT / "scripts/es4/cleanup-es4.sh").read_text()
        doctor = text.index('streams-partition-doctor.sh" --repair $wedged_deploys')
        self.assertLess(text.index('*"invalid partitions: expected"*) wedged='), doctor)
        rescan = text.index("scan_wedged_topologies", doctor)
        self.assertLess(rescan, text.index('echo "WEDGED STREAMS TOPOLOGIES'))
        self.assertNotIn('wedged=""\n    fi', text[doctor:rescan], "the doctor's exit code must not clear the verdict")


if __name__ == "__main__":
    unittest.main()
