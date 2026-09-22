"""oe-pipeline-selfheal.sh: the decisions it makes, exercised against stubs.

The script exists because on 2026-09-21 prod came back from a power cut with every systemd unit
healthy and no data moving. What makes it dangerous is the same thing that makes it useful: it
restarts production services and empties their state on its own. Its rule is that NOTHING happens
on the absence of progress, and nothing happens on a single observation — every action needs
positive, attributed, persistent evidence of the fault it fixes. So the tests here are about the
cases where it must NOT act, the destructive path's every failure mode, and each "must not act"
case is paired with a mutation showing the guard under test is what held it back. An assertion
that passes with the guard removed is not an assertion.
"""
import os
import subprocess
import textwrap
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "oe-pipeline-selfheal.sh"

DEPLOY = "strike-liquidity-heatmap-service"
GROUP = "options-edge-strike-liquidity-heatmap-prod"
CLAIM = f"{DEPLOY}-streams-state"
PV = "pvc-deadbeef"
DEAD_PROC = "76f02087-3c02-42ad-8266-247d8888bab4"    # process UUID of the producer that died
LIVE_PROC = "90aa94b7-00a8-4d95-b1a9-8550d8770f8a"    # process UUID of the pod that is running now
FRAME = "\tat org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.fetchCommittedOffsets(ConsumerCoordinator.java:996)\n"
WEDGE_DUMP = f'"{GROUP}-{LIVE_PROC}-StreamThread-1" #55 prio=5 waiting on condition\n{FRAME}'
OTHER_THREAD_DUMP = f'"kafka-admin-client-thread | {GROUP}-admin" #50 daemon prio=5 runnable\n{FRAME}'
HEALTHY_DUMP = (f'"{GROUP}-{LIVE_PROC}-StreamThread-1" #55 prio=5 runnable\n'
                '\tat org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer.pollForFetches(ClassicKafkaConsumer.java:731)\n')
CORRUPT_LOG = "ERROR ... ProcessorStateException: Invalid state during store open"
CORRUPT_SIG = "Invalid state during store open,ProcessorStateException"


def _sandbox(tmp_path, *, replicas="1", pod_age="4h18m", restoring=False, lag=1_900_000,
             advance=0, source_advance=50_000, grow_state=False, wedge=WEDGE_DUMP, corrupt=False,
             hist_line="", open_tx_age_minutes=None, tx_id=None, tx_proc=DEAD_PROC,
             members=(LIVE_PROC,), group_state="Stable", no_header_for="", abort_fail=False,
             hanging_fail=False, scale_fail_to="", pods_linger=False, find_fail=False,
             stale_twin=False, pv_path_override=None, extra_claim=False, rescale_between=False,
             intruder=False, hpa=False, deployments=(DEPLOY,)):
    """A fake estate: one consumer group with lag, one deployment, one pod, one PV."""
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    kbin = tmp_path / "kbin"; kbin.mkdir()
    storage = tmp_path / "storage"; storage.mkdir()
    actions = tmp_path / "actions.log"
    calls = tmp_path / "calls"
    aborts = tmp_path / "aborts.log"
    dumped = tmp_path / "dumped"
    pv_path = storage / f"{PV}_options-edge_{CLAIM}"
    pv_path.mkdir(); (pv_path / "rocksdb").write_bytes(b"x" * 1000)
    if find_fail:   # the wipe itself fails (EIO, a vanished mount): only the wipe call, nothing else
        (bin_dir / "find").write_text(textwrap.dedent("""\
            #!/usr/bin/env bash
            case "$*" in *"-mindepth 1 -delete"*) echo "find: cannot delete: Input/output error" >&2; exit 1 ;; esac
            exec /usr/bin/find "$@"
            """))
    if stale_twin:   # a lexically-earlier directory with the same claim name, NOT bound to the PVC
        twin = storage / f"pvc-0000stale_options-edge_{CLAIM}"; twin.mkdir()
        (twin / "rocksdb").write_bytes(b"s" * 1000)
    reported_pv = pv_path_override if pv_path_override is not None else str(pv_path)
    (tmp_path / "dump.txt").write_text(wedge or "")

    pod = f"{DEPLOY}-685f9ffd4c-tt69c"
    logline = ("StateUpdater-1 INFO StoreChangelogReader - Finished restoring changelog"
               if restoring else "ConsumerCoordinator - Request joining group")
    scaled_file = tmp_path / "scaled"
    claims = f"{CLAIM}\\n" + (f"second-{CLAIM}\\n" if extra_claim else "")
    deploy_rows = "".join(f'echo "{d}   $reps/$reps   $reps   $reps   4h"; ' for d in deployments)

    (bin_dir / "k3s").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        shift 3; [[ "${{1:-}}" == --as=* ]] && shift
        all="$*"
        reps={replicas}; [ -f "{scaled_file}" ] && reps=$(cat "{scaled_file}")
        case "$all" in
          "get deploy --no-headers")             {deploy_rows} ;;
          "get deploy {DEPLOY} -o jsonpath={{.spec.replicas}}")
              if [ "{int(rescale_between)}" = 1 ] && [ -f "{tmp_path}/pods_checked" ]; then printf '1'; else printf '%s' "$reps"; fi ;;
          "get deploy {DEPLOY} -o jsonpath="*claimName*)        printf '{claims}' ;;
          "get pvc {CLAIM} -o jsonpath={{.spec.volumeName}}")  printf '{PV}' ;;
          "get pv {PV} -o jsonpath="*)           printf '%s' '{reported_pv}' ;;
          "get hpa -o jsonpath="*)               [ "{int(hpa)}" = 1 ] && echo "{DEPLOY}"; true ;;
          "get pods --no-headers")
              if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{pod}   1/1   Running   2 (71m ago)   {pod_age}"; fi ;;
          "get pods -o jsonpath="*)
              if [ -f "{tmp_path}/wiped" ] && [ "{int(intruder)}" = 1 ]; then echo "{pod}-intruder"; fi
              touch "{tmp_path}/pods_checked"
              if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{pod}"; fi ;;
          "exec {pod} -- kill -3 1")             touch "{dumped}" ;;
          "logs {pod} --since=1m")               [ -f "{dumped}" ] && cat "{tmp_path}/dump.txt"; true ;;
          "logs {pod} --since=10m")
              echo "{logline}"; echo "{hist_line}"; [ "{int(corrupt)}" = 1 ] && echo "{CORRUPT_LOG}"; true ;;
          "logs "*)                              echo "{logline}" ;;
          "scale deploy/{DEPLOY} --replicas="*)
              want=${{all##*--replicas=}}; echo "$all" >> "{actions}"
              if [ "$want" = "{scale_fail_to}" ]; then echo "Error from server (Forbidden)"; exit 1; fi
              echo "$want" > "{scaled_file}" ;;
          "rollout restart deploy/"*|"delete pod "*) echo "$all" >> "{actions}" ;;
          *) echo "unexpected kubectl call: $all" >&2; exit 2 ;;
        esac
        exit 0
        """))
    # the wipe is observed through the storage dir: a marker lets the pod stub grow an intruder afterwards
    (bin_dir / "find").exists() or (bin_dir / "find").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        case "$*" in *"-mindepth 1 -delete"*) touch "{tmp_path}/wiped" ;; esac
        exec /usr/bin/find "$@"
        """))

    (kbin / "kafka-broker-api-versions.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
    (kbin / "kafka-topics.sh").write_text(
        "#!/usr/bin/env bash\necho 'Topic: __consumer_offsets\tTopicId: x\tPartitionCount: 50\tReplicationFactor: 1'\n")
    tx_id = tx_id if tx_id is not None else f"{GROUP}-{tx_proc}-11"
    hdr = lambda name, text: "" if name in no_header_for else text
    hdr_p = hdr("describe", "echo 'ProducerId\tProducerEpoch\tLatestCoordinatorEpoch\tLastSequence\tLastTimestamp\tCurrentTransactionStartOffset'\n")
    if open_tx_age_minutes is None:
        producers = hdr_p + "echo $'50123\\t288\\t126\\t-1\\t0\\tNone'"
    else:
        producers = hdr_p + f"echo \"50123\t288\t126\t30\t$(( $(date +%s)*1000 - {open_tx_age_minutes}*60000 ))\t151600145\""
    listing = hdr("list", "echo 'TransactionalId\tCoordinator\tProducerId\tTransactionState'\n") + f"echo $'{tx_id}\\t1\\t50123\\tOngoing'"
    hanging = ("echo 'Error: broker unreachable'; exit 1" if hanging_fail else
               hdr("hanging", "echo 'Topic\tPartition\tProducerId\tProducerEpoch\tCoordinatorEpoch\tStartOffset\tLastTimestamp\tDuration(min)'\n").strip() or "true")
    (kbin / "kafka-transactions.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        for a in "$@"; do
          case "$a" in
            find-hanging)       {hanging}; exit 0 ;;
            list)               {listing}
                                exit 0 ;;
            describe-producers) {producers}
                                exit 0 ;;
            abort)              echo "$*" >> "{aborts}"
                                if [ "{int(abort_fail)}" = 1 ]; then echo "Error: coordinator not available"; exit 1; fi
                                exit 0 ;;
          esac
        done
        exit 0
        """))
    grow = (f'printf "y%.0s" $(seq 1 5000) >> "{pv_path}/rocksdb"') if grow_state else "true"
    member_rows = "".join(f"echo '{GROUP} {GROUP}-{m}-StreamThread-1-consumer-{m} /10.0.0.1 {GROUP}-{m}-StreamThread-1-consumer 3'\n" for m in members)
    (kbin / "kafka-consumer-groups.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        case "$*" in
          *--members*) echo "GROUP CONSUMER-ID HOST CLIENT-ID #PARTITIONS"; {member_rows.strip() or 'true'}; exit 0 ;;
          *--state*)   echo; echo "GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS"; echo "{GROUP} 192.168.100.252:9092 (1) stream {group_state} 1"; exit 0 ;;
        esac
        n=0; [ -f "{calls}" ] && n=$(cat "{calls}"); n=$((n+1)); echo "$n" > "{calls}"
        if [ "$n" -ge 2 ]; then {grow}; fi
        cur=$((1000 + (n-1)*{advance})); end=$((1000 + {lag} + (n-1)*{source_advance}))
        echo "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID"
        echo "{GROUP} t 0 $cur $end $((end-cur)) c h cl"
        """))
    for f in list(bin_dir.iterdir()) + list(kbin.iterdir()):
        f.chmod(0o755)

    env = dict(os.environ)
    env.update(
        PATH=f"{bin_dir}:{env['PATH']}",
        KUBECTL="k3s kubectl -n options-edge", SA="--as=system:serviceaccount:options-edge:jenkins-deployer",
        KBIN=str(kbin), STORAGE=str(storage), GROUP_MAP=str(tmp_path / "groups.map"),
        LOG=str(tmp_path / "selfheal.log"), STATEDIR=str(tmp_path / "state"),
        SAMPLE_SECONDS="1", LAG_FLOOR="2000", LOAD_CEILING="9999", CONFIRM_CYCLES="2",
        EVIDENCE_MIN_SECONDS="0", DUMP_SETTLE_SECONDS="0", POD_GONE_WAIT_SECONDS="5",
        _ABORTS=str(aborts), _PV=str(pv_path), _STORAGE=str(storage), _DUMPED=str(dumped),
    )
    return env, actions


def _run(env, **over):
    r = subprocess.run(["bash", str(SCRIPT)], env={**env, **over}, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
    return r.stdout


def _acted(actions):
    return actions.read_text() if actions.exists() else ""


def _escalate(env, times, **over):
    """Drive `times` consecutive cycles (same STATEDIR); returns every cycle's output joined."""
    out = ""
    for _ in range(times):
        Path(env["_DUMPED"]).unlink(missing_ok=True)   # every cycle takes its own dump
        out += _run(env, **over)
    return out


def _seed(env, *, observed=5, strikes=0, wedge=None, corrupt=None):
    """Put the group past confirmation with evidence already recorded one cycle ago."""
    sd = Path(env["STATEDIR"]); sd.mkdir(exist_ok=True)
    (sd / f"{GROUP}.observed").write_text(f"{observed}\n")
    (sd / f"{GROUP}.strikes").write_text(f"{strikes}\n")
    ago = int(time.time()) - 600
    if wedge: (sd / f"{GROUP}.wedge").write_text(f"{ago} {wedge}\n")
    if corrupt: (sd / f"{GROUP}.corrupt").write_text(f"{ago} {corrupt}\n")


# --------------------------------------------------------------------------------------
# Evidence must be attributed (a StreamThread's own stack frame) and persistent (two cycles).
# --------------------------------------------------------------------------------------
def test_a_stalled_group_with_a_healthy_stack_is_never_touched(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP)
    out = _escalate(env, 5)
    assert "no StreamThread is parked in a wedge frame" in out and "NOT touched" in out
    assert _acted(actions) == "", "restarted a slow-but-healthy service"


def test_a_historical_log_line_mentioning_the_wedge_word_is_not_evidence(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP,
                            hist_line="WARN ConsumerCoordinator - fetchCommittedOffsets timed out (retrying)")
    _escalate(env, 5)
    assert _acted(actions) == ""


def test_a_wedge_frame_in_a_non_stream_thread_is_not_evidence(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=OTHER_THREAD_DUMP)
    _escalate(env, 5)
    assert _acted(actions) == ""


def test_a_wedge_frame_seen_once_is_not_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 2)   # cycle 2 is the first dump
    assert "one snapshot is not a wedge" in out and _acted(actions) == ""


def test_the_same_frame_on_two_consecutive_cycles_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 3)
    assert "CONFIRMED evidence on two cycles" in out and "STRIKE 1" in out
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_a_frame_that_does_not_persist_is_not_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path)
    _escalate(env, 2)                                   # frame seen once
    (tmp_path / "dump.txt").write_text(HEALTHY_DUMP)    # gone next cycle
    _escalate(env, 2)
    assert _acted(actions) == ""


def test_stale_evidence_from_long_ago_does_not_confirm(tmp_path):
    env, actions = _sandbox(tmp_path)
    _seed(env, wedge="fetchCommittedOffsets")
    (Path(env["STATEDIR"]) / f"{GROUP}.wedge").write_text(f"{int(time.time()) - 7200} fetchCommittedOffsets\n")
    _escalate(env, 1)
    assert _acted(actions) == ""


def test_one_stalled_sample_is_confirmed_before_evidence_is_gathered(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _run(env)
    assert "stalled on 1 of 2 consecutive checks" in out
    assert not Path(env["_DUMPED"]).exists(), "took a thread dump on a single observation"


def test_mutation_confirm_gate_removed_gathers_evidence_immediately(tmp_path):
    env, _ = _sandbox(tmp_path)
    _run(env, CONFIRM_CYCLES="1")
    assert Path(env["_DUMPED"]).exists()


# --------------------------------------------------------------------------------------
# Group -> deployment: exact transformation or explicit mapping; never a resemblance.
# --------------------------------------------------------------------------------------
def test_a_group_that_only_resembles_a_deployment_is_never_touched(tmp_path):
    env, actions = _sandbox(tmp_path, deployments=("strike-liquidity-heatmap-canary",))
    out = _escalate(env, 3)
    assert "resolves to no deployment by exact transformation" in out
    assert _acted(actions) == ""


def test_mutation_an_explicit_mapping_makes_the_same_group_resolve(tmp_path):
    env, actions = _sandbox(tmp_path, deployments=(DEPLOY, "strike-liquidity-heatmap-canary"))
    Path(env["GROUP_MAP"]).write_text(f"{GROUP} {DEPLOY}\n")
    _escalate(env, 3)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_a_mapping_to_a_deployment_that_does_not_exist_resolves_to_nothing(tmp_path):
    env, actions = _sandbox(tmp_path)
    Path(env["GROUP_MAP"]).write_text(f"{GROUP} ghost-service\n")
    out = _escalate(env, 3)
    assert "resolves to no deployment" in out and _acted(actions) == ""


# --------------------------------------------------------------------------------------
# A consumer whose SOURCE is not moving is not even a candidate; exemptions; signs of life.
# --------------------------------------------------------------------------------------
def test_flat_committed_offsets_on_a_quiet_source_are_not_a_candidate(tmp_path):
    env, actions = _sandbox(tmp_path, source_advance=0)
    out = _escalate(env, 3)
    assert "SOURCE did not move" in out and "STALLED" not in out and _acted(actions) == ""


def test_mutation_the_same_flat_consumer_becomes_a_candidate_once_its_source_moves(tmp_path):
    env, _ = _sandbox(tmp_path, source_advance=50_000)
    assert "STALLED" in _run(env)


def test_an_exempted_group_is_never_judged(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 3, EXEMPT_GROUPS=f"other-group {GROUP}")
    assert "STALLED" not in out and _acted(actions) == ""


def test_mutation_without_the_exemption_the_group_is_judged(tmp_path):
    env, _ = _sandbox(tmp_path)
    assert "STALLED" in _escalate(env, 1, EXEMPT_GROUPS="other-group")


def test_restoring_app_is_never_restarted_however_long_it_stalls(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=True)
    out = _escalate(env, 4)
    assert "NOT stuck" in out and "restoration" in out and _acted(actions) == ""


def test_mutation_same_app_without_the_restoration_log_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=False)
    _escalate(env, 3)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_growing_local_state_vetoes_for_an_app_that_logs_nothing(tmp_path):
    env, actions = _sandbox(tmp_path, grow_state=True)
    out = _escalate(env, 3)
    assert "local state grew" in out and _acted(actions) == ""


def test_mutation_same_app_with_static_state_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, grow_state=False)
    _escalate(env, 3)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_young_pod_is_given_its_grace_period(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age="3m")
    out = _escalate(env, 3)
    assert "pod is only 3m old" in out and _acted(actions) == ""


def test_mutation_with_no_grace_the_young_pod_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age="3m")
    _escalate(env, 3, GRACE_MINUTES="0")
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


# --------------------------------------------------------------------------------------
# Deliberately-down services, dry runs, recovery, a broker that is not up yet, the lock.
# --------------------------------------------------------------------------------------
def test_a_deployment_held_at_zero_replicas_is_never_started(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="0")
    out = _escalate(env, 3)
    assert "held down by decision" in out and _acted(actions) == ""


def test_dry_run_changes_nothing_at_all(tmp_path):
    env, actions = _sandbox(tmp_path)
    for _ in range(4):
        _run(env, DRY_RUN="true")
    assert _acted(actions) == ""
    statedir = Path(env["STATEDIR"])
    assert not statedir.exists() or [p for p in statedir.iterdir() if not p.name.startswith(".")] == []
    assert "stalled on 1 of 2 consecutive checks" in _run(env)


def test_a_group_that_recovers_loses_its_history(tmp_path):
    env, actions = _sandbox(tmp_path)
    _run(env)
    assert (Path(env["STATEDIR"]) / f"{GROUP}.observed").exists()
    other = tmp_path / "b"; other.mkdir()
    healthy, _ = _sandbox(other, advance=50_000)
    healthy["STATEDIR"] = env["STATEDIR"]
    _run(healthy)
    assert not (Path(env["STATEDIR"]) / f"{GROUP}.observed").exists()


def test_it_stays_quiet_when_kafka_is_not_answering_yet(tmp_path):
    env, actions = _sandbox(tmp_path)
    Path(env["KBIN"], "kafka-broker-api-versions.sh").write_text("#!/usr/bin/env bash\nexit 1\n")
    out = _run(env)
    assert "not answering" in out and _acted(actions) == ""


def test_a_failed_find_hanging_is_reported_not_read_as_none(tmp_path):
    env, _ = _sandbox(tmp_path, hanging_fail=True)
    out = _run(env)
    assert "find-hanging FAILED" in out and "hanging transactions: none" not in out


def test_a_second_instance_leaves_while_the_first_is_alive(tmp_path):
    env, _ = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir(); (sd / ".lock.d").mkdir()
    holder = subprocess.Popen(["sleep", "30"])
    try:
        (sd / ".lock.d" / "pid").write_text(str(holder.pid))
        out = _run(env)
        assert "another self-heal run is active" in out and "self-heal start" not in out
    finally:
        holder.kill()


def test_a_stale_lock_from_a_dead_process_is_taken_over(tmp_path):
    env, _ = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir(); (sd / ".lock.d").mkdir()
    (sd / ".lock.d" / "pid").write_text("999999")
    out = _run(env)
    assert "taking over" in out and "self-heal start" in out
    assert not (sd / ".lock.d").exists(), "lock not released on exit"


# --------------------------------------------------------------------------------------
# The abort: exactly this group's transaction, from a process provably gone, group Stable.
# --------------------------------------------------------------------------------------
def test_abandoned_transaction_of_a_dead_process_is_aborted_instead_of_restarting(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)   # 2 stall, 3 first dead sighting, 4 confirmed
    assert "ABANDONED transaction" in out and "absent from a Stable group on two consecutive cycles" in out
    assert "--start-offset 151600145" in Path(env["_ABORTS"]).read_text()
    assert _acted(actions) == "", "bounced the service instead of clearing the blocker"


def test_a_process_absent_on_only_one_cycle_is_not_declared_dead(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 3)
    assert "must still be absent next cycle" in out and not Path(env["_ABORTS"]).exists()


def test_a_rebalancing_group_is_never_judged_for_ownership(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(), group_state="PreparingRebalance")
    out = _escalate(env, 5)
    assert "a rebalance hides live members" in out and not Path(env["_ABORTS"]).exists()


def test_mutation_the_same_group_once_stable_is_judged(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(), group_state="Stable")
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists()


def test_an_open_transaction_owned_by_a_live_member_is_never_aborted(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=LIVE_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 5)
    assert "owned by LIVE member process" in out and not Path(env["_ABORTS"]).exists()


def test_a_recent_transaction_is_never_aborted_even_from_a_dead_process(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=1, tx_proc=DEAD_PROC)
    _escalate(env, 5)
    assert not Path(env["_ABORTS"]).exists()


def test_mutation_lowering_the_staleness_bar_aborts_the_recent_one(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=1, tx_proc=DEAD_PROC)
    _escalate(env, 4, STALE_TX_MINUTES="0")
    assert Path(env["_ABORTS"]).exists()


def test_a_sibling_application_sharing_the_group_prefix_is_never_matched(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_id=f"{GROUP}-canary-{DEAD_PROC}-11")
    out = _escalate(env, 4)
    assert "no transactional producer named for this group" in out and not Path(env["_ABORTS"]).exists()


def test_another_groups_transaction_on_the_shared_partition_is_never_aborted(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_id=f"options-edge-unified-sr-prod-{DEAD_PROC}-3")
    _escalate(env, 4)
    assert not Path(env["_ABORTS"]).exists()


def test_a_failed_abort_does_not_count_and_the_restart_path_stays_open(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, abort_fail=True)
    out = _escalate(env, 4)
    assert "abort did NOT succeed" in out and "not restarting this cycle" not in out
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_headerless_describe_producers_output_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, no_header_for="describe")
    out = _escalate(env, 4)
    assert "describe-producers output carried no recognisable header" in out and not Path(env["_ABORTS"]).exists()


def test_headerless_transaction_listing_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, no_header_for="list")
    out = _escalate(env, 4)
    assert "transaction listing carried no recognisable header" in out and not Path(env["_ABORTS"]).exists()


def test_headerless_find_hanging_output_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, no_header_for="hanging")
    assert "find-hanging output carried no recognisable header" in _run(env)


# --------------------------------------------------------------------------------------
# Strike 2 is destructive: it needs a confirmed corruption signature, and every step is checked.
# --------------------------------------------------------------------------------------
def test_strike_two_is_withheld_without_a_confirmed_corruption_signature(tmp_path):
    env, actions = _sandbox(tmp_path, corrupt=False)
    _seed(env, strikes=1, wedge="fetchCommittedOffsets")
    out = _run(env)
    assert "STRIKE 2 withheld" in out and (Path(env["_PV"]) / "rocksdb").exists()
    assert "--replicas=0" not in _acted(actions)


def test_corruption_seen_once_does_not_wipe(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True)
    _seed(env, strikes=1)
    out = _run(env)
    assert "must repeat next cycle" in out and (Path(env["_PV"]) / "rocksdb").exists()


def test_strike_two_wipes_only_the_pvc_bound_volume_and_restores_the_desired_replicas(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="2", wedge=HEALTHY_DUMP, corrupt=True, stale_twin=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "STRIKE 2" in out and "state dir emptied" in out and "scaled back to 2" in out
    assert not (Path(env["_PV"]) / "rocksdb").exists()
    twin = Path(env["_STORAGE"]) / f"pvc-0000stale_options-edge_{CLAIM}"
    assert (twin / "rocksdb").exists(), "wiped a stale same-name directory that the PVC is not bound to"
    acts = _acted(actions)
    assert "--replicas=0" in acts and "--replicas=2" in acts and "--replicas=1" not in acts
    assert not (Path(env["STATEDIR"]) / f"{DEPLOY}.down").exists()


def test_strike_two_is_withheld_when_an_autoscaler_targets_the_deployment(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, hpa=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "an HPA targets this deployment" in out and (Path(env["_PV"]) / "rocksdb").exists()


def test_strike_two_refuses_a_pv_path_that_escapes_the_storage_root(tmp_path):
    outside = tmp_path / "outside"; outside.mkdir(); (outside / "victim").write_text("x")
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, pv_path_override=f"{tmp_path}/storage/../outside")
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "NOT wiping anything" in out and (outside / "victim").exists()
    assert "--replicas=0" not in _acted(actions)


def test_strike_two_refuses_when_two_streams_state_claims_are_attached(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, extra_claim=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "NOT wiping anything" in out and (Path(env["_PV"]) / "rocksdb").exists()


def test_strike_two_does_not_wipe_when_the_scale_down_fails(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, scale_fail_to="0")
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "scale to 0 FAILED" in out and "nothing wiped" in out and (Path(env["_PV"]) / "rocksdb").exists()


def test_strike_two_does_not_wipe_under_a_lingering_pod(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, pods_linger=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "still present" in out and (Path(env["_PV"]) / "rocksdb").exists()
    assert "--replicas=1" in _acted(actions), "left the deployment at zero"


def test_strike_two_does_not_wipe_if_something_rescaled_the_deployment_meanwhile(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, rescale_between=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "scaled the deployment back up between the check and the wipe" in out
    assert (Path(env["_PV"]) / "rocksdb").exists()


def test_a_pod_that_appears_during_the_wipe_is_deleted_and_shouted(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, intruder=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "CRITICAL" in out and "appeared DURING the wipe" in out
    assert f"delete pod {DEPLOY}-685f9ffd4c-tt69c-intruder" in _acted(actions)


def test_strike_two_reports_a_partial_wipe_failure_and_still_scales_back(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, find_fail=True)
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "emptying" in out and "FAILED" in out and "--replicas=1" in _acted(actions)


def test_strike_two_scale_back_failure_is_shouted_and_retried_by_the_next_run(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, scale_fail_to="1")
    _seed(env, strikes=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "SCALE BACK TO 1 FAILED THREE TIMES" in out
    marker = Path(env["STATEDIR"]) / f"{DEPLOY}.down"
    assert marker.exists() and marker.read_text().strip() == "1"
    fixed = tmp_path / "fixed"; fixed.mkdir()
    env2, actions2 = _sandbox(fixed)
    env2["STATEDIR"] = env["STATEDIR"]
    Path(fixed / "scaled").write_text("0\n")
    out2 = _run(env2)
    assert "RESTORE:" in out2 and "--replicas=1" in _acted(actions2) and not marker.exists()
