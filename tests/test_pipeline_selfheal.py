"""oe-pipeline-selfheal.sh: the decisions it makes, exercised against stubs.

The script exists because on 2026-09-21 prod came back from a power cut with every systemd unit
healthy and no data moving. What makes it dangerous is the same thing that makes it useful: it
restarts production services and empties their state on its own. So the tests here are about the
cases where it must NOT act, and about the destructive path's every failure mode — and each
"must not act" case is paired with a mutation showing the guard under test is what held it back.
An assertion that passes with the guard removed is not an assertion.
"""
import os
import subprocess
import textwrap
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "oe-pipeline-selfheal.sh"

DEPLOY = "strike-liquidity-heatmap-service"
GROUP = "options-edge-strike-liquidity-heatmap-prod"
CLAIM = f"{DEPLOY}-streams-state"
PV = "pvc-deadbeef"


def _sandbox(tmp_path, *, replicas="1", pod_age="4h18m", restoring=False, lag=1_900_000,
             advance=0, source_advance=50_000, grow_state=False, state_dir=True,
             open_tx_age_minutes=None, tx_owner=GROUP, no_header=False, abort_fail=False,
             scale_fail_to="", pods_linger=False, find_fail=False):
    """A fake estate: one consumer group with lag, one deployment, one pod, one PV.

    The k3s stub answers the exact jsonpath queries the script makes and records every mutating
    call in actions.log. The kafka stubs are driven by the keyword arguments above.
    """
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    kbin = tmp_path / "kbin"; kbin.mkdir()
    storage = tmp_path / "storage"; storage.mkdir()
    actions = tmp_path / "actions.log"
    calls = tmp_path / "calls"
    aborts = tmp_path / "aborts.log"
    pv_path = storage / f"{PV}_options-edge_{CLAIM}"
    if state_dir:
        pv_path.mkdir()
        (pv_path / "rocksdb").write_bytes(b"x" * 1000)
        if find_fail:
            (pv_path / "rocksdb").chmod(0o444); pv_path.chmod(0o555)

    pod = f"{DEPLOY}-685f9ffd4c-tt69c"
    logline = ("StateUpdater-1 INFO StoreChangelogReader - Finished restoring changelog"
               if restoring else "ConsumerCoordinator - Request joining group")
    scaled_file = tmp_path / "scaled"   # holds the replica count after the last successful scale

    (bin_dir / "k3s").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        # $1=kubectl $2=-n $3=options-edge, then optional --as=..., then the verb
        shift 3; [[ "${{1:-}}" == --as=* ]] && shift
        all="$*"
        reps={replicas}; [ -f "{scaled_file}" ] && reps=$(cat "{scaled_file}")
        case "$all" in
          "get deploy --no-headers")             echo "{DEPLOY}   $reps/$reps   $reps   $reps   4h" ;;
          "get deploy {DEPLOY} -o jsonpath={{.spec.replicas}}") printf '%s' "$reps" ;;
          "get deploy {DEPLOY} -o jsonpath="*claimName*)        printf '{CLAIM}\\n' ;;
          "get pvc {CLAIM} -o jsonpath={{.spec.volumeName}}")  printf '{PV}' ;;
          "get pv {PV} -o jsonpath="*)           printf '{pv_path}' ;;
          "get pods --no-headers")
              if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{pod}   1/1   Running   2 (71m ago)   {pod_age}"; fi ;;
          "get pods -o jsonpath="*)
              if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{pod}"; fi ;;
          "logs "*)                              echo "{logline}" ;;
          "scale deploy/{DEPLOY} --replicas="*)
              want=${{all##*--replicas=}}; echo "$all" >> "{actions}"
              if [ "$want" = "{scale_fail_to}" ]; then echo "Error from server (Forbidden)"; exit 1; fi
              echo "$want" > "{scaled_file}" ;;
          "rollout restart deploy/{DEPLOY}")    echo "$all" >> "{actions}" ;;
          *) echo "unexpected kubectl call: $all" >&2; exit 2 ;;
        esac
        exit 0
        """))

    (kbin / "kafka-broker-api-versions.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
    (kbin / "kafka-topics.sh").write_text(
        "#!/usr/bin/env bash\necho 'Topic: __consumer_offsets\tTopicId: x\tPartitionCount: 50\tReplicationFactor: 1'\n")
    hdr_p = "" if no_header else "echo 'ProducerId\tProducerEpoch\tLatestCoordinatorEpoch\tLastSequence\tLastTimestamp\tCurrentTransactionStartOffset'\n"
    if open_tx_age_minutes is None:
        producers = hdr_p + "echo $'50123\\t288\\t126\\t-1\\t0\\tNone'"
    else:
        producers = hdr_p + f"echo \"50123\t288\t126\t30\t$(( $(date +%s)*1000 - {open_tx_age_minutes}*60000 ))\t151600145\""
    hdr_l = "" if no_header else "echo 'TransactionalId\tCoordinator\tProducerId\tTransactionState'\n"
    listing = hdr_l + f"echo $'{tx_owner}-76f02087-3c02-42ad-8266-247d8888bab4-11\\t1\\t50123\\tOngoing'"
    hdr_h = "" if no_header else "echo 'Topic\tPartition\tProducerId\tProducerEpoch\tCoordinatorEpoch\tStartOffset\tLastTimestamp\tDuration(min)'\n"
    (kbin / "kafka-transactions.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        for a in "$@"; do
          case "$a" in
            find-hanging)       {hdr_h.strip() or 'true'}; exit 0 ;;
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
    (kbin / "kafka-consumer-groups.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
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
        KBIN=str(kbin), STORAGE=str(storage),
        LOG=str(tmp_path / "selfheal.log"), STATEDIR=str(tmp_path / "state"),
        SAMPLE_SECONDS="1", LAG_FLOOR="2000", LOAD_CEILING="9999", CONFIRM_CYCLES="2",
        _ABORTS=str(aborts), _PV=str(pv_path),
    )
    return env, actions


def _run(env, **over):
    env = {**env, **over}
    r = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
    return r.stdout


def _acted(actions):
    return actions.read_text() if actions.exists() else ""


def _escalate(env, times):
    """Drive the same sandbox through `times` consecutive runs (same STATEDIR)."""
    out = ""
    for _ in range(times):
        out = _run(env)
    return out


# --------------------------------------------------------------------------------------
# It must not act on a single bad sample.
# --------------------------------------------------------------------------------------
def test_one_stuck_sample_is_confirmed_before_anything_is_restarted(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _run(env)
    assert "stuck on 1 of 2 consecutive checks" in out
    assert "rollout restart" not in _acted(actions), "acted on a single observation"
    out2 = _run(env)          # same STATEDIR: this is the confirming cycle
    assert "STRIKE 1" in out2
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_mutation_confirm_gate_removed_makes_it_act_immediately(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _run(env, CONFIRM_CYCLES="1")
    assert "STRIKE 1" in out and f"rollout restart deploy/{DEPLOY}" in _acted(actions)


# --------------------------------------------------------------------------------------
# A consumer whose SOURCE is not moving has nothing to commit and is not stuck.
# --------------------------------------------------------------------------------------
def test_flat_committed_offsets_on_a_quiet_source_are_not_a_stuck_consumer(tmp_path):
    env, actions = _sandbox(tmp_path, source_advance=0)
    out = _escalate(env, 3)
    assert "SOURCE did not move" in out and "NOT ADVANCING" not in out
    assert _acted(actions) == ""


def test_mutation_the_same_flat_consumer_is_acted_on_once_its_source_moves(tmp_path):
    env, actions = _sandbox(tmp_path, source_advance=50_000)
    _escalate(env, 2)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_an_exempted_group_is_never_judged(tmp_path):
    env, actions = _sandbox(tmp_path)
    _escalate({**env, "EXEMPT_GROUPS": f"other-group {GROUP}"}, 3)
    assert _acted(actions) == ""


# --------------------------------------------------------------------------------------
# A healthy app rebuilding state commits nothing and must never be mistaken for a wedged one.
# --------------------------------------------------------------------------------------
def test_restoring_app_is_never_restarted_however_long_it_stalls(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=True)
    out = _escalate(env, 3)
    assert "NOT stuck" in out and "restoration" in out
    assert _acted(actions) == "", "restarted an app that was restoring its state"


def test_mutation_same_app_without_the_restoration_log_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=False)
    _escalate(env, 2)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_growing_local_state_vetoes_the_strike_for_an_app_that_logs_nothing(tmp_path):
    """databento-volume-aggregator ships a no-op SLF4J binder: disk growth is its only sign of life."""
    env, actions = _sandbox(tmp_path, grow_state=True)
    out = _escalate(env, 2)
    assert "local state grew" in out and _acted(actions) == ""


def test_young_pod_is_given_its_grace_period(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age="3m")
    out = _escalate(env, 2)
    assert "pod is only 3m old" in out and _acted(actions) == ""


# --------------------------------------------------------------------------------------
# Deliberately-down services, and dry runs, must stay untouched.
# --------------------------------------------------------------------------------------
def test_a_deployment_held_at_zero_replicas_is_never_started(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="0")
    out = _escalate(env, 2)
    assert "held down by decision" in out and _acted(actions) == ""


def test_dry_run_changes_nothing_at_all(tmp_path):
    """Two DRY_RUN passes silently advanced two groups to strike 2 on 2026-09-21, so the first
    real run opened at strike 3 and declared healthy services defective."""
    env, actions = _sandbox(tmp_path)
    for _ in range(3):
        _run(env, DRY_RUN="true")
    assert _acted(actions) == ""
    statedir = Path(env["STATEDIR"])
    assert not statedir.exists() or [p for p in statedir.iterdir() if not p.name.startswith(".")] == []
    assert "stuck on 1 of 2 consecutive checks" in _run(env)


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


# --------------------------------------------------------------------------------------
# The failure that actually took prod down: an abandoned transaction on the group's
# __consumer_offsets partition, which `find-hanging` does not report at all.
# --------------------------------------------------------------------------------------
def test_abandoned_offset_transaction_of_this_group_is_aborted_instead_of_restarting(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145)
    out = _run(env)
    assert "ABANDONED transaction of its own producer" in out and "not restarting this cycle" in out
    assert "--start-offset 151600145" in Path(env["_ABORTS"]).read_text()
    assert _acted(actions) == "", "bounced the service instead of clearing the blocker"


def test_an_in_flight_transaction_is_never_aborted(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=1)
    _run(env)
    assert not Path(env["_ABORTS"]).exists(), "aborted a transaction that was still in flight"


def test_mutation_lowering_the_staleness_bar_aborts_the_in_flight_one(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=1)
    _run(env, STALE_TX_MINUTES="0")
    assert Path(env["_ABORTS"]).exists()


def test_another_groups_transaction_on_the_shared_partition_is_never_aborted(tmp_path):
    """Many groups hash to one coordinator partition; only THIS group's producers may be touched."""
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_owner="options-edge-unified-sr-prod")
    out = _run(env)
    assert not Path(env["_ABORTS"]).exists()
    assert "no transactional producer of this group" in out


def test_a_failed_abort_does_not_count_and_leaves_the_escalation_open(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, abort_fail=True)
    out = _run(env)
    assert "abort did NOT succeed" in out and "not restarting this cycle" not in out
    assert "stuck on 1 of 2" in out                      # the confirm/strike path continued
    assert f"rollout restart deploy/{DEPLOY}" in _acted(_run_and_get(env, actions))


def _run_and_get(env, actions):
    _run(env); return actions


def test_cli_output_without_a_header_is_refused_not_misparsed(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, no_header=True)
    out = _run(env)
    assert "REFUSING" in out or "no transactional producer" in out
    assert not Path(env["_ABORTS"]).exists(), "aborted something parsed from headerless output"


# --------------------------------------------------------------------------------------
# Strike 2 is destructive. Every step is checked; production is never left at zero replicas.
# --------------------------------------------------------------------------------------
def test_strike_two_wipes_only_the_pvc_bound_volume_and_restores_the_desired_replicas(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="2")
    Path(env["STATEDIR"]).mkdir(exist_ok=True)
    (Path(env["STATEDIR"]) / f"{GROUP}.observed").write_text("5\n")
    (Path(env["STATEDIR"]) / f"{GROUP}.strikes").write_text("1\n")
    out = _run(env)
    assert "STRIKE 2" in out and "state dir emptied" in out and "scaled back to 2" in out
    assert not (Path(env["_PV"]) / "rocksdb").exists()
    acts = _acted(actions)
    assert "--replicas=0" in acts and "--replicas=2" in acts and "--replicas=1" not in acts
    assert not (Path(env["STATEDIR"]) / f"{DEPLOY}.down").exists()


def test_strike_two_does_not_wipe_when_the_scale_down_fails(tmp_path):
    env, actions = _sandbox(tmp_path, scale_fail_to="0")
    Path(env["STATEDIR"]).mkdir(exist_ok=True)
    (Path(env["STATEDIR"]) / f"{GROUP}.observed").write_text("5\n")
    (Path(env["STATEDIR"]) / f"{GROUP}.strikes").write_text("1\n")
    out = _run(env)
    assert "scale to 0 FAILED" in out and "nothing wiped" in out
    assert (Path(env["_PV"]) / "rocksdb").exists()


def test_strike_two_does_not_wipe_under_a_lingering_pod(tmp_path):
    env, actions = _sandbox(tmp_path, pods_linger=True)
    Path(env["STATEDIR"]).mkdir(exist_ok=True)
    (Path(env["STATEDIR"]) / f"{GROUP}.observed").write_text("5\n")
    (Path(env["STATEDIR"]) / f"{GROUP}.strikes").write_text("1\n")
    out = _run(env, POD_GONE_WAIT_SECONDS="5")
    assert "still present" in out and (Path(env["_PV"]) / "rocksdb").exists()
    assert "--replicas=1" in _acted(actions), "left the deployment at zero"


def test_strike_two_scale_back_failure_is_shouted_and_retried_by_the_next_run(tmp_path):
    env, actions = _sandbox(tmp_path, scale_fail_to="1")
    Path(env["STATEDIR"]).mkdir(exist_ok=True)
    (Path(env["STATEDIR"]) / f"{GROUP}.observed").write_text("5\n")
    (Path(env["STATEDIR"]) / f"{GROUP}.strikes").write_text("1\n")
    out = _run(env)
    assert "SCALE BACK TO 1 FAILED THREE TIMES" in out
    marker = Path(env["STATEDIR"]) / f"{DEPLOY}.down"
    assert marker.exists() and marker.read_text().strip() == "1"
    # the fault clears: the very next run restores BEFORE judging anything
    fixed = tmp_path / "fixed"; fixed.mkdir()
    env2, actions2 = _sandbox(fixed)
    env2["STATEDIR"] = env["STATEDIR"]
    Path(fixed / "scaled").write_text("0\n")
    out2 = _run(env2)
    assert "RESTORE:" in out2 and "--replicas=1" in _acted(actions2) and not marker.exists()
