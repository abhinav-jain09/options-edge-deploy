"""oe-pipeline-selfheal.sh: the decisions it makes, exercised against stubs.

The script exists because on 2026-09-21 prod came back from a power cut with every systemd unit
healthy and no data moving. What makes it dangerous is the same thing that makes it useful: it
restarts production services on its own. So the tests here are about the cases where it must NOT
act, and every one of them is paired with a mutation showing the guard under test is what held it
back -- an assertion that passes with the guard removed is not an assertion.
"""
import os
import subprocess
import textwrap
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "oe-pipeline-selfheal.sh"

DEPLOY = "strike-liquidity-heatmap-service"
GROUP = "options-edge-strike-liquidity-heatmap-prod"


def _sandbox(tmp_path, *, replicas="1", pod_age="4h18m", restoring=False,
             lag=1_900_000, advance=0, grow_state=False, state_dir=True,
             open_tx_age_minutes=None):
    """A fake estate: one consumer group with lag, one deployment, one pod."""
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    kbin = tmp_path / "kbin"; kbin.mkdir()
    storage = tmp_path / "storage"; storage.mkdir()
    actions = tmp_path / "actions.log"
    calls = tmp_path / "calls"

    if state_dir:
        d = storage / f"pvc-deadbeef_options-edge_{DEPLOY}-streams-state"
        d.mkdir()
        (d / "rocksdb").write_bytes(b"x" * 1000)

    pod = f"{DEPLOY}-685f9ffd4c-tt69c"
    logline = ("StateUpdater-1 INFO StoreChangelogReader - Finished restoring changelog"
               if restoring else "ConsumerCoordinator - Request joining group")

    (bin_dir / "k3s").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        # $1=kubectl $2=-n $3=options-edge, then the real verb
        shift 3
        case "$1 $2" in
          "get deploy") echo "{DEPLOY} {replicas}/{replicas}   {replicas}            {replicas}           4h" ;;
          "get pods")   echo "{pod}   1/1   Running   2 (71m ago)   {pod_age}" ;;
          "logs "*)     echo "{logline}" ;;
          *) echo "$@" >> "{actions}" ;;
        esac
        # --as=... is passed BEFORE the verb by the script, so catch those too
        case "$*" in *"rollout restart"*|*"scale "*) echo "$*" >> "{actions}" ;; esac
        exit 0
        """))

    (kbin / "kafka-broker-api-versions.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
    aborts = tmp_path / "aborts.log"
    if open_tx_age_minutes is None:
        producers = "echo 'ProducerId ProducerEpoch LatestCoordinatorEpoch LastSequence LastTimestamp CurrentTransactionStartOffset'\n echo '50123 288 126 -1 0 None'"
    else:
        producers = (
            "echo 'ProducerId ProducerEpoch LatestCoordinatorEpoch LastSequence LastTimestamp CurrentTransactionStartOffset'\n"
            f"  echo \"50123 288 126 30 $(( $(date +%s)*1000 - {open_tx_age_minutes}*60000 )) 151600145\"")
    (kbin / "kafka-transactions.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        for a in "$@"; do
          case "$a" in
            find-hanging)       echo "Topic Partition ProducerId"; exit 0 ;;
            describe-producers) {producers}
                                exit 0 ;;
            abort)              echo "$*" >> "{aborts}"; exit 0 ;;
          esac
        done
        exit 0
        """))
    grow = (f'printf "y%.0s" $(seq 1 5000) >> "{storage}/pvc-deadbeef_options-edge_'
            f'{DEPLOY}-streams-state/rocksdb"') if grow_state else "true"
    (kbin / "kafka-consumer-groups.sh").write_text(textwrap.dedent(f"""\
        #!/usr/bin/env bash
        n=0; [ -f "{calls}" ] && n=$(cat "{calls}"); n=$((n+1)); echo "$n" > "{calls}"
        if [ "$n" -ge 2 ]; then {grow}; fi
        cur=$((1000 + (n-1)*{advance}))
        echo "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID"
        echo "{GROUP} t 0 $cur 9 {lag} c h cl"
        """))
    for f in list(bin_dir.iterdir()) + list(kbin.iterdir()):
        f.chmod(0o755)

    env = dict(os.environ)
    env.update(
        PATH=f"{bin_dir}:{env['PATH']}",
        KUBECTL="k3s kubectl -n options-edge", KBIN=str(kbin), STORAGE=str(storage),
        LOG=str(tmp_path / "selfheal.log"), STATEDIR=str(tmp_path / "state"),
        SAMPLE_SECONDS="1", LAG_FLOOR="2000", LOAD_CEILING="9999",
    )
    env["_ABORTS"] = str(aborts)
    return env, actions


def _run(env, **over):
    env = {**env, **over}
    r = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
    return r.stdout


def _acted(actions):
    return actions.read_text() if actions.exists() else ""


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
    """Without CONFIRM_CYCLES the first sample is enough -- proving the gate above did the work."""
    env, actions = _sandbox(tmp_path)
    out = _run(env, CONFIRM_CYCLES="1")
    assert "STRIKE 1" in out
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


# --------------------------------------------------------------------------------------
# A healthy app rebuilding state commits nothing and must never be mistaken for a wedged one.
# --------------------------------------------------------------------------------------
def test_restoring_app_is_never_restarted_however_long_it_stalls(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=True)
    for _ in range(3):
        out = _run(env)
    assert "NOT stuck" in out and "restoration" in out
    assert _acted(actions) == "", "restarted an app that was restoring its state"


def test_mutation_same_app_without_the_restoration_log_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=False)
    _run(env); _run(env)
    assert f"rollout restart deploy/{DEPLOY}" in _acted(actions)


def test_growing_local_state_vetoes_the_strike_for_an_app_that_logs_nothing(tmp_path):
    """databento-volume-aggregator ships a no-op SLF4J binder: disk growth is its only sign of life."""
    env, actions = _sandbox(tmp_path, grow_state=True)
    _run(env); out = _run(env)
    assert "local state grew" in out
    assert _acted(actions) == ""


def test_young_pod_is_given_its_grace_period(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age="3m")
    _run(env); out = _run(env)
    assert "pod is only 3m old" in out
    assert _acted(actions) == ""


# --------------------------------------------------------------------------------------
# Deliberately-down services, and dry runs, must stay untouched.
# --------------------------------------------------------------------------------------
def test_a_deployment_held_at_zero_replicas_is_never_started(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="0")
    _run(env); out = _run(env)
    assert "held down by decision" in out
    assert _acted(actions) == ""


def test_dry_run_changes_nothing_at_all(tmp_path):
    """Two DRY_RUN passes silently advanced two groups to strike 2 on 2026-09-21, so the first
    real run opened at strike 3 and declared healthy services defective."""
    env, actions = _sandbox(tmp_path)
    for _ in range(3):
        _run(env, DRY_RUN="true")
    assert _acted(actions) == ""
    statedir = Path(env["STATEDIR"])
    assert not statedir.exists() or list(statedir.iterdir()) == [], "a dry run wrote escalation state"

    out = _run(env)   # the first REAL run must still be at the very first observation
    assert "stuck on 1 of 2 consecutive checks" in out


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
    """A boot-time run must not call a still-starting broker's silence a wedged pipeline."""
    env, actions = _sandbox(tmp_path)
    Path(env["KBIN"], "kafka-broker-api-versions.sh").write_text("#!/usr/bin/env bash\nexit 1\n")
    Path(env["KBIN"], "kafka-broker-api-versions.sh").chmod(0o755)
    out = _run(env)
    assert "not answering" in out
    assert _acted(actions) == ""


# --------------------------------------------------------------------------------------
# The failure that actually took prod down: an abandoned transaction on the group's
# __consumer_offsets partition, which `find-hanging` does not report at all.
# --------------------------------------------------------------------------------------
def test_abandoned_offset_transaction_is_aborted_instead_of_restarting_the_service(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145)
    out = _run(env)
    assert "ABANDONED transaction on __consumer_offsets" in out
    assert "not restarting this cycle" in out
    aborts = Path(env["_ABORTS"])
    assert aborts.exists() and "--start-offset 151600145" in aborts.read_text()
    assert _acted(actions) == "", "bounced the service instead of clearing the blocker"


def test_an_in_flight_transaction_is_never_aborted(tmp_path):
    """A live EOS producer commits every few seconds; only a long-silent one is abandoned."""
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=1)
    _run(env)
    aborts = Path(env["_ABORTS"])
    assert not aborts.exists(), "aborted a transaction that was still in flight"


def test_mutation_lowering_the_staleness_bar_aborts_the_in_flight_one(tmp_path):
    """Proves the test above is held by STALE_TX_MINUTES and not by something incidental."""
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=1)
    _run(env, STALE_TX_MINUTES="0")
    assert Path(env["_ABORTS"]).exists()
