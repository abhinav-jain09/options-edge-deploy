"""oe-pipeline-selfheal.sh: the decisions it makes, exercised against stubs.

The script exists because on 2026-09-21 prod came back from a power cut with every systemd unit
healthy and no data moving. What makes it dangerous is the same thing that makes it useful: it
aborts Kafka transactions and resets Streams state on its own (it never restarts anything). Its
rule is that NOTHING happens on the absence of progress, and nothing happens on a single
observation — every action needs positive, attributed, persistent evidence of the fault it fixes. So the tests here are about the
cases where it must NOT act, the destructive path's every failure mode, and each "must not act"
case is paired with a mutation showing the guard under test is what held it back. An assertion
that passes with the guard removed is not an assertion.
"""
import datetime
import os
import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "oe-pipeline-selfheal.sh"

DEPLOY = "strike-liquidity-heatmap-service"
GROUP = "options-edge-strike-liquidity-heatmap-prod"
CLAIM = f"{DEPLOY}-streams-state"
PV = "pvc-deadbeef"
RS = f"{DEPLOY}-685f9ffd4c"
POD = f"{RS}-tt69c"
DECOY_DEPLOY = f"{DEPLOY}-canary"
DECOY_RS = f"{DECOY_DEPLOY}-1a2b3c4d"
DECOY_POD = f"{DECOY_RS}-zz999"
DEAD_PROC = "76f02087-3c02-42ad-8266-247d8888bab4"    # process UUID of the producer that died
OTHER_PROC = "5c1d2e3f-4a5b-4c6d-8e7f-90a1b2c3d4e5"   # a live member that is NOT the transaction's owner
LIVE_PROC = "90aa94b7-00a8-4d95-b1a9-8550d8770f8a"    # process UUID of the pod that is running now
FETCH = "\tat org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.fetchCommittedOffsets(ConsumerCoordinator.java:996)\n"
SLEEP = "\tat java.lang.Thread.sleep0(java.base@21.0.12/Native Method)\n\tat org.apache.kafka.common.utils.Timer.sleep(Timer.java:212)\n"
INIT = "\tat org.apache.kafka.clients.producer.KafkaProducer.initTransactions(KafkaProducer.java:659)\n"
PARK = "\tat jdk.internal.misc.Unsafe.park(java.base@21.0.12/Native Method)\n"
HEAD = f'"{GROUP}-{LIVE_PROC}-StreamThread-1" #55 prio=5 waiting on condition\n'
WEDGE_DUMP = HEAD + SLEEP + FETCH                      # the coordinator retry loop: a real park
FETCH_LIVE_DUMP = HEAD + FETCH                          # inside the call, not sleeping: in flight
INIT_DUMP = HEAD + PARK + INIT                          # initTransactions in flight
OTHER_THREAD_DUMP = f'"kafka-admin-client-thread | {GROUP}-admin" #50 daemon prio=5 runnable\n' + SLEEP + FETCH
HEALTHY_DUMP = HEAD + "\tat org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer.pollForFetches(ClassicKafkaConsumer.java:731)\n"
CORRUPT_LOG = "ERROR ... ProcessorStateException: Invalid state during store open"
CORRUPT_SIG = "Invalid state during store open,ProcessorStateException"
RETRY_LOG = "WARN StreamsProducer - Timeout exception caught trying to initialize transactions. Reattempting initialization"


def _script(path, lines):
    """A stub executable: one bash line per list entry, no dedent games — the shebang is always line 1."""
    path.write_text("#!/usr/bin/env bash\n" + "\n".join(lines) + "\n")
    path.chmod(0o755)


def _table(headers, rows):
    """Render rows the way Kafka's printer does: each column as wide as its widest cell."""
    widths = [max(len(str(c)) for c in col) for col in zip(headers, *rows)]
    return [" ".join(f"{str(c):<{w}}" for c, w in zip(r, widths)).rstrip() for r in [headers, *rows]]


def _sandbox(tmp_path, *, group=GROUP, replicas="1", pod_age=258, restoring=False, lag=1_900_000,
             advance=0, source_advance=50_000, grow_state=False, wedge=WEDGE_DUMP, corrupt=False,
             hist_line="", retry_lines=0, decoy=False, open_tx_age_minutes=145, tx_id=None,
             tx_proc=DEAD_PROC, members=(LIVE_PROC,), group_state="Stable", no_header_for="",
             abort_fail=False, hanging_fail=False, scale_fail_to="", pods_linger=False,
             find_fail=False, stale_twin=False, pv_path_override=None, extra_claim=False,
             rescale_between=False, intruder=False, hpa=False, deployments=(DEPLOY,),
             mkdir_fail=False, coordinator_silent=False, group_col=True, commit_between=0,
             rx_kib=0, shared_claim_pod=False, shared_claim_workload=False, hanging_row=False,
             shared_claim_cronjob=False, late_holder=False, sub_path=False, tx_layout="tabs",
             init_sub_path=False, shared_claim_rs=False, foreign_file=False, nested_foreign=False,
             corrupt_stale=False, symlink_inside=False, members_fail=False, pods_fail_after_scale=False, workload_fail=False,
             hpa_fail=False, init_query_fail=False, list_fail_with_header=False, describe_fail_with_header=False,
             sidecar=False, sidecar_corrupt=False, old_pod_first=False, kafka_down=False, sidecar_mounts=False):
    """A fake estate: one consumer group with lag, one deployment (plus an optional look-alike),
    pods owned through ReplicaSets, one PV. Every stub is a list of bash lines."""
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    kbin = tmp_path / "kbin"; kbin.mkdir()
    storage = tmp_path / "storage"; storage.mkdir()
    actions = tmp_path / "actions.log"
    calls = tmp_path / "calls"
    aborts = tmp_path / "aborts.log"
    dumped = tmp_path / "dumped"
    pv_path = storage / f"{PV}_options-edge_{CLAIM}"
    # the complete tree Kafka Streams writes, as measured on prod: <app>/<task>/rocksdb/<store>/<RocksDB files>
    store = pv_path / group / "0_1" / "rocksdb" / "heatmap-state"; store.mkdir(parents=True)
    marker = store / "000012.sst"; marker.write_bytes(b"x" * 1000)
    for f, body in [("CURRENT", "MANIFAST-000004"), ("IDENTITY", "id"), ("LOCK", ""), ("LOG", ""), ("MANIFEST-000004", "m"), ("OPTIONS-000007", "o"), ("000011.log", "")]:
        (store / f).write_text(body)
    (pv_path / group / "0_1" / ".checkpoint").write_text("0")
    (pv_path / group / "kafka-streams-process-metadata").write_text("{}")
    (pv_path / group / ".lock").write_text("")
    if foreign_file:   # something that is NOT Kafka Streams state lives on the same volume
        (pv_path / "audit").mkdir(); (pv_path / "audit" / "retained-events").write_text("keep me")
    if symlink_inside:   # a symlink where a store file is expected
        os.symlink("/etc/hostname", store / "000099.sst")
    if nested_foreign:   # foreign data hidden INSIDE an accepted task directory
        (pv_path / group / "0_1" / "audit").mkdir(); (pv_path / group / "0_1" / "audit" / "retained-events").write_text("keep me")
    if stale_twin:   # a lexically-earlier directory with the same claim name, NOT bound to the PVC
        twin = storage / f"pvc-0000stale_options-edge_{CLAIM}"; (twin / group / "0_1").mkdir(parents=True)
        (twin / group / "0_1" / "rocksdb").write_bytes(b"s" * 1000)   # a stale twin need not be well-formed
    reported_pv = pv_path_override if pv_path_override is not None else str(pv_path)
    (tmp_path / "dump.txt").write_text(wedge or "")
    (tmp_path / "decoy-dump.txt").write_text(WEDGE_DUMP)
    created = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=pod_age)).strftime("%Y-%m-%dT%H:%M:%SZ")
    logline = ("StateUpdater-1 INFO StoreChangelogReader - Finished restoring changelog"
               if restoring else "ConsumerCoordinator - Request joining group")
    scaled_file = tmp_path / "scaled"
    claims = [CLAIM] + ([f"second-{CLAIM}"] if extra_claim else [])

    # ---- kubectl ----
    k = ['shift 3; [[ "${1:-}" == --as=* ]] && shift', 'all="$*"',
         f'reps={replicas}; [ -f "{scaled_file}" ] && reps=$(cat "{scaled_file}")',
         'case "$all" in',
         '  "get deploy --no-headers") ' + " ".join(f'echo "{d}   $reps/$reps   $reps   $reps   4h";' for d in deployments) + ' ;;',
         f'  "get deploy {DEPLOY} -o jsonpath={{.spec.replicas}}")',
         f'      if [ "{int(rescale_between)}" = 1 ] && [ -f "{tmp_path}/pods_checked" ]; then printf 1; else printf "%s" "$reps"; fi ;;',
         f'  "get deploy {DEPLOY} -o jsonpath="*"initContainers"*"volumeMounts"*) [ "{int(init_query_fail)}" = 1 ] && {{ echo "Unable to connect to the server: i/o timeout" >&2; exit 1; }}; [ "{int(init_sub_path)}" = 1 ] && echo "state bootstrap-cache" || true ;;',
         f'  "get deploy {DEPLOY} -o jsonpath="*"ephemeralContainers"*"volumeMounts"*) true ;;',
         f'  "get deploy {DEPLOY} -o jsonpath="*"volumeMounts"*) [ "{int(sub_path)}" = 1 ] && echo "state rocksdb" || echo "state" ;;',
         f'  "get deploy {DEPLOY} -o jsonpath="*"{{.name}}"*) echo "state {CLAIM}" ;;',
         f'  "get deploy {DEPLOY} -o jsonpath="*claimName*) ' + " ".join(f'echo "{c}";' for c in claims) + ' ;;',
         f'  "get pvc {CLAIM} -o jsonpath={{.spec.volumeName}}") printf "{PV}" ;;',
         f'  "get pv {PV} -o jsonpath="*) printf "%s" \'{reported_pv}\' ;;',
         f'  "get hpa -o jsonpath="*) [ "{int(hpa_fail)}" = 1 ] && {{ echo "Error from server (Forbidden)" >&2; exit 1; }}; [ "{int(hpa)}" = 1 ] && echo "{DEPLOY}"; true ;;',
         f'  "get rs -o jsonpath="*".kind"*) echo "{RS} Deployment/{DEPLOY}"; ' + (f'echo "{DECOY_RS} Deployment/{DECOY_DEPLOY}"; ' if decoy else '') + 'true ;;',
         f'  "get rs -o jsonpath="*) echo "{RS} {DEPLOY}"; ' + (f'echo "{DECOY_RS} {DECOY_DEPLOY}"; ' if decoy else '') + 'true ;;',
         '  "get pods --no-headers")',
         f'      if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{POD}   1/1   Running   0   4h"; fi ;;',
         '  "get pods -o jsonpath="*".spec.volumes"*)',
         f'      [ "{int(shared_claim_pod)}" = 1 ] && echo "some-other-job-abc {CLAIM} "',
         f'      [ "{int(late_holder)}" = 1 ] && [ -f "{tmp_path}/swapped" ] && echo "cron-late-xyz {CLAIM} "',
         f'      [ "$reps" != 0 ] && echo "{POD} {CLAIM} "; true ;;',
         # the deployment's pods (owner kind + creation time); the decoy pod is listed FIRST so any prefix-based selection would pick it
         '  "get pods -o jsonpath="*".kind"*)',
         f'      if [ "{int(pods_fail_after_scale)}" = 1 ] && [ "$reps" = 0 ]; then echo "Unable to connect to the server: EOF" >&2; exit 1; fi',
         (f'      echo "{DECOY_POD} ReplicaSet/{DECOY_RS} Running {created}"' if decoy else '      true'),
         f'      if [ -f "{tmp_path}/swapped" ] && [ "{int(intruder)}" = 1 ] && [ ! -f "{tmp_path}/intruder-deleted" ]; then echo "{POD}-intruder ReplicaSet/{RS} Running {created}"; fi',
         f'      touch "{tmp_path}/pods_checked"',
         (f'      echo "{POD}-old ReplicaSet/{RS} Running {(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=300)).strftime("%Y-%m-%dT%H:%M:%SZ")}"' if old_pod_first else '      true'),
         f'      if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{POD} ReplicaSet/{RS} Running {created}"; fi ;;',
         # the RX sampler's form: owner NAME only, no kind
         '  "get pods -o jsonpath="*"status.phase"*)',
         f'      if [ "$reps" != 0 ] || [ "{int(pods_linger)}" = 1 ]; then echo "{POD} {RS} Running"; fi; true ;;',
         f'  "get cronjobs -o jsonpath="*) [ "{int(shared_claim_cronjob)}" = 1 ] && echo "CronJob/nightly-compact {CLAIM} "; true ;;',
         '  "get deploy,sts,ds,jobs,rs,rc -o jsonpath="*)',
         f'      if [ "{int(workload_fail)}" = 1 ]; then echo "Error from server (Forbidden)" >&2; exit 1; fi',
         f'      echo "Deployment/{DEPLOY} {CLAIM} "; echo "ReplicaSet/{RS} {CLAIM} "',
         f'      [ "{int(shared_claim_workload)}" = 1 ] && echo "StatefulSet/{DEPLOY}-twin {CLAIM} "',
         f'      [ "{int(shared_claim_rs)}" = 1 ] && echo "ReplicaSet/orphan-rs-7f9 {CLAIM} "; true ;;',
         f'  "get pod "*" -o jsonpath="*"volumeMounts"*) [ "{int(sidecar)}" = 1 ] && echo "log-shipper{" state" if sidecar_mounts else ""}"; echo "app state" ;;',
         f'  "get pod "*" -o jsonpath="*".spec.volumes"*) echo "state {CLAIM}" ;;',
         f'  "get pod "*" -o jsonpath="*"containers"*) [ "{int(sidecar)}" = 1 ] && echo "log-shipper"; echo "app" ;;',
         f'  "exec {POD} --container app -- kill -3 1") touch "{dumped}" ;;',
         f'  "exec {DECOY_POD} --container app -- kill -3 1") touch "{dumped}-decoy" ;;',
         '  "exec "*" -- kill -3 1") echo "container-less exec refused" >&2; exit 2 ;;',
         '  "exec "*" --container app -- cat /proc/net/dev")',
         f'      c=0; [ -f "{tmp_path}/rxcalls" ] && c=$(cat "{tmp_path}/rxcalls"); c=$((c+1)); echo "$c" > "{tmp_path}/rxcalls"',
         '      echo "Inter-|   Receive"; echo " face |bytes"',
         '      echo "    lo: 999999 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"',
         f'      echo "  eth0: $(( 1000000 + c * {rx_kib} * 1024 )) 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0" ;;',
         f'  "logs {POD} --container app --since=1m") echo app >> "{tmp_path}/logs-read"; [ -f "{dumped}" ] && cat "{tmp_path}/dump.txt"; true ;;',
         f'  "logs {DECOY_POD} --container app --since=1m") echo app >> "{tmp_path}/logs-read"; [ -f "{dumped}-decoy" ] && cat "{tmp_path}/decoy-dump.txt"; true ;;',
         f'  "logs {POD} --container app --since=10m --timestamps")',
         # prod-shaped: local offset, nanoseconds (kubectl --timestamps on the host prints +02:00 and 9 digits)
         f'      ts=$(date +%Y-%m-%dT%H:%M:%S).123456789$(date +%z | sed -E "s/([+-][0-9]{{2}})([0-9]{{2}})/\\1:\\2/"); [ "{int(corrupt_stale)}" = 1 ] && ts="2000-01-01T12:00:00.000000000+02:00"',
         f'      echo "$ts {logline}"; echo "$ts {hist_line}"; ' + " ".join(f'echo "$ts {RETRY_LOG}";' for _ in range(retry_lines)) + f' [ "{int(corrupt)}" = 1 ] && echo "$ts {CORRUPT_LOG}"; echo app >> "{tmp_path}/logs-read"; true ;;',
         f'  "logs {POD} --container app --since=10m")',
         f'      echo "{logline}"; echo "{hist_line}"; ' + " ".join(f'echo "{RETRY_LOG}";' for _ in range(retry_lines)) + f' [ "{int(corrupt)}" = 1 ] && echo "{CORRUPT_LOG}"; true ;;',
         f'  "logs {DECOY_POD} --container app --since=10m"*) echo app >> "{tmp_path}/logs-read"; echo "$(date -u +%Y-%m-%dT%H:%M:%S.000000Z) {CORRUPT_LOG}" ;;',
         f'  "logs "*" --container app --since=3m") echo app >> "{tmp_path}/logs-read"; echo "{logline}" ;;',
         f'  "logs "*" --container log-shipper "*) echo log-shipper >> "{tmp_path}/logs-read"; [ "{int(sidecar_corrupt)}" = 1 ] && echo "$(date -u +%Y-%m-%dT%H:%M:%S.000000Z) {CORRUPT_LOG}"; true ;;',
         '  "logs "*) echo "container-less logs refused" >&2; exit 2 ;;',
         f'  "scale deploy/{DEPLOY} --replicas="*)',
         f'      want=${{all##*--replicas=}}; echo "$all" >> "{actions}"',
         f'      if [ "$want" = "{scale_fail_to}" ]; then echo "Error from server (Forbidden)"; exit 1; fi',
         f'      echo "$want" > "{scaled_file}" ;;',
         f'  "delete pod {POD}-intruder "*) echo "$all" >> "{actions}"; touch "{tmp_path}/intruder-deleted" ;;',
         f'  "rollout restart deploy/"*|"delete pod "*) echo "$all" >> "{actions}" ;;',
         '  *) echo "unexpected kubectl call: $all" >&2; exit 2 ;;',
         'esac', 'exit 0']
    _script(bin_dir / "k3s", k)

    # ---- host tools the reset uses ----
    _script(bin_dir / "mv", [f'/bin/mv "$@" && touch "{tmp_path}/swapped"'])
    if find_fail:   # removing the parked old tree fails: only that call, nothing else
        _script(bin_dir / "find", ['case "$*" in *".reset-"*"-mindepth 1 -delete"*) echo "find: cannot delete: Input/output error" >&2; exit 1 ;; esac',
                                   'exec /usr/bin/find "$@"'])
    else:           # every deletion of a parked tree is recorded with its arguments (the mount-boundary policy is asserted on)
        _script(bin_dir / "find", [f'case "$*" in *"-mindepth 1 -delete"*) echo "$*" >> "{tmp_path}/find-args" ;; esac',
                                   'exec /usr/bin/find "$@"'])
    if mkdir_fail:  # the empty replacement directory cannot be created (inodes, quota, permissions)
        _script(bin_dir / "mkdir", [f'case "$*" in "{pv_path}") echo "mkdir: cannot create directory: No space left on device" >&2; exit 1 ;; esac',
                                    'exec /bin/mkdir "$@"'])

    # ---- kafka ----
    _script(kbin / "kafka-broker-api-versions.sh", [f"exit {int(kafka_down)}"])
    _script(kbin / "kafka-topics.sh", ["echo $'Topic: __consumer_offsets\\tTopicId: x\\tPartitionCount: 50\\tReplicationFactor: 1'"])
    tx_id = tx_id if tx_id is not None else f"{group}-{tx_proc}-11"
    def hdr(name, line):
        if name in no_header_for: return []
        return [line if tx_layout == "tabs" else "echo '" + "  ".join(line.split("$'")[1].rstrip("'").split("\\t")) + "'"]
    def row(cells):   # a data row in the chosen layout
        return "echo $'" + "\\t".join(cells) + "'" if tx_layout == "tabs" else "echo '" + "  ".join(cells) + "'"
    tx = ['for a in "$@"; do', '  case "$a" in']
    if hanging_fail:
        tx += ['    find-hanging) echo "Error: broker unreachable"; exit 1 ;;']
    else:
        tx += ['    find-hanging)'] + ["      " + l for l in hdr("hanging", "echo $'Topic\\tPartition\\tProducerId\\tProducerEpoch\\tCoordinatorEpoch\\tStartOffset\\tLastTimestamp\\tDuration(min)'")] \
            + (["      " + row(["options.databento.normalized","0","50123","288","126","777","0","61"])] if hanging_row else []) + ['      exit 0 ;;']
    tx += ['    list)'] + ["      " + l for l in hdr("list", "echo $'TransactionalId\\tCoordinator\\tProducerId\\tTransactionState'")] \
        + ["      " + row([tx_id,"1","50123","Ongoing"]), f'      exit {int(list_fail_with_header)} ;;']
    tx += ['    describe-producers)'] + ["      " + l for l in hdr("describe", "echo $'ProducerId\\tProducerEpoch\\tLatestCoordinatorEpoch\\tLastSequence\\tLastTimestamp\\tCurrentTransactionStartOffset'")]
    if open_tx_age_minutes is None:
        tx += ["      " + row(["50123","288","126","-1","0","None"])]
    else:
        ts = f"$(( $(date +%s)*1000 - {open_tx_age_minutes}*60000 ))"
        tx += ["      " + (f"echo \"50123\t288\t126\t30\t{ts}\t151600145\"" if tx_layout == "tabs" else f"echo \"50123  288  126  30  {ts}  151600145\"")]
    tx += [f'      exit {int(describe_fail_with_header)} ;;', f'    abort) echo "$*" >> "{aborts}"',
           f'      if [ "{int(abort_fail)}" = 1 ]; then echo "Error: coordinator not available"; exit 1; fi', '      exit 0 ;;',
           '  esac', 'done', 'exit 0']
    _script(kbin / "kafka-transactions.sh", tx)

    # Kafka 4.3.0 on prod prints a leading GROUP column in both single-group tables; older/newer
    # printers may not. Both layouts are exercised, rendered the way Kafka's printer renders them.
    gc = lambda cells: (cells if group_col else cells[1:])
    state_lines = _table(gc(["GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS"]),
                         [gc([group, "192.168.100.252:9092  (1)", "stream", group_state, "1"])])
    member_lines = _table(gc(["GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"]),
                          [gc([group, f"{group}-{m}-StreamThread-1-consumer-{m}", "/10.0.0.1", f"{group}-{m}-StreamThread-1-consumer", "3"]) for m in members])
    grow = (f'printf "y%.0s" $(seq 1 5000) >> "{marker}"') if grow_state else "true"
    cg = ['case "$*" in',
          f'  *--members*) [ "{int(members_fail)}" = 1 ] && {{ echo "Error: Executing consumer group command failed due to org.apache.kafka.common.errors.TimeoutException" >&2; exit 1; }}',
          '      echo; ' + " ".join(f"echo '{l}';" for l in member_lines) + ' exit 0 ;;',
          f'  *--state*) [ "{int(coordinator_silent)}" = 1 ] && exit 0',
          '      echo; ' + " ".join(f"echo '{l}';" for l in state_lines) + ' exit 0 ;;',
          'esac',
          f'n=0; [ -f "{calls}" ] && n=$(cat "{calls}"); n=$((n+1)); echo "$n" > "{calls}"',
          f'if [ "$n" -ge 2 ]; then {grow}; fi',
          # commit_between: the consumer commits once between every pair of cycles (after t1, before the next t0)
          f'cycle=$(( (n-1)/2 )); cur=$((1000 + (n-1)*{advance} + cycle*{commit_between})); end=$((1000 + {lag} + (n-1)*{source_advance}))',
          'echo "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID"',
          f'echo "{group} t 0 $cur $end $((end-cur)) c h cl"']
    _script(kbin / "kafka-consumer-groups.sh", cg)

    env = dict(os.environ)
    env.update(
        PATH=f"{bin_dir}:{env['PATH']}",
        KUBECTL="k3s kubectl -n options-edge", SA="--as=system:serviceaccount:options-edge:jenkins-deployer",
        KBIN=str(kbin), STORAGE=str(storage), GROUP_MAP=str(tmp_path / "groups.map"), CONTAINER_MAP=str(tmp_path / "containers.map"),
        LOG=str(tmp_path / "selfheal.log"), STATEDIR=str(tmp_path / "state"),
        SAMPLE_SECONDS="1", LAG_FLOOR="2000", LOAD_CEILING="9999", CONFIRM_CYCLES="2",
        EVIDENCE_MIN_SECONDS="0", DUMP_SETTLE_SECONDS="0", POD_GONE_WAIT_SECONDS="5", WEDGE_CYCLES="2",
        _ABORTS=str(aborts), _PV=str(pv_path), _MARKER=str(marker), _STORAGE=str(storage), _DUMPED=str(dumped), _GROUP=group,
    )
    return env, actions


def _run(env, **over):
    r = subprocess.run(["bash", str(SCRIPT)], env={**env, **over}, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
    return r.stdout


def _acted(actions):
    return actions.read_text() if actions.exists() else ""


def _no_abort(env):
    """Kafka aborts are recorded separately from kubectl actions; a negative test must deny both."""
    return not Path(env["_ABORTS"]).exists()


def _escalate(env, times, **over):
    """Drive `times` consecutive cycles (same STATEDIR); returns every cycle's output joined."""
    out = ""
    for _ in range(times):
        Path(env["_DUMPED"]).unlink(missing_ok=True)   # every cycle takes its own dump
        Path(env["_DUMPED"] + "-decoy").unlink(missing_ok=True)
        out += _run(env, **over)
    return out


def _seed(env, *, observed=5, resets=0, wedge=None, corrupt=None):
    """Put the group past confirmation with evidence already recorded one cycle ago."""
    sd = Path(env["STATEDIR"]); sd.mkdir(exist_ok=True)
    g = env["_GROUP"]
    (sd / f"{g}.observed").write_text(f"{int(time.time())} {observed}\n")
    (sd / f"{g}.resets").write_text(f"{resets}\n")
    ago = int(time.time()) - 600
    if wedge: (sd / f"{g}.wedge").write_text(f"{ago} 1 {wedge}\n")
    if corrupt: (sd / f"{g}.corrupt").write_text(f"{ago} 1 {corrupt}\n")


def _aside_dirs(env):
    return [p for p in Path(env["_STORAGE"]).iterdir() if ".reset-" in p.name]


# --------------------------------------------------------------------------------------
# Evidence must be a PARK (retry), attributed to a StreamThread of the deployment's own pod,
# and persistent across two cycles.
# --------------------------------------------------------------------------------------
def test_a_stalled_group_with_a_healthy_stack_is_never_touched(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP)
    out = _escalate(env, 5)
    assert "no StreamThread is parked in a retry" in out and "NOT touched" in out
    assert _acted(actions) == "", "restarted a slow-but-healthy service"
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_a_live_fetch_without_a_sleep_frame_is_not_a_park(tmp_path):
    """Inside fetchCommittedOffsets but not sleeping between retries: an in-flight call."""
    env, actions = _sandbox(tmp_path, wedge=FETCH_LIVE_DUMP)
    _escalate(env, 5)
    assert _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_fetch_with_the_retry_sleep_is_a_park(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=WEDGE_DUMP)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_a_mixed_park_with_any_init_transactions_component_is_never_restarted(tmp_path):
    """Two StreamThreads in one dump: one in the fetch retry sleep, one in initTransactions with
    retry lines — the initTransactions component alone rules the restart out."""
    mixed = WEDGE_DUMP + f'"{GROUP}-{LIVE_PROC}-StreamThread-2" #56 prio=5 waiting on condition\n' + PARK + INIT
    env, actions = _sandbox(tmp_path, wedge=mixed, retry_lines=3, open_tx_age_minutes=None)
    out = _escalate(env, 5)
    assert "confirmed park (fetchCommittedOffsets,initTransactions) with no abandoned transaction to abort" in out
    assert "nothing is restarted by this script" in out
    assert _acted(actions) == "" and not Path(env["_ABORTS"]).exists()


def test_a_stalled_observation_from_long_ago_does_not_count_as_consecutive(tmp_path):
    env, actions = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir()
    (sd / f"{GROUP}.observed").write_text(f"{int(time.time()) - 7200} 5\n")   # two hours ago
    out = _run(env)
    assert "stalled on 1 of 2 consecutive checks" in out and not Path(env["_DUMPED"]).exists()


def test_a_live_transaction_initialisation_is_not_a_wedge(tmp_path):
    """A slow EOS producer caught inside initTransactions twice, with no retry log lines."""
    env, actions = _sandbox(tmp_path, wedge=INIT_DUMP, retry_lines=0)
    _escalate(env, 5)
    assert _acted(actions) == "", "restarted a healthy transactional producer"
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_initialisation_with_repeated_timeouts_is_a_confirmed_park_but_never_restarted(tmp_path):
    """With retry lines it IS a park (unlike the live case above) — and still no restart, opt-in or
    not: the transaction coordinator's health cannot be judged from this host."""
    env, actions = _sandbox(tmp_path, wedge=INIT_DUMP, retry_lines=3, open_tx_age_minutes=None)
    out = _escalate(env, 5)
    assert "confirmed park (initTransactions) with no abandoned transaction to abort" in out
    assert _acted(actions) == "" and not Path(env["_ABORTS"]).exists()


def test_a_historical_log_line_mentioning_the_wedge_word_is_not_evidence(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP,
                            hist_line="WARN ConsumerCoordinator - fetchCommittedOffsets timed out (retrying) Thread.sleep")
    _escalate(env, 5)
    assert _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_a_park_in_a_non_stream_thread_is_not_evidence(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=OTHER_THREAD_DUMP)
    _escalate(env, 5)
    assert _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_evidence_from_a_look_alike_deployments_pod_is_never_attributed(tmp_path):
    """foo-canary's pod is listed first and is genuinely wedged; foo's own pod is healthy."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, decoy=True, deployments=(DEPLOY, DECOY_DEPLOY))
    _escalate(env, 5)
    assert _acted(actions) == "", "acted on evidence read from another deployment's pod"
    assert not Path(env["_DUMPED"] + "-decoy").exists(), "dumped a pod this deployment does not own"
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_estate_with_the_own_pod_wedged_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=WEDGE_DUMP, decoy=True, deployments=(DEPLOY, DECOY_DEPLOY))
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_a_wedge_seen_once_is_not_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 2)
    assert "seen 1 of 2" in out and "must show the same next cycle" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_the_same_wedge_on_two_consecutive_cycles_is_acted_on(tmp_path):
    """Stall, park seen once, park confirmed (owner absent once), owner absent twice → abort."""
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 4)
    assert "CONFIRMED evidence" in out and Path(env["_ABORTS"]).exists()
    assert _acted(actions) == "", "something other than the abort happened"


def test_a_wedge_that_does_not_persist_is_not_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path)
    _escalate(env, 2)
    (tmp_path / "dump.txt").write_text(HEALTHY_DUMP)
    _escalate(env, 2)
    assert _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_stale_evidence_from_long_ago_does_not_confirm(tmp_path):
    env, actions = _sandbox(tmp_path)
    _seed(env, wedge="fetchCommittedOffsets")
    (Path(env["STATEDIR"]) / f"{GROUP}.wedge").write_text(f"{int(time.time()) - 7200} 1 fetchCommittedOffsets\n")
    _escalate(env, 1)
    assert _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_a_park_while_the_coordinator_is_not_answering_is_a_broker_incident_not_a_wedge(tmp_path):
    env, actions = _sandbox(tmp_path, coordinator_silent=True)
    out = _escalate(env, 5)
    assert "COORDINATOR is not answering" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_park_with_an_answering_coordinator_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, coordinator_silent=False)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_at_the_default_three_parks_are_needed_before_anything_is_aborted(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 3, WEDGE_CYCLES="3")          # stall, park 1, park 2
    assert "seen 2 of 3" in out and not Path(env["_ABORTS"]).exists()
    _escalate(env, 2, WEDGE_CYCLES="3")                # park 3 (owner absent once), then absent twice → abort
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_a_confirmed_park_with_nothing_to_abort_is_reported_and_nothing_is_restarted(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=None)
    out = _escalate(env, 5)
    assert "nothing is restarted by this script" in out
    assert _acted(actions) == "" and not Path(env["_ABORTS"]).exists()


def test_one_stalled_sample_is_confirmed_before_evidence_is_gathered(tmp_path):
    env, _ = _sandbox(tmp_path)
    out = _run(env)
    assert "stalled on 1 of 2 consecutive checks" in out and not Path(env["_DUMPED"]).exists()


def test_mutation_confirm_gate_removed_gathers_evidence_immediately(tmp_path):
    env, _ = _sandbox(tmp_path)
    _run(env, CONFIRM_CYCLES="1")
    assert Path(env["_DUMPED"]).exists()


# --------------------------------------------------------------------------------------
# Group -> deployment: exact transformation or explicit mapping; never a resemblance.
# --------------------------------------------------------------------------------------
def test_a_group_that_only_resembles_a_deployment_is_never_touched(tmp_path):
    env, actions = _sandbox(tmp_path, deployments=(DECOY_DEPLOY,))
    out = _escalate(env, 3)
    assert "resolves to no deployment by exact transformation" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_a_nonconforming_group_resolves_only_through_an_explicit_mapping(tmp_path):
    env, actions = _sandbox(tmp_path, group="legacy-heatmap-consumer")
    out = _escalate(env, 4)
    assert "resolves to no deployment" in out and _acted(actions) == ""
    Path(env["GROUP_MAP"]).write_text(f"legacy-heatmap-consumer {DEPLOY}\n")
    # same estate, mapping now present: the group is judged and acted on
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_a_mapping_to_a_deployment_that_does_not_exist_resolves_to_nothing(tmp_path):
    env, actions = _sandbox(tmp_path)
    Path(env["GROUP_MAP"]).write_text(f"{GROUP} ghost-service\n")
    out = _escalate(env, 3)
    assert "resolves to no deployment" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


# --------------------------------------------------------------------------------------
# Candidates, exemptions, signs of life — each with its mutation.
# --------------------------------------------------------------------------------------
def test_flat_committed_offsets_on_a_quiet_source_are_not_a_candidate(tmp_path):
    env, actions = _sandbox(tmp_path, source_advance=0)
    out = _escalate(env, 3)
    assert "SOURCE did not move" in out and "STALLED" not in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_flat_consumer_becomes_a_candidate_once_its_source_moves(tmp_path):
    env, _ = _sandbox(tmp_path, source_advance=50_000)
    assert "STALLED" in _run(env)


def test_a_commit_that_lands_between_cycles_is_progress(tmp_path):
    """Flat inside every 90-second window, yet ahead of the previous cycle: a slow committer."""
    env, actions = _sandbox(tmp_path, commit_between=500)
    out = _escalate(env, 5)
    assert "committed BETWEEN cycles" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_consumer_with_no_commit_between_cycles_is_a_candidate(tmp_path):
    env, actions = _sandbox(tmp_path, commit_between=0)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_group_tables_without_a_leading_group_column_are_read_by_header(tmp_path):
    env, _ = _sandbox(tmp_path, group_col=False, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)
    assert "ABANDONED transaction" in out and Path(env["_ABORTS"]).exists()


def test_group_tables_without_a_leading_group_column_still_see_a_live_owner(tmp_path):
    env, _ = _sandbox(tmp_path, group_col=False, open_tx_age_minutes=145, tx_proc=LIVE_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 5)
    assert "owned by LIVE member process" in out and not Path(env["_ABORTS"]).exists()


def test_an_exempted_group_is_never_judged(tmp_path):
    env, actions = _sandbox(tmp_path)
    out = _escalate(env, 3, EXEMPT_GROUPS=f"other-group {GROUP}")
    assert "STALLED" not in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_without_the_exemption_the_group_is_judged(tmp_path):
    env, _ = _sandbox(tmp_path)
    assert "STALLED" in _escalate(env, 1, EXEMPT_GROUPS="other-group")


def test_restoring_app_is_never_restarted_however_long_it_stalls(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=True)
    out = _escalate(env, 4)
    assert "NOT stuck" in out and "restoration" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_same_app_without_the_restoration_log_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, restoring=False)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_growing_local_state_vetoes_for_an_app_that_logs_nothing(tmp_path):
    env, actions = _sandbox(tmp_path, grow_state=True)
    out = _escalate(env, 3)
    assert "local state grew" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_same_app_with_static_state_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, grow_state=False)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_a_pod_that_is_receiving_data_is_fetching_not_parked(tmp_path):
    """Everything else says wedged (park on every dump, no commit) but the pod pulls megabytes:
    it is consuming a moving source, and a parked consumer only heartbeats."""
    env, actions = _sandbox(tmp_path, rx_kib=2048)
    out = _escalate(env, 5)
    assert "fetching; a parked consumer only heartbeats" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_pod_receiving_only_heartbeats_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, rx_kib=8)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


def test_young_pod_is_given_its_grace_period(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age=3)
    out = _escalate(env, 3)
    assert "pod is only 3m old" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_with_no_grace_the_young_pod_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age=3)
    _escalate(env, 4, GRACE_MINUTES="0")
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


# --------------------------------------------------------------------------------------
# Deliberately-down services, dry runs, recovery, a broker that is not up yet, the lock.
# --------------------------------------------------------------------------------------
def test_a_deployment_held_at_zero_replicas_is_never_started(tmp_path):
    """A lingering, genuinely wedged pod of a deployment the owner scaled to 0: without the guard
    the evidence would be complete and it would be restarted."""
    env, actions = _sandbox(tmp_path, replicas="0", pods_linger=True)
    out = _escalate(env, 4)
    assert "held down by decision" in out and _acted(actions) == ""
    assert not Path(env["_DUMPED"]).exists(), "gathered evidence on a held-down deployment"
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_mutation_the_same_wedged_pod_at_one_replica_is_acted_on(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="1", pods_linger=True)
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists(), "the abort — the one automated action on a park — did not happen"


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
    env, actions = _sandbox(tmp_path, hanging_fail=True, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)
    assert "find-hanging FAILED" in out and "find-hanging: nothing listed" not in out
    assert "options.databento.normalized" not in (Path(env["_ABORTS"]).read_text() if Path(env["_ABORTS"]).exists() else "")
    assert _acted(actions) == ""


def test_a_second_instance_leaves_while_the_first_is_alive(tmp_path):
    env, _ = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir()
    holder = subprocess.Popen(["sleep", "30"])
    try:
        os.symlink(str(holder.pid), sd / ".lock")
        out = _run(env)
        assert "another self-heal run is active" in out and "self-heal start" not in out
    finally:
        holder.kill()


def test_five_simultaneous_starts_over_a_stale_lock_admit_exactly_one(tmp_path):
    """The takeover of a dead owner's lock is itself guarded: contenders that lose the token leave."""
    env, _ = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir(); os.symlink("999999", sd / ".lock")
    procs = [subprocess.Popen(["bash", str(SCRIPT)], env={**env, "SAMPLE_SECONDS": "4"},
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for _ in range(5)]
    outs = [p.communicate(timeout=120)[0] for p in procs]
    started = sum(o.count("self-heal start") for o in outs)
    assert started == 1, outs
    assert not (sd / ".lock.takeover").exists()


def test_a_stale_lock_from_a_dead_process_is_taken_over(tmp_path):
    env, _ = _sandbox(tmp_path)
    sd = Path(env["STATEDIR"]); sd.mkdir()
    os.symlink("999999", sd / ".lock")
    out = _run(env)
    assert "taking over" in out and "self-heal start" in out
    assert not (sd / ".lock").exists() and not (sd / ".lock").is_symlink(), "lock not released on exit"


def test_five_simultaneous_starts_admit_exactly_one(tmp_path):
    """The acquisition itself is one atomic symlink: there is no gap in which a lock exists
    without its owner, so contenders arriving at the same instant cannot both proceed."""
    env, _ = _sandbox(tmp_path)
    procs = [subprocess.Popen(["bash", str(SCRIPT)], env={**env, "SAMPLE_SECONDS": "4"},
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for _ in range(5)]
    outs = [p.communicate(timeout=120)[0] for p in procs]
    started = sum(o.count("self-heal start") for o in outs)
    left = sum("another self-heal run is active" in o for o in outs)
    assert started == 1 and left == 4, outs


# --------------------------------------------------------------------------------------
# The abort: exactly this group's transaction, from a process provably gone, group Stable.
# --------------------------------------------------------------------------------------
def test_abandoned_transaction_of_a_dead_process_is_aborted_instead_of_restarting(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)   # 2 stall, 3 first dead sighting (verdict pending), 4 confirmed
    assert "ABANDONED transaction" in out and "absent from a Stable group on two consecutive cycles" in out
    assert "verdict is pending — next cycle decides" in out
    assert "--start-offset 151600145" in Path(env["_ABORTS"]).read_text()
    assert _acted(actions) == "", "bounced the service instead of clearing the blocker"


def test_a_process_absent_on_only_one_cycle_is_not_declared_dead(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 3)
    assert "must still be absent next cycle" in out and not Path(env["_ABORTS"]).exists()


def test_a_rebalancing_group_is_never_judged_for_ownership_nor_restarted(tmp_path):
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(), group_state="PreparingRebalance")
    out = _escalate(env, 5)
    assert "not Stable" in out and "NOT touched" in out
    assert not Path(env["_ABORTS"]).exists() and _acted(actions) == "", "acted on a rebalancing group"


def test_an_empty_group_never_proves_a_producer_dead(tmp_path):
    """No registered members: the producer may simply be between sessions."""
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(), group_state="Empty")
    out = _escalate(env, 5)
    assert not Path(env["_ABORTS"]).exists() and _acted(actions) == ""


def test_the_youngest_pod_decides_the_grace_during_a_rolling_update(tmp_path):
    env, actions = _sandbox(tmp_path, pod_age=2, old_pod_first=True)
    out = _escalate(env, 3)
    assert "pod is only 2m old" in out and _acted(actions) == ""
    assert _no_abort(env), "an abort happened in a must-not-act case"


def test_corruption_logged_by_a_sidecar_is_not_the_apps(tmp_path):
    """The sidecar is listed first (the pod's default); only the app mounts the state volume."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, sidecar=True, sidecar_corrupt=True, corrupt=False)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    reads = (tmp_path / "logs-read").read_text().split()
    assert "app" in reads and "log-shipper" not in reads, reads   # the app's logs WERE read, the sidecar's never
    assert "container log is clean" in out and "NOT touched" in out
    assert Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_two_containers_mounting_the_state_volume_yield_no_evidence(tmp_path):
    """A debug sidecar also mounts the volume and logs the corruption: ambiguous → nothing."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, sidecar=True, sidecar_mounts=True, sidecar_corrupt=True, corrupt=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "cannot be named unambiguously" in Path(env["LOG"]).read_text() and not (tmp_path / "logs-read").exists()
    assert Path(env["_MARKER"]).exists() and _acted(actions) == ""


def test_mutation_naming_the_app_container_resolves_the_ambiguity(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, sidecar=True, sidecar_mounts=True, sidecar_corrupt=True, corrupt=True)
    Path(env["CONTAINER_MAP"]).write_text(f"{DEPLOY} app\n")
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    reads = (tmp_path / "logs-read").read_text().split()
    assert "log-shipper" not in reads and "old tree removed" in out


def test_mutation_the_same_corruption_in_the_app_container_resets(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, sidecar=True, sidecar_corrupt=False, corrupt=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "old tree removed" in out


def test_a_deployment_left_at_zero_is_restored_even_while_kafka_is_still_down(tmp_path):
    env, actions = _sandbox(tmp_path, kafka_down=True)
    sd = Path(env["STATEDIR"]); sd.mkdir(); (sd / f"{DEPLOY}.down").write_text("2\n")
    out = _run(env)
    assert "RESTORE:" in out and "--replicas=2" in _acted(actions) and "Kafka is not answering" in out
    assert not (sd / f"{DEPLOY}.down").exists()


def test_mutation_the_same_group_once_stable_is_judged(tmp_path):
    """Stable, with a live member that is NOT the owner: the owner is proven absent, and aborted."""
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(OTHER_PROC,), group_state="Stable")
    _escalate(env, 4)
    assert Path(env["_ABORTS"]).exists()


def test_a_failed_members_read_is_never_an_empty_member_list(tmp_path):
    """--members times out: the owner cannot be judged absent, so nothing is aborted — ever."""
    env, actions = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members_fail=True)
    out = _escalate(env, 5)
    assert "the members table could not be read" in out and _no_abort(env) and _acted(actions) == ""


def test_a_stable_group_whose_members_table_names_nobody_is_inconsistent(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(), group_state="Stable")
    out = _escalate(env, 5)
    assert "names no member is inconsistent" in out and _no_abort(env)


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


def test_a_failed_abort_keeps_the_dead_owner_evidence_and_is_retried_next_run(tmp_path):
    """The coordinator refuses the abort once: the two-cycle 'owner is dead' verdict must survive,
    and the very next run must try the same abort again rather than starting over."""
    env, actions = _sandbox(tmp_path, abort_fail=True)
    out = _escalate(env, 4)
    assert "abort did NOT succeed" in out and _acted(actions) == ""
    dead = Path(env["STATEDIR"]) / f"{GROUP}.dead-{DEAD_PROC}"
    assert dead.exists(), "the dead-owner evidence was dropped after a failed abort"
    attempts = Path(env["_ABORTS"]).read_text().count("--start-offset 151600145")
    # the fault clears: the same sandbox, abort now succeeding
    stub = Path(env["KBIN"]) / "kafka-transactions.sh"
    stub.write_text(stub.read_text().replace('if [ "1" = 1 ]; then echo "Error: coordinator not available"; exit 1; fi', 'true'))
    _escalate(env, 1)
    assert Path(env["_ABORTS"]).read_text().count("--start-offset 151600145") == attempts + 1
    assert not dead.exists(), "a successful abort must clear the evidence"


def test_space_aligned_transaction_tables_are_read_the_same(tmp_path):
    env, actions = _sandbox(tmp_path, tx_layout="aligned", open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)
    assert "ABANDONED transaction" in out and "--start-offset 151600145" in Path(env["_ABORTS"]).read_text()


def test_space_aligned_tables_still_spare_a_live_owner(tmp_path):
    env, _ = _sandbox(tmp_path, tx_layout="aligned", open_tx_age_minutes=145, tx_proc=LIVE_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 5)
    assert "owned by LIVE member process" in out and not Path(env["_ABORTS"]).exists()


def test_headerless_describe_producers_output_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, no_header_for="describe")
    out = _escalate(env, 4)
    assert "describe-producers output carried no recognisable header" in out and not Path(env["_ABORTS"]).exists()


def test_headerless_transaction_listing_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, open_tx_age_minutes=145, no_header_for="list")
    out = _escalate(env, 4)
    assert "transaction listing carried no recognisable header" in out and not Path(env["_ABORTS"]).exists()


def test_a_transaction_listed_by_find_hanging_is_never_aborted_on_the_listing_alone(tmp_path):
    """A healthy producer with a long transaction.timeout.ms, busy for a few minutes, is "hanging"
    by find-hanging's threshold. Its owner is a live member: nothing may be aborted."""
    env, _ = _sandbox(tmp_path, hanging_row=True, open_tx_age_minutes=145, tx_proc=LIVE_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 5)
    assert "reported only" in out and not Path(env["_ABORTS"]).exists()


def test_mutation_the_same_listed_transaction_of_a_proven_dead_owner_is_aborted(tmp_path):
    env, _ = _sandbox(tmp_path, hanging_row=True, open_tx_age_minutes=145, tx_proc=DEAD_PROC, members=(LIVE_PROC,))
    out = _escalate(env, 4)
    aborts = Path(env["_ABORTS"]).read_text()
    assert "also holds an open transaction on options.databento.normalized-0" in out
    assert "--topic options.databento.normalized --partition 0 --start-offset 777" in aborts


def test_headerless_find_hanging_output_is_refused(tmp_path):
    env, _ = _sandbox(tmp_path, no_header_for="hanging")
    assert "find-hanging output carried no recognisable header" in _run(env)


# --------------------------------------------------------------------------------------
# Strike 2 is destructive: confirmed corruption only, an atomic swap, every step checked.
# --------------------------------------------------------------------------------------
def test_no_reset_without_a_confirmed_corruption_signature(tmp_path):
    env, actions = _sandbox(tmp_path, corrupt=False, open_tx_age_minutes=None)
    _seed(env, resets=0, wedge="fetchCommittedOffsets")
    out = _run(env)
    assert "no abandoned transaction to abort" in out and Path(env["_MARKER"]).exists()
    assert "--replicas=0" not in _acted(actions)


def test_a_repeated_historical_corruption_line_never_resets_a_recovered_service(tmp_path):
    """The same two-hour-old ProcessorStateException lines sit in every ten-minute read."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, corrupt_stale=True)
    _seed(env, resets=1)
    out = _escalate(env, 3)
    assert "old tree removed" not in out and "RESET:" not in out
    assert Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_mutation_fresh_corruption_lines_on_two_cycles_do_reset(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, corrupt_stale=False)
    _seed(env, resets=1)          # a first episode was already confirmed and reported
    out = _escalate(env, 2)       # fresh lines on two cycles confirm the second episode → reset
    assert "old tree removed" in out


def test_the_first_confirmed_corruption_episode_is_reported_not_reset(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True)
    _seed(env, resets=0)
    out = _escalate(env, 2)
    assert "first episode is reported only" in out
    assert Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_a_second_confirmed_episode_on_later_cycles_resets_once(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True)
    _seed(env, resets=0)
    out = _escalate(env, 4)       # episode 1: confirm+report; episode 2: confirm+reset
    assert "first episode is reported only" in out and "old tree removed" in out
    assert out.count("old tree removed") == 1


def test_strike_two_is_withheld_when_a_symlink_hides_in_the_tree(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, symlink_inside=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "symlink inside the volume" in out and Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_corruption_seen_once_does_not_reset(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True)
    _seed(env, resets=1)
    out = _run(env)
    assert "must repeat next cycle" in out and Path(env["_MARKER"]).exists()


def test_strike_two_swaps_only_the_pvc_bound_volume_and_restores_the_desired_replicas(tmp_path):
    env, actions = _sandbox(tmp_path, replicas="2", wedge=HEALTHY_DUMP, corrupt=True, stale_twin=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "RESET:" in out and "is now empty" in out and "old tree removed" in out and "scaled back to 2" in out
    pv = Path(env["_PV"])
    assert pv.is_dir() and list(pv.iterdir()) == [], "the live directory is not an empty directory"
    assert _aside_dirs(env) == [], "the parked old tree was not removed"
    twin = Path(env["_STORAGE"]) / f"pvc-0000stale_options-edge_{CLAIM}"
    assert (twin / GROUP / "0_1" / "rocksdb").exists(), "touched a stale same-name directory the PVC is not bound to"
    acts = _acted(actions)
    assert "--replicas=0" in acts and "--replicas=2" in acts and "--replicas=1" not in acts
    assert not (Path(env["STATEDIR"]) / f"{DEPLOY}.down").exists()


def test_strike_two_is_withheld_when_another_pod_mounts_the_same_claim(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, shared_claim_pod=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "also mounted or templated by pod/some-other-job-abc" in out
    assert Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_strike_two_is_withheld_when_another_workload_is_templated_on_the_claim(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, shared_claim_workload=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert f"StatefulSet/{DEPLOY}-twin" in out and Path(env["_MARKER"]).exists()


def test_mutation_with_the_claim_unshared_the_reset_proceeds(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "old tree removed" in out
    assert "-xdev" in (tmp_path / "find-args").read_text(), "the parked tree deletion may cross a mount boundary"


def test_strike_two_is_withheld_when_a_cronjob_is_templated_on_the_claim(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, shared_claim_cronjob=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "CronJob/nightly-compact" in out and Path(env["_MARKER"]).exists()


def test_a_holder_that_appears_after_the_swap_keeps_the_old_tree_parked(tmp_path):
    """A workload with no pod during the checks launches in the window: it can only have bound the
    whole old tree or the new empty one — and the old tree is never deleted while anything holds it."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, late_holder=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "stays parked" in out and "cron-late-xyz" in out
    assert len(_aside_dirs(env)) == 1 and (_aside_dirs(env)[0] / GROUP / "0_1" / "rocksdb").exists(), "deleted a tree something still holds"
    assert list(Path(env["_PV"]).iterdir()) == [] and "--replicas=1" in _acted(actions)


def test_strike_two_is_withheld_when_an_init_container_mounts_the_claim_with_a_subpath(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, init_sub_path=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "mounted with a subPath" in out and Path(env["_MARKER"]).exists()


def test_strike_two_is_withheld_when_the_volume_holds_anything_but_streams_state(tmp_path):
    """The claim's name says streams-state; the volume also carries audit/retained-events."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, foreign_file=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "holds something other than Kafka Streams state" in out
    assert Path(env["_MARKER"]).exists() and (Path(env["_PV"]) / "audit" / "retained-events").exists()
    assert "--replicas=0" not in _acted(actions)


def test_strike_two_is_withheld_when_foreign_data_hides_inside_a_task_directory(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, nested_foreign=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "holds something other than Kafka Streams state" in out and "unexpected entry in a task directory" in out
    assert Path(env["_MARKER"]).exists() and (Path(env["_PV"]) / GROUP / "0_1" / "audit" / "retained-events").exists()
    assert "--replicas=0" not in _acted(actions)


def test_strike_two_is_withheld_when_an_independent_replicaset_is_templated_on_the_claim(tmp_path):
    """The deployment's OWN ReplicaSets are always templated on the claim and must not count."""
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, shared_claim_rs=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "ReplicaSet/orphan-rs-7f9" in out and f"ReplicaSet/{RS}" not in out and Path(env["_MARKER"]).exists()


def test_strike_two_is_withheld_when_the_claim_is_mounted_with_a_subpath(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, sub_path=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "mounted with a subPath" in out and Path(env["_MARKER"]).exists()


def test_a_failed_hpa_read_withholds_the_reset(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, hpa_fail=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "HPA listing could not be read" in out and Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_a_failed_init_container_mount_query_withholds_the_reset(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, init_query_fail=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "mounts could not be read" in out and Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_a_transaction_listing_that_fails_with_a_parseable_header_is_not_trusted(tmp_path):
    env, _ = _sandbox(tmp_path, list_fail_with_header=True)
    out = _escalate(env, 5)
    assert "transaction listing FAILED" in out and _no_abort(env)


def test_a_describe_producers_that_fails_with_a_parseable_header_is_not_trusted(tmp_path):
    env, _ = _sandbox(tmp_path, describe_fail_with_header=True)
    out = _escalate(env, 5)
    assert "describe-producers FAILED" in out and _no_abort(env)


def test_strike_two_is_withheld_when_an_autoscaler_targets_the_deployment(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, hpa=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "an HPA targets this deployment" in out and Path(env["_MARKER"]).exists()


def test_strike_two_refuses_a_pv_path_that_escapes_the_storage_root(tmp_path):
    outside = tmp_path / "outside"; outside.mkdir(); (outside / "victim").write_text("x")
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, pv_path_override=f"{tmp_path}/storage/../outside")
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "NOT resetting anything" in out and (outside / "victim").exists()
    assert "--replicas=0" not in _acted(actions)


def test_strike_two_refuses_when_two_streams_state_claims_are_attached(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, extra_claim=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "NOT resetting anything" in out and Path(env["_MARKER"]).exists()


def test_strike_two_does_not_reset_when_the_scale_down_fails(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, scale_fail_to="0")
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "scale to 0 FAILED" in out and "nothing reset" in out and Path(env["_MARKER"]).exists()


def test_a_failed_pod_listing_after_scale_down_is_not_taken_as_no_pods(tmp_path):
    """The API drops right after the scale-down: an empty answer from a failed call is not 'gone'."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, pods_fail_after_scale=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env, POD_GONE_WAIT_SECONDS="5")
    assert "the listing failed (failed)" in out and "NOT resetting" in out
    assert Path(env["_MARKER"]).exists() and _aside_dirs(env) == []
    assert "--replicas=1" in _acted(actions), "left the deployment at zero"


def test_a_failed_workload_listing_withholds_the_reset(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, workload_fail=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "listing FAILED — exclusivity cannot be established" in out
    assert Path(env["_MARKER"]).exists() and "--replicas=0" not in _acted(actions)


def test_strike_two_does_not_reset_under_a_lingering_pod(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, pods_linger=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "still present" in out and Path(env["_MARKER"]).exists()
    assert "--replicas=1" in _acted(actions), "left the deployment at zero"


def test_strike_two_does_not_reset_if_something_rescaled_the_deployment_meanwhile(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, rescale_between=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "scaled the deployment back up" in out and "NOT resetting" in out
    assert Path(env["_MARKER"]).exists()


def test_a_failed_replacement_directory_rolls_the_swap_back(tmp_path):
    """mv succeeded, mkdir failed: the old tree must be back at the live path, never absent."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, mkdir_fail=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "rolled back" in out and "nothing changed on disk" not in out
    assert Path(env["_MARKER"]).exists() and _aside_dirs(env) == []
    assert "--replicas=1" in _acted(actions)


def test_a_pod_present_after_the_swap_is_stopped_before_the_old_tree_is_removed(tmp_path):
    """Covers the cleanup branch: a pod seen after the swap (however it got there) is stopped and
    waited for before the parked tree goes. It does not exercise a pod starting concurrently with
    the rename itself — the rename is atomic, so such a pod binds a whole tree either way."""
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, intruder=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "CRITICAL" in out and "appeared DURING the reset" in out
    acts = _acted(actions)
    assert f"delete pod {POD}-intruder --wait=true" in acts
    assert acts.index("delete pod") < acts.index("--replicas=1"), "scaled back before the intruder was stopped"
    assert "old tree removed" in out and _aside_dirs(env) == []


def test_a_failed_old_tree_removal_leaves_the_live_directory_empty_and_scales_back(tmp_path):
    env, actions = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, find_fail=True)
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
    out = _run(env)
    assert "removing the old tree" in out and "FAILED" in out and "--replicas=1" in _acted(actions)
    assert list(Path(env["_PV"]).iterdir()) == [] and len(_aside_dirs(env)) == 1


def test_strike_two_scale_back_failure_is_shouted_and_retried_by_the_next_run(tmp_path):
    env, _ = _sandbox(tmp_path, wedge=HEALTHY_DUMP, corrupt=True, scale_fail_to="1")
    _seed(env, resets=1, corrupt=CORRUPT_SIG)
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
