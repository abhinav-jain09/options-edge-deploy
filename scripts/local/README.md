# Mac-local nightly Kafka changelog cleanup

Same trim logic as the in-cluster CronJob, runnable on the operator's Mac.

## When to use this

- **Dev cluster runs on this Mac** (docker-desktop k3s). The in-cluster
  CronJob already covers it — this script is for ad-hoc runs and as a
  belt-and-suspenders nightly fallback when the cluster CronJob is
  paused or the cluster is down.
- **Local Kafka not under k3s** (rare — e.g. testing against a separate
  brew-installed Kafka). The launchd job is the primary scheduler.

In production, you don't need this — the in-cluster CronJob is sufficient.

## Files

| Path | Purpose |
|---|---|
| `cleanup-kafka-changelogs.sh` | Runnable bash script. Calls Kafka tools via a one-shot `confluentinc/cp-kafka` Docker container — no host Kafka CLI needed. |
| `com.optionsedge.kafka-cleanup.plist` | launchd job: fires daily at 00:00 local Mac time. |
| `install-launchd.sh` | Idempotent installer: copies script to `~/bin`, templates the plist, loads via `launchctl`. |

## Install

```bash
./scripts/local/install-launchd.sh
```

That's it. The job is now scheduled.

## Run ad-hoc (no wait for midnight)

```bash
# Either invoke the script directly:
./scripts/local/cleanup-kafka-changelogs.sh

# Or trigger the launchd job (uses the installed copy in ~/bin):
launchctl start com.optionsedge.kafka-cleanup
```

## Config (env vars)

| Var | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `192.168.100.102:9092` | Dev Kafka per memory:dev-deploy-topology |
| `RETENTION_MS` | `86400000` (24h) | Match `KAFKA_CHANGELOG_RETENTION_MS` in the configmap |
| `KAFKA_IMAGE` | `confluentinc/cp-kafka:7.6.0` | Same image the CronJob uses |
| `LOG_DIR` | `~/.local/var/log/oe-kafka-cleanup` | One file per run, auto-pruned >30 days |

Examples:

```bash
RETENTION_MS=43200000 ./scripts/local/cleanup-kafka-changelogs.sh   # 12h
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ./scripts/local/cleanup-kafka-changelogs.sh
```

## Safety

The script mirrors the CronJob's fail-closed semantics line-for-line:

- zero candidate topics → FAIL the run (likely misconfig)
- `kafka-configs --describe` non-zero exit → SKIP topic
- `GetOffsetShell` non-zero exit → SKIP topic
- unparseable stdout (log4j noise) → SKIP topic
- compact-only policy → SKIP topic
- all-old partitions (omitted from `--time <ts>`) → trim to latest end offset via per-partition merge

The script is idempotent — running it twice in close succession trims
nothing the second time (the watermark is already at the trim line).

## Logs

```bash
# Per-run log (one file per run, timestamped):
ls -lt ~/.local/var/log/oe-kafka-cleanup/

# launchd's own stdout/stderr (cumulative, append-only):
tail -f /tmp/oe-kafka-cleanup.stdout.log
tail -f /tmp/oe-kafka-cleanup.stderr.log
```

## Uninstall

```bash
launchctl unload -w ~/Library/LaunchAgents/com.optionsedge.kafka-cleanup.plist
rm ~/Library/LaunchAgents/com.optionsedge.kafka-cleanup.plist
rm ~/bin/oe-kafka-cleanup.sh
```

## Notes

- launchd does not natively support per-job timezones. The plist fires
  at `00:00 local Mac time`. If your Mac is in Europe/Amsterdam, this
  matches the cluster CronJob. Otherwise the firing time differs.
- launchd will fire the job at next wake if the Mac was asleep at the
  scheduled time. Idempotency makes this safe.
- **Networking on Docker Desktop Mac**: the script uses the default
  Docker bridge network (NOT `--network host`). Bridge networking is
  the safe default — it works on every Docker Desktop version. The
  default `KAFKA_BOOTSTRAP_SERVERS=192.168.100.102:9092` is a LAN IP
  routable from inside a bridge-networked container.
  - For a Kafka bound to `localhost:9092`, use:
    `KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092`
  - If you specifically need host networking (e.g. for an obscure
    bind), Docker Desktop 4.34+ supports it via the opt-in
    "Enable host networking" setting. Add `--network host` to the
    `docker run` line manually in that case.
- **Run timeout**: the script enforces a 30-minute hard ceiling per
  run via an in-script watchdog that calls `docker stop` on the
  container. Override with `TIMEOUT_SECS=N`. launchd's
  `AbandonProcessGroup` does **not** implement a timeout — it only
  controls process-group cleanup on job teardown — so the timeout
  must live in the script itself.
