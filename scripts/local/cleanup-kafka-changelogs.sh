#!/usr/bin/env bash
#
# Mac-local daily Kafka changelog/repartition cleanup.
#
# Runs the SAME trim logic as the in-cluster CronJob
# (k8s/base/kafka-changelog-cleanup-cronjob.yaml) by shelling out to a
# one-shot `confluentinc/cp-kafka` container. The only host dependency
# is Docker — no host-side Kafka CLI install needed.
#
# Discovers every `*-(changelog|repartition)` topic plus an explicit
# allowlist of managed high-volume business topics (covers every
# Kafka Streams app-id prefix we use — options-edge-*, options-flow-*, AND
# options-databento-* such as the maxpain streams app) and trims records
# older than RETENTION_MS via kafka-delete-records.
#
# USAGE
#   ./cleanup-kafka-changelogs.sh                 # uses defaults
#   KAFKA_BOOTSTRAP_SERVERS=host:port ./cleanup-kafka-changelogs.sh
#
# SCHEDULING
#   Run nightly via the bundled launchd plist:
#   ./install-launchd.sh
#
# SAFETY (matches the CronJob — fail-closed everywhere)
#   - zero candidate topics -> FAIL (treat as misconfig)
#   - kafka-configs describe non-zero -> SKIP the topic
#   - GetOffsetShell non-zero -> SKIP the topic
#   - GetOffsetShell stdout has any non-blank unparseable line -> SKIP
#   - compact-only policy -> SKIP
#   - all-old partitions (omitted from --time <ts>) -> trim to latest end
#
set -euo pipefail

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-host.docker.internal:19092}"
RETENTION_MS="${RETENTION_MS:-86400000}"     # 24h
KAFKA_IMAGE="${KAFKA_IMAGE:-confluentinc/cp-kafka:7.6.0}"
LOG_DIR="${LOG_DIR:-$HOME/.local/var/log/oe-kafka-cleanup}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"   # 30-min hard ceiling per run

# Unique container name so concurrent runs don't collide and the
# watchdog has an unambiguous handle to `docker stop`.
CONTAINER_NAME="oe-kafka-cleanup-$$"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y%m%d-%H%M%S).log"

# Portable ISO-8601 timestamp. We deliberately avoid `date -Iseconds`
# because that is a GNU/newer-Darwin extension and is NOT supported by
# older macOS / stock BSD `date`; this format string works on every
# BSD and GNU date. (The in-container `date -u -d @epoch` calls below
# are fine — they run inside the GNU-coreutils cp-kafka image.)
host_ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }

# Tee everything to a per-run log. launchd's stdout/stderr go to its own
# files; this tee captures both into one place keyed by start time.
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[host] start          : $(host_ts)"
echo "[host] bootstrap      : $KAFKA_BOOTSTRAP_SERVERS"
echo "[host] retention_ms   : $RETENTION_MS"
echo "[host] image          : $KAFKA_IMAGE"
echo "[host] log            : $LOG_FILE"

if ! command -v docker >/dev/null 2>&1; then
  echo "[host] FATAL: docker not on PATH" >&2
  exit 1
fi

# The script body below is FUNCTIONALLY EQUIVALENT to the inline args in
# k8s/base/kafka-changelog-cleanup-cronjob.yaml — same discovery regex,
# same fail-closed policy/offset/parse logic, same per-partition merge and
# delete-records spec. It is NOT byte-identical: quoting differs (this copy
# is wrapped in a single-quoted `docker run bash -c '...'`, so it uses
# double quotes internally), and comments/log strings differ. Keep the
# LOGIC in sync — if you change the trim behaviour in one, change both.
#
# Networking note: we DO NOT use `--network host` here. Docker Desktop
# Mac only supports host networking on 4.34+ as an opt-in feature, and
# we can't assume it's enabled. The default KAFKA_BOOTSTRAP_SERVERS
# (host.docker.internal:19092) is the native dev Kafka endpoint reachable from the
# default Docker bridge network on Docker Desktop. For localhost-only
# Kafka, pass KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:19092.
docker run --rm --name "$CONTAINER_NAME" \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e RETENTION_MS="$RETENTION_MS" \
  "$KAFKA_IMAGE" \
  bash -c '
    set -euo pipefail
    TS_MS=$(( $(date +%s) * 1000 - RETENTION_MS ))
    printf "[cleanup] bootstrap=%s\n" "$KAFKA_BOOTSTRAP_SERVERS"
    printf "[cleanup] cutoff_ts_ms=%s (utc=%s)\n" "$TS_MS" \
      "$(date -u -d "@$((TS_MS/1000))" "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo unknown)"

    ALL=$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list)
    # Trim Streams internal topics plus the exact high-volume business topics
    # intentionally managed by this cleanup.
    MANAGED_DATA_TOPICS="options.databento.strike-flow.strike.avro
option-price-behavior-by-strike
option-price-behavior-by-option"
    CANDIDATES=$(printf "%s\n" "$ALL" | awk -v managed="$MANAGED_DATA_TOPICS" "
      BEGIN { n=split(managed,a,/\\n/); for(i=1;i<=n;i++){gsub(/^[[:space:]]+|[[:space:]]+$/,\"\",a[i]); if(a[i]!=\"\") keep[a[i]]=1} }
      /-(changelog|repartition)\$/ || (\$0 in keep)
    ")

    if [ -z "$CANDIDATES" ]; then
      echo "[cleanup] FATAL: no managed cleanup topics were discovered" >&2
      echo "[cleanup] zero-discovery is treated as a fail-closed signal (likely misconfig)" >&2
      exit 1
    fi

    N=$(printf "%s\n" "$CANDIDATES" | wc -l | tr -d " ")
    printf "[cleanup] candidates (%s):\n" "$N"
    printf "%s\n" "$CANDIDATES" | sed "s/^/  - /"

    TRIMMED=0; SKIPPED=0; FAILED=0
    for T in $CANDIDATES; do
      # FAIL-CLOSED on policy lookup.
      if ! DESCRIBE=$(kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                                     --entity-type topics --entity-name "$T" --describe 2>&1); then
        echo "[cleanup] SKIP $T (kafka-configs describe failed: $DESCRIBE)" >&2
        SKIPPED=$((SKIPPED+1)); continue
      fi
      POLICY=$(printf "%s" "$DESCRIBE" | grep -oE "cleanup\.policy=[a-z,]+" | head -1 || true)
      POLICY=${POLICY:-cleanup.policy=delete}
      if [ "$POLICY" = "cleanup.policy=compact" ]; then
        echo "[cleanup] SKIP $T (compact-only)"
        SKIPPED=$((SKIPPED+1)); continue
      fi

      # Per-partition offset resolution with strict-parse fail-closed.
      LATEST_ERR=$(mktemp); LATEST_OUT=$(mktemp)
      if ! kafka-run-class kafka.tools.GetOffsetShell \
                            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                            --topic "$T" --time -1 >"$LATEST_OUT" 2>"$LATEST_ERR"; then
        echo "[cleanup] SKIP $T (GetOffsetShell --time -1 failed: $(cat "$LATEST_ERR"))" >&2
        SKIPPED=$((SKIPPED+1)); rm -f "$LATEST_OUT" "$LATEST_ERR"; continue
      fi
      BAD_OUT=$(grep -vE "^[^[:space:]]+:[0-9]+:[0-9]+$" "$LATEST_OUT" | grep -vE "^[[:space:]]*$" || true)
      if [ -n "$BAD_OUT" ]; then
        echo "[cleanup] SKIP $T (unparseable --time -1 stdout: $(printf "%s" "$BAD_OUT" | head -3 | tr "\n" "|"))" >&2
        SKIPPED=$((SKIPPED+1)); rm -f "$LATEST_OUT" "$LATEST_ERR"; continue
      fi
      LATEST=$(grep -E "^[^[:space:]]+:[0-9]+:[0-9]+$" "$LATEST_OUT" || true)
      rm -f "$LATEST_OUT" "$LATEST_ERR"
      if [ -z "$LATEST" ]; then
        echo "[cleanup] SKIP $T (no end offsets — empty topic)"
        SKIPPED=$((SKIPPED+1)); continue
      fi

      TS_ERR=$(mktemp); TS_OUT=$(mktemp)
      if ! kafka-run-class kafka.tools.GetOffsetShell \
                            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                            --topic "$T" --time "$TS_MS" >"$TS_OUT" 2>"$TS_ERR"; then
        echo "[cleanup] SKIP $T (GetOffsetShell --time $TS_MS failed: $(cat "$TS_ERR"))" >&2
        SKIPPED=$((SKIPPED+1)); rm -f "$TS_OUT" "$TS_ERR"; continue
      fi
      BAD_OUT=$(grep -vE "^[^[:space:]]+:[0-9]+:[0-9]+$" "$TS_OUT" | grep -vE "^[[:space:]]*$" || true)
      if [ -n "$BAD_OUT" ]; then
        echo "[cleanup] SKIP $T (unparseable --time $TS_MS stdout: $(printf "%s" "$BAD_OUT" | head -3 | tr "\n" "|"))" >&2
        SKIPPED=$((SKIPPED+1)); rm -f "$TS_OUT" "$TS_ERR"; continue
      fi
      TS_OFFSETS=$(grep -E "^[^[:space:]]+:[0-9]+:[0-9]+$" "$TS_OUT" || true)
      rm -f "$TS_OUT" "$TS_ERR"

      OFFSETS=$(awk -F: "
        NR==FNR { ts[\$1 FS \$2] = \$3; next }
        { key=\$1 FS \$2; if (key in ts) print \$1\":\"\$2\":\"ts[key]; else print \$0 }
      " <(printf "%s\n" "$TS_OFFSETS") <(printf "%s\n" "$LATEST"))

      SPEC=$(mktemp)
      {
        printf "{\"partitions\":["
        first=1
        while IFS=: read -r topic part off; do
          [ -z "$topic" ] && continue
          [ $first -eq 0 ] && printf ","
          printf "{\"topic\":\"%s\",\"partition\":%s,\"offset\":%s}" "$topic" "$part" "$off"
          first=0
        done <<EOF
$OFFSETS
EOF
        printf "],\"version\":1}"
      } >"$SPEC"

      if kafka-delete-records --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                              --offset-json-file "$SPEC" >/dev/null 2>&1; then
        echo "[cleanup] TRIMMED $T -> $(printf "%s" "$OFFSETS" | tr "\n" " ")"
        TRIMMED=$((TRIMMED+1))
      else
        echo "[cleanup] FAILED to trim $T" >&2
        FAILED=$((FAILED+1))
      fi
      rm -f "$SPEC"
    done

    printf "[cleanup] summary: trimmed=%s skipped=%s failed=%s of %s candidates\n" \
      "$TRIMMED" "$SKIPPED" "$FAILED" "$N"
    [ "$FAILED" -eq 0 ]
  ' &
DOCKER_PID=$!

# Tear-down helper. Order matters here: the docker CLI is killed FIRST
# (local, daemon-independent, always succeeds), THEN container cleanup
# is dispatched as fire-and-forget so a wedged Docker daemon cannot
# extend our deadline. Without that ordering, a hung daemon would
# block `docker stop` indefinitely and the watchdog would never reach
# the kill stage. Idempotent.
DOCKER_TEARDOWN_DONE=0
tear_down_docker() {
  [ "$DOCKER_TEARDOWN_DONE" = "1" ] && return 0
  DOCKER_TEARDOWN_DONE=1

  # STEP 1: kill the docker CLI parent. SIGTERM with grace, then SIGKILL.
  # This step is purely local — it does NOT call into Docker, so it
  # completes even when the daemon is wedged. This is what makes the
  # watchdog a true hard ceiling.
  if kill -0 "$DOCKER_PID" 2>/dev/null; then
    kill -TERM "$DOCKER_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$DOCKER_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$DOCKER_PID" 2>/dev/null; then
      kill -KILL "$DOCKER_PID" 2>/dev/null || true
    fi
  fi

  # STEP 2: best-effort container cleanup as a DETACHED background
  # process. Either call could block indefinitely if the Docker daemon
  # is wedged; backgrounding + disown lets us move on regardless. The
  # container name is unique-per-PID so the next nightly run does not
  # collide with a residual cleanup process still trying to run.
  (
    docker stop -t 10 "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  ) &
  disown "$!" 2>/dev/null || true

  # STEP 3: kill the watchdog so it doesn't outlive us.
  if [ -n "${WATCHDOG_PID:-}" ] && kill -0 "$WATCHDOG_PID" 2>/dev/null; then
    kill -KILL "$WATCHDOG_PID" 2>/dev/null || true
  fi
}

# External-signal handler: SIGINT/SIGTERM from launchd or the shell.
on_signal() {
  echo "[host] received signal, tearing down" >&2
  tear_down_docker
  exit 130
}
trap on_signal INT TERM

# Watchdog: enforce $TIMEOUT_SECS hard ceiling. Calls the SAME tear-down
# helper as the signal handler — so image-pull-stuck and
# daemon-unreachable cases are handled identically to a runaway
# in-container script.
(
  sleep "$TIMEOUT_SECS"
  if kill -0 "$DOCKER_PID" 2>/dev/null; then
    echo "[host] WATCHDOG: ${TIMEOUT_SECS}s timeout reached, tearing down" >&2
    tear_down_docker
  fi
) &
WATCHDOG_PID=$!

# Wait for the docker run to finish (either naturally or via watchdog).
set +e
wait "$DOCKER_PID"
RC=$?
set -e

# Tear down the watchdog if it's still sleeping (i.e. cleanup finished
# in time). Don't go through tear_down_docker — it would needlessly try
# to kill the already-completed DOCKER_PID.
if kill -0 "$WATCHDOG_PID" 2>/dev/null; then
  kill "$WATCHDOG_PID" 2>/dev/null || true
fi
wait "$WATCHDOG_PID" 2>/dev/null || true

echo "[host] exit code      : $RC"
echo "[host] end            : $(host_ts)"

# Prune logs older than 30 days so $LOG_DIR doesn't grow forever.
find "$LOG_DIR" -type f -name "*.log" -mtime +30 -delete 2>/dev/null || true

exit "$RC"
