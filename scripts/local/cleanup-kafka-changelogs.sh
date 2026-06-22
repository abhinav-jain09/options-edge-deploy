#!/usr/bin/env bash
#
# Mac-local daily Kafka changelog/repartition cleanup.
#
# Runs the SAME trim logic as the in-cluster CronJob
# (k8s/base/kafka-changelog-cleanup-cronjob.yaml) by shelling out to a
# one-shot `confluentinc/cp-kafka` container. The only host dependency
# is Docker — no host-side Kafka CLI install needed.
#
# Discovers `^options-(edge|flow)-.*-(changelog|repartition)$` topics
# and trims records older than RETENTION_MS via kafka-delete-records.
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

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-192.168.100.102:9092}"
RETENTION_MS="${RETENTION_MS:-86400000}"     # 24h
KAFKA_IMAGE="${KAFKA_IMAGE:-confluentinc/cp-kafka:7.6.0}"
LOG_DIR="${LOG_DIR:-$HOME/.local/var/log/oe-kafka-cleanup}"
TIMEOUT_SECS="${TIMEOUT_SECS:-1800}"   # 30-min hard ceiling per run

# Unique container name so concurrent runs don't collide and the
# watchdog has an unambiguous handle to `docker stop`.
CONTAINER_NAME="oe-kafka-cleanup-$$"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y%m%d-%H%M%S).log"

# Tee everything to a per-run log. launchd's stdout/stderr go to its own
# files; this tee captures both into one place keyed by start time.
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[host] start          : $(date -Iseconds)"
echo "[host] bootstrap      : $KAFKA_BOOTSTRAP_SERVERS"
echo "[host] retention_ms   : $RETENTION_MS"
echo "[host] image          : $KAFKA_IMAGE"
echo "[host] log            : $LOG_FILE"

if ! command -v docker >/dev/null 2>&1; then
  echo "[host] FATAL: docker not on PATH" >&2
  exit 1
fi

# The script body below is byte-identical (modulo indentation) to the
# inline args in k8s/base/kafka-changelog-cleanup-cronjob.yaml. Keep
# them in sync. If you change one, change the other.
#
# Networking note: we DO NOT use `--network host` here. Docker Desktop
# Mac only supports host networking on 4.34+ as an opt-in feature, and
# we can't assume it's enabled. The default KAFKA_BOOTSTRAP_SERVERS
# (192.168.100.102:9092) is a LAN IP that is reachable from the
# default Docker bridge network on Docker Desktop. For localhost-only
# Kafka, pass KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092.
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
    CANDIDATES=$(printf "%s\n" "$ALL" \
      | grep -E "^options-(edge|flow)-.*-(changelog|repartition)$" || true)

    if [ -z "$CANDIDATES" ]; then
      echo "[cleanup] FATAL: no candidate topics matched options-(edge|flow)-*-(changelog|repartition)" >&2
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

# Watchdog: enforce $TIMEOUT_SECS hard ceiling. If the docker run is
# still alive after the timeout, send SIGTERM (graceful stop, allowing
# Kafka client cleanup) and then SIGKILL the container if it doesn't
# exit within 30s. `docker stop` does both for us.
(
  # Subshell sleeps without holding the main shell's wait().
  sleep "$TIMEOUT_SECS"
  if kill -0 "$DOCKER_PID" 2>/dev/null; then
    echo "[host] WATCHDOG: ${TIMEOUT_SECS}s timeout reached, stopping container $CONTAINER_NAME" >&2
    docker stop -t 30 "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
) &
WATCHDOG_PID=$!
# Disown the watchdog so it doesn't block trap propagation if the
# script is signalled externally.
disown "$WATCHDOG_PID" 2>/dev/null || true

# Wait for the docker run to finish (either naturally or via watchdog).
set +e
wait "$DOCKER_PID"
RC=$?
set -e

# Tear down the watchdog if it's still sleeping (i.e. cleanup finished
# in time). Suppress errors — the watchdog may have already exited.
kill "$WATCHDOG_PID" 2>/dev/null || true
wait "$WATCHDOG_PID" 2>/dev/null || true

echo "[host] exit code      : $RC"
echo "[host] end            : $(date -Iseconds)"

# Prune logs older than 30 days so $LOG_DIR doesn't grow forever.
find "$LOG_DIR" -type f -name "*.log" -mtime +30 -delete 2>/dev/null || true

exit "$RC"
