#!/usr/bin/env bash
set -euo pipefail

WEB_LOCAL_URL="${WEB_LOCAL_URL:-http://127.0.0.1:8090}"
if [[ -z "${WEB_PUBLIC_URL:-}" ]]; then
  WEB_PUBLIC_URL="http://192.168.100.252:8090"
fi
TOMCAT_PID_FILE="${TOMCAT_PID_FILE:-/home/abhinav/ci/tomcat/tomcat.pid}"
TOMCAT_LOG_FILE="${TOMCAT_LOG_FILE:-/home/abhinav/ci/tomcat/current/logs/catalina.out}"
TIMEOUT_SECONDS="${OPTIONS_EDGE_WEB_TIMEOUT_SECONDS:-120}"

WEB_LOCAL_URL="${WEB_LOCAL_URL%/}"
WEB_PUBLIC_URL="${WEB_PUBLIC_URL%/}"

print_diagnostics() {
  echo "OptionsEdge web app diagnostics:" >&2
  if [ -f "$TOMCAT_PID_FILE" ]; then
    echo "Tomcat pid file: $TOMCAT_PID_FILE pid=$(cat "$TOMCAT_PID_FILE")" >&2
    ps -fp "$(cat "$TOMCAT_PID_FILE")" >&2 || true
  else
    echo "Tomcat pid file missing: $TOMCAT_PID_FILE" >&2
  fi
  ss -ltnp 2>/dev/null | grep ':8090' >&2 || true
  echo "Recent Tomcat deployment/logging errors:" >&2
  if [ -f "$TOMCAT_LOG_FILE" ]; then
    grep -E 'Error deploying|LoggerFactory|SLF4J|LifecycleException|ROOT.war' "$TOMCAT_LOG_FILE" | tail -80 >&2 || true
    echo "Last 120 catalina.out lines:" >&2
    tail -120 "$TOMCAT_LOG_FILE" >&2 || true
  else
    echo "Tomcat log file missing: $TOMCAT_LOG_FILE" >&2
  fi
}

pid_alive() {
  [ -f "$TOMCAT_PID_FILE" ] && kill -0 "$(cat "$TOMCAT_PID_FILE")" 2>/dev/null
}

config_ok() {
  curl -fsS --connect-timeout 5 --max-time 10 "$WEB_PUBLIC_URL/api/config" | grep -q '"provider"'
}

root_ok() {
  curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null "$WEB_LOCAL_URL/" \
    && curl -fsS --connect-timeout 5 --max-time 10 -o /dev/null "$WEB_PUBLIC_URL/"
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if pid_alive && root_ok && config_ok; then
    echo "OptionsEdge web app is healthy at $WEB_PUBLIC_URL/"
    exit 0
  fi
  sleep 2
done

echo "OptionsEdge web app did not become healthy before timeout." >&2
echo "Expected local URL: $WEB_LOCAL_URL/" >&2
echo "Expected public URL: $WEB_PUBLIC_URL/" >&2
echo "Expected config URL: $WEB_PUBLIC_URL/api/config" >&2
print_diagnostics
exit 1
